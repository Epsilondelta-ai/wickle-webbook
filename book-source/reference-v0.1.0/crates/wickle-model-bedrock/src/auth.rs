use crate::error;
use aws_credential_types::Credentials;
use aws_sigv4::{
    http_request::{SignableBody, SignableRequest, SigningSettings, sign},
    sign::v4,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::{
    fmt,
    time::{Duration, UNIX_EPOCH},
};
use wickle::*;

/// Distinguishes inference access from AWS control-plane model inspection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BedrockAudience {
    /// One model inference request.
    Inference,
    /// Foundation-model or inference-profile metadata.
    Metadata,
}
/// Host authorization context; the adapter does not load an environment chain.
pub struct BedrockCredentialContext<'a> {
    /// Authenticated connection owner.
    pub scope: &'a Scope,
    /// Requested service purpose.
    pub audience: BedrockAudience,
    /// Explicit request origin region.
    pub region: &'a str,
    /// Cancellation while loading or refreshing credentials.
    pub cancellation: &'a tokio_util::sync::CancellationToken,
    /// Deadline covering credential resolution and HTTP.
    pub deadline: tokio::time::Instant,
}
/// Host-supplied credentials. No credential material enters the catalog or model body.
#[derive(Clone)]
pub enum BedrockCredential {
    /// IAM credentials, optionally temporary, signed with the AWS SigV4 library.
    Aws(Credentials),
    /// Bedrock API key/token, usable for inference only.
    Bearer(String),
}
impl fmt::Debug for BedrockCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockCredential([redacted])")
    }
}
/// Applications can adapt an AWS SDK credential chain without putting it in the core.
pub trait BedrockCredentialProvider: Send + Sync {
    /// Resolve credentials for this scope and service purpose.
    fn credential<'a>(
        &'a self,
        context: &'a BedrockCredentialContext<'a>,
    ) -> PortFuture<'a, BedrockCredential>;
}
impl BedrockCredentialProvider for BedrockCredential {
    fn credential<'a>(
        &'a self,
        _: &'a BedrockCredentialContext<'a>,
    ) -> PortFuture<'a, BedrockCredential> {
        Box::pin(async { Ok(self.clone()) })
    }
}
pub(crate) struct SigningRequest<'a> {
    pub method: &'a str,
    pub url: &'a reqwest::Url,
    pub body: &'a [u8],
    pub service: &'a str,
    pub clock: &'a dyn Clock,
    pub headers: HeaderMap,
}
pub(crate) async fn authorize(
    provider: &dyn BedrockCredentialProvider,
    context: BedrockCredentialContext<'_>,
    request: SigningRequest<'_>,
) -> Result<HeaderMap, ContractError> {
    let credential = tokio::select! {biased;
        _=context.cancellation.cancelled()=>return Err(error(ErrorCode::Cancelled,"credential")),
        _=tokio::time::sleep_until(context.deadline)=>return Err(error(ErrorCode::DeadlineExceeded,"credential")),
        result=provider.credential(&context)=>result.map_err(|_|error(ErrorCode::AccessDenied,"credential"))?,
    };
    let mut headers = request.headers;
    match credential {
        BedrockCredential::Bearer(token) => {
            if context.audience != BedrockAudience::Inference
                || token.is_empty()
                || token.chars().any(char::is_whitespace)
            {
                return Err(error(ErrorCode::AccessDenied, "bearer"));
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| error(ErrorCode::AccessDenied, "bearer"))?;
            value.set_sensitive(true);
            headers.insert("authorization", value);
        }
        BedrockCredential::Aws(credentials) => {
            // A refresh can take time; check expiry and sign only after it completes.
            let millis = u64::try_from(request.clock.now()?.utc_ms)
                .map_err(|_| error(ErrorCode::ClockUnavailable, "signing_time"))?;
            let time = UNIX_EPOCH
                .checked_add(Duration::from_millis(millis))
                .ok_or_else(|| error(ErrorCode::ClockUnavailable, "signing_time"))?;
            if credentials.expiry().is_some_and(|expiry| expiry <= time) {
                return Err(error(ErrorCode::AccessDenied, "expired_credential"));
            }
            let identity = credentials.into();
            let parameters = v4::SigningParams::builder()
                .identity(&identity)
                .region(context.region)
                .name(request.service)
                .time(time)
                .settings(SigningSettings::default())
                .build()
                .map_err(|_| error(ErrorCode::AccessDenied, "signing_parameters"))?
                .into();
            let values: Vec<_> = headers
                .iter()
                .map(|(name, value)| value.to_str().map(|value| (name.as_str(), value)))
                .collect::<Result<_, _>>()
                .map_err(|_| error(ErrorCode::InvalidContract, "headers"))?;
            let signable = SignableRequest::new(
                request.method,
                request.url.as_str(),
                values.into_iter(),
                SignableBody::Bytes(request.body),
            )
            .map_err(|_| error(ErrorCode::AccessDenied, "signable_request"))?;
            let (instructions, _) = sign(signable, &parameters)
                .map_err(|_| error(ErrorCode::AccessDenied, "signature"))?
                .into_parts();
            if !instructions.params().is_empty() {
                return Err(error(ErrorCode::InvalidContract, "signature_query"));
            }
            let (signed, _) = instructions.into_parts();
            for header in signed {
                let name = HeaderName::from_bytes(header.name().as_bytes())
                    .map_err(|_| error(ErrorCode::InvalidContract, "signature_header"))?;
                let mut value = HeaderValue::from_str(header.value())
                    .map_err(|_| error(ErrorCode::InvalidContract, "signature_header"))?;
                value.set_sensitive(
                    header.sensitive() || name == "authorization" || name == "x-amz-security-token",
                );
                headers.insert(name, value);
            }
        }
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    struct FixedClock;
    impl Clock for FixedClock {
        fn now(&self) -> Result<ClockReading, ContractError> {
            Ok(ClockReading {
                utc_ms: 1735689600000,
                monotonic_ms: 0,
            })
        }
        fn sleep_until<'a>(&'a self, _: u64) -> PortFuture<'a, ()> {
            Box::pin(std::future::pending())
        }
    }
    #[tokio::test]
    async fn signing_matches_an_independent_botocore_reference() {
        let scope = Scope {
            tenant_id: Id::new("tenant").unwrap(),
            workspace_id: Id::new("workspace").unwrap(),
            user_id: None,
        };
        let cancellation = Default::default();
        let credential = BedrockCredential::Aws(Credentials::new(
            "AKIDEXAMPLE",
            "test-secret",
            Some("test-session".into()),
            None,
            "fixture",
        ));
        let url =
            reqwest::Url::parse("https://bedrock-mantle.us-east-1.api.aws/anthropic/v1/messages")
                .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        let headers=authorize(&credential,BedrockCredentialContext{scope:&scope,audience:BedrockAudience::Inference,region:"us-east-1",cancellation:&cancellation,deadline:tokio::time::Instant::now()+std::time::Duration::from_secs(1)},SigningRequest{method:"POST",url:&url,body:br#"{"model":"anthropic.claude-opus-5","messages":[],"max_tokens":8,"stream":true}"#,service:"bedrock-mantle",clock:&FixedClock,headers}).await.unwrap();
        // Generated independently by botocore SigV4Auth, not the implementation under test.
        assert_eq!(
            headers["authorization"],
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20250101/us-east-1/bedrock-mantle/aws4_request, SignedHeaders=anthropic-version;content-type;host;x-amz-date;x-amz-security-token, Signature=dc66a56c9bce0eb42494f3b79f654bea31449e043e98573b9bb615dc7a1c44f1"
        );
        assert_eq!(headers["x-amz-date"], "20250101T000000Z");
        assert_eq!(headers["x-amz-security-token"], "test-session");
        assert!(headers["authorization"].is_sensitive());
        assert!(headers["x-amz-security-token"].is_sensitive());
    }
    #[tokio::test]
    async fn encoded_profile_arn_signature_matches_botocore() {
        let scope = Scope {
            tenant_id: Id::new("tenant").unwrap(),
            workspace_id: Id::new("workspace").unwrap(),
            user_id: None,
        };
        let cancellation = Default::default();
        let credential = BedrockCredential::Aws(Credentials::new(
            "AKIDEXAMPLE",
            "test-secret",
            Some("test-session".into()),
            None,
            "fixture",
        ));
        let url = reqwest::Url::parse("https://bedrock-runtime.us-east-1.amazonaws.com/model/arn:aws:bedrock:us-east-1:123456789012:inference-profile%2Fprofile/invoke-with-response-stream").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let headers = authorize(
            &credential,
            BedrockCredentialContext {
                scope: &scope,
                audience: BedrockAudience::Inference,
                region: "us-east-1",
                cancellation: &cancellation,
                deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            },
            SigningRequest {
                method: "POST",
                url: &url,
                body: br#"{"anthropic_version":"bedrock-2023-05-31","messages":[],"max_tokens":8}"#,
                service: "bedrock",
                clock: &FixedClock,
                headers,
            },
        )
        .await
        .unwrap();
        // Independent botocore reference includes canonical URI double encoding.
        assert_eq!(
            headers["authorization"],
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20250101/us-east-1/bedrock/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-security-token, Signature=6506f742d4c41cbb50b62a020f1c026d844d42a224fcec4aa114fa8489a623cc"
        );
    }
}
