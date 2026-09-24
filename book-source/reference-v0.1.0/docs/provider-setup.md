# Live model test configuration

Prepare credentials and one environment-variable block per model/version using
[the model test `.env` guide](env/README.md). It links separate guides for OpenAI,
Azure OpenAI, Anthropic, Bedrock, Gemini API, Vertex AI, and xAI.

[`.env.example`](../.env.example) contains shared authentication/connection settings
and the first numbered model slot for each provider. Add further slots for more
models or versions. No provider enable-list variable is required.

[Provider adapters](model-providers.md) implement the seven documented inference paths.
Model-specific live verification is separate from local transport-contract checks.
These files and numbered model variables are test-harness conventions only.
Wickle core accepts configured adapters and does not load `.env` or depend on
these variables. The `.env` files and template are excluded from its `.crate`
package. Production Host applications choose their own configuration mechanism.
