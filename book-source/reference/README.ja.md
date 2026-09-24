# Wickle

[English](README.md) | [한국어](README.ko.md) | **日本語** | [简体中文](README.zh-CN.md) | [Español](README.es.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

**Rust アプリケーションに組み込む拡張可能なエージェントエンジン。**

プロファイル、モデル、ツールを組み合わせてエージェントを実行します。Wickle はアプリケーションと同じプロセスでモデルの判断とツール呼び出しを繰り返し、データアクセス・認証情報・権限はアプリケーションが提供します。

<p align="center">
  <img src="assets/mascot/wickle.png" alt="Wickle" width="320" />
</p>

## インストール

Rust 1.85 以上と Tokio ランタイムが必要です。コアとアダプターには同じ Git タグを使用します。v0.2.0 は GitHub Releases で配布します。

```toml
[dependencies]
wickle = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
wickle-model-openai = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
wickle-model-router = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
```

## エージェントの実行

`AgentProfile` に指示、ツール、モデルバインディング、実行上限を設定します。モデル・状態ストア・権限ポリシーで `AgentBindings` を構成し、認証済みの `ExecutionContext` と `RunRequest` を渡します。

```rust
use wickle::*;

pub async fn run_once(
    profile: AgentProfile,
    bindings: AgentBindings,
    request: RunRequest,
    context: ExecutionContext,
) -> Result<Guarded<RunOutcome>, ContractError> {
    let agent = create_agent(profile, bindings)?;
    match agent.start(request, context.clone()).await? {
        Guarded::Completed(handle) => handle.outcome(&context).await,
        Guarded::ApprovalRequired(challenge) => {
            Ok(Guarded::ApprovalRequired(challenge))
        }
    }
}
```

`start` は実行ハンドル、`outcome` は保存済みの結果を返します。`Guarded::ApprovalRequired` は承認が必要な状態です。例の構成要素はアプリケーション側で用意します。詳細は実行ガイドを参照してください。

モデルの認証情報なしで試すには、ソースを取得して `python3 scripts/check-package.py --consumer agent` を実行します。手順は[クイックスタート](docs/quickstart.md)を参照してください。

## 主な機能

- **ツール入力の分離:** モデル用引数だけを公開し、workspace ID などはシステム入力から注入します。
- **状態の永続化:** 結果とイベント、承認・入力待ち、明示的な再開と復旧を扱います。
- **拡張:** ツール、ContextSource、Skills、Hooks、MCP stdio を接続します。
- **実行上限:** モデル呼び出し、ツール試行、修正、経過時間を制限します。
- **アクセス制御:** Host ポリシーで組織とワークスペースの範囲を分離します。
- **文脈と出力:** アーティファクトと根拠、文脈圧縮、構造化出力、verifier に対応します。

- **ツールスキーマの変換:** 元の入力制約を保ち、プロバイダーごとのスキーマと引数表現に変換します。
- **明示的なモデルオプション:** バインディングの既定値を Profile、Run の順に上書きし、設定の由来を保存します。
- **中断と診断:** 復旧可能な中断にアプリ状態を記録し、機密値を伏せた保存済みステップを確認できます。

## モデルとアダプター

以下のサービスを個別の crate で接続できます。モデルバージョン、デプロイ名、API 契約、オプションを明示します。対応操作と制限は各ガイドを確認してください。

| Provider | Crate |
| --- | --- |
| OpenAI GPT | `wickle-model-openai` |
| Azure OpenAI / Microsoft Foundry | `wickle-model-azure-openai` |
| Anthropic Claude | `wickle-model-anthropic` |
| AWS Bedrock Claude | `wickle-model-bedrock` |
| Google Gemini API / AI Studio | `wickle-model-gemini` |
| Google Vertex AI Gemini | `wickle-model-vertex` |
| xAI Grok | `wickle-model-xai` |

ローカル永続化には `wickle-state-sqlite`、拡張の構成には `wickle-adapter-runtime` を使用します。メモリやグラフは ContextSource またはツールとして接続し、実行後の更新は外部イベントコンシューマーが担当します。

## ドキュメント

- [クイックスタート](docs/quickstart.md)
- [v0.2.0 への移行](docs/migration-v0.2.md)
- [プロバイダー別ツール契約](docs/provider-tool-schemas.md)
- [中断ポリシー](docs/interruption-policy.md)
- [保存済みステップの診断](docs/step-inspection.md)

- [依存関係の設定](docs/installation.md)
- [バインディング・リクエスト・結果](docs/agents.md)
- [ツールとシステム入力](docs/tool-inputs.md)
- [文脈とメモリ](docs/context-sources.md)
- [Skills](docs/skills.md)
- [Hooks](docs/hooks.md)
- [モデルルーティング](docs/model-routing.md)
- [モデル設定と対応範囲](docs/model-providers.md)
- [SQLite と復旧](docs/sqlite-state-store.md)
- [MCP ツール](docs/mcp.md)
- [Artifacts](docs/artifacts.md)
- [文脈圧縮](docs/context-compaction.md)
- [出力検証](docs/verification.md)

## ライセンス

[MIT](LICENSE) · MIT © EpsilonDelta. 0.2 系の API は今後変更される場合があります。
