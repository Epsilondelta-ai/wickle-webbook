# Wickle

[English](README.md) | [한국어](README.ko.md) | [日本語](README.ja.md) | [简体中文](README.zh-CN.md) | [Español](README.es.md) | [Français](README.fr.md) | [Deutsch](README.de.md) | **Русский**

**Расширяемый движок агентов для Rust.**

Встраивайте агентов в приложение с помощью профилей, моделей и инструментов. Wickle выполняет цикл решений модели и вызовов инструментов в том же процессе. Приложение предоставляет доступ к данным, учётные данные и правила авторизации.

<p align="center">
  <img src="assets/mascot/wickle.png" alt="Wickle" width="320" />
</p>

## Установка

Требуются Rust 1.85 или новее и Tokio. Используйте один Git-тег для ядра и адаптеров. Версия 0.1.0 распространяется через GitHub Releases.

```toml
[dependencies]
wickle = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
wickle-model-openai = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
wickle-model-router = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
```

## Запуск агента

Задайте инструкции, инструменты, модель и ограничения в `AgentProfile`. Настройте `AgentBindings` с моделью, хранилищем и политиками приложения, затем передайте `RunRequest` с аутентифицированным `ExecutionContext`.

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

`start` возвращает дескриптор выполнения, а `outcome` — сохранённый результат. `Guarded::ApprovalRequired` означает, что приложение должно получить одобрение. Пример принимает готовые компоненты; настройка описана в руководстве по агентам.

## Возможности

- **Разделение входов:** модель задаёт только свои аргументы; доверенные ID поступают из системных входов.
- **Сохранение состояния:** результаты, события, ожидание одобрения или ввода, явное продолжение и восстановление.
- **Расширения:** инструменты, ContextSource, Skills, Hooks и MCP stdio.
- **Ограничения:** вызовы модели, попытки инструментов, исправления и время выполнения.
- **Контроль доступа:** политики Host и изоляция организаций и рабочих пространств.
- **Контекст и результаты:** артефакты, доказательства, сжатие контекста и проверка вывода.

## Модели и адаптеры

Эти сервисы подключаются отдельными crate. Версии моделей, развёртывания, API-контракты и параметры задаются явно. Поддерживаемые операции и ограничения описаны в руководствах.

| Provider | Crate |
| --- | --- |
| OpenAI GPT | `wickle-model-openai` |
| Azure OpenAI / Microsoft Foundry | `wickle-model-azure-openai` |
| Anthropic Claude | `wickle-model-anthropic` |
| AWS Bedrock Claude | `wickle-model-bedrock` |
| Google Gemini API / AI Studio | `wickle-model-gemini` |
| Google Vertex AI Gemini | `wickle-model-vertex` |
| xAI Grok | `wickle-model-xai` |

Используйте `wickle-state-sqlite` для локального хранения и `wickle-adapter-runtime` для сборки расширений. Память и графы подключаются как ContextSource или инструменты; запись после выполнения выполняет внешний обработчик событий.

## Документация

- [Установка](docs/installation.md)
- [Агенты, запросы и результаты](docs/agents.md)
- [Инструменты и системные входы](docs/tool-inputs.md)
- [Контекст и память](docs/context-sources.md)
- [Skills](docs/skills.md)
- [Hooks](docs/hooks.md)
- [Маршрутизация моделей](docs/model-routing.md)
- [Настройка и поддержка провайдеров](docs/model-providers.md)
- [SQLite и восстановление](docs/sqlite-state-store.md)
- [MCP](docs/mcp.md)
- [Артефакты](docs/artifacts.md)
- [Сжатие контекста](docs/context-compaction.md)
- [Проверка вывода](docs/verification.md)

## Лицензия

[MIT](LICENSE) · MIT © EpsilonDelta. Серия 0.1 содержит начальную версию API, которая может изменяться.
