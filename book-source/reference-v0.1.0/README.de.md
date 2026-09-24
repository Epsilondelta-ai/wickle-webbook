# Wickle

[English](README.md) | [한국어](README.ko.md) | [日本語](README.ja.md) | [简体中文](README.zh-CN.md) | [Español](README.es.md) | [Français](README.fr.md) | **Deutsch** | [Русский](README.ru.md)

**Eine erweiterbare Agenten-Engine für Rust.**

Bette Agenten mit konfigurierbaren Profilen, Modellen und Werkzeugen in deine Anwendung ein. Wickle führt Modellentscheidungen und Werkzeugaufrufe im selben Prozess aus. Datenzugriff, Zugangsdaten und Berechtigungen stellt die Anwendung bereit.

<p align="center">
  <img src="assets/mascot/wickle.png" alt="Wickle" width="320" />
</p>

## Installation

Benötigt Rust 1.85 oder neuer und Tokio. Verwende denselben Git-Tag für den Kern und die Adapter. v0.1.0 wird über GitHub Releases verteilt.

```toml
[dependencies]
wickle = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
wickle-model-openai = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
wickle-model-router = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
```

## Einen Agenten ausführen

Lege Anweisungen, Werkzeuge, Modellbindung und Grenzen im `AgentProfile` fest. Konfiguriere `AgentBindings` mit Modell, Zustandsspeicher und Richtlinien und übergib einen `RunRequest` mit authentifiziertem `ExecutionContext`.

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

`start` liefert ein Ausführungshandle, `outcome` das gespeicherte Ergebnis. `Guarded::ApprovalRequired` erfordert eine Freigabe durch die Anwendung. Das Beispiel nimmt bereits konfigurierte Komponenten entgegen; deren Einrichtung erklärt die Agentenanleitung.

## Funktionen

- **Getrennte Eingaben:** Das Modell liefert seine Argumente; vertrauenswürdige IDs werden aus Systemeingaben ergänzt.
- **Persistenz:** Ergebnisse, Ereignisse, Freigabe- und Eingabewartezustände sowie explizite Wiederaufnahme und Wiederherstellung.
- **Erweiterungen:** Werkzeuge, ContextSource, Skills, Hooks und MCP stdio.
- **Ausführungsgrenzen:** Modellaufrufe, Werkzeugversuche, Korrekturen und Laufzeit.
- **Zugriffskontrolle:** Host-Richtlinien und getrennte Organisations- und Arbeitsbereichsgrenzen.
- **Kontext und Ausgabe:** Artefakte, Belege, Kontextkomprimierung und Ergebnisvalidierung.

## Modellanbieter und Adapter

Diese Dienste lassen sich über optionale Crates anbinden. Modellversionen, Deployments, API-Verträge und Optionen werden explizit konfiguriert. Die Anleitungen beschreiben unterstützte Operationen und Einschränkungen.

| Provider | Crate |
| --- | --- |
| OpenAI GPT | `wickle-model-openai` |
| Azure OpenAI / Microsoft Foundry | `wickle-model-azure-openai` |
| Anthropic Claude | `wickle-model-anthropic` |
| AWS Bedrock Claude | `wickle-model-bedrock` |
| Google Gemini API / AI Studio | `wickle-model-gemini` |
| Google Vertex AI Gemini | `wickle-model-vertex` |
| xAI Grok | `wickle-model-xai` |

Verwende `wickle-state-sqlite` für lokale Persistenz und `wickle-adapter-runtime` zum Zusammenstellen von Erweiterungen. Speicher- und Graphdienste können ContextSource oder Werkzeuge implementieren; nachgelagerte Schreibvorgänge übernimmt ein externer Ereigniskonsument.

## Dokumentation

- [Installation](docs/installation.md)
- [Agenten, Anfragen und Ergebnisse](docs/agents.md)
- [Werkzeuge und Systemeingaben](docs/tool-inputs.md)
- [Kontext und Speicher](docs/context-sources.md)
- [Skills](docs/skills.md)
- [Hooks](docs/hooks.md)
- [Modellrouting](docs/model-routing.md)
- [Anbieterkonfiguration und Unterstützung](docs/model-providers.md)
- [SQLite und Wiederherstellung](docs/sqlite-state-store.md)
- [MCP](docs/mcp.md)
- [Artefakte](docs/artifacts.md)
- [Kontextkomprimierung](docs/context-compaction.md)
- [Ausgabevalidierung](docs/verification.md)

## Lizenz

[MIT](LICENSE) · MIT © EpsilonDelta. Die 0.1-Reihe ist eine erste API und kann sich ändern.
