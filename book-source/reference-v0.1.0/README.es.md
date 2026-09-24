# Wickle

[English](README.md) | [한국어](README.ko.md) | [日本語](README.ja.md) | [简体中文](README.zh-CN.md) | **Español** | [Français](README.fr.md) | [Deutsch](README.de.md) | [Русский](README.ru.md)

**Un motor de agentes extensible para Rust.**

Integra agentes en tu aplicación mediante perfiles, modelos y herramientas. Wickle ejecuta el ciclo de decisiones y llamadas a herramientas dentro del mismo proceso. La aplicación proporciona acceso a datos, credenciales y autorización.

<p align="center">
  <img src="assets/mascot/wickle.png" alt="Wickle" width="320" />
</p>

## Instalación

Requiere Rust 1.85 o posterior y Tokio. Usa la misma etiqueta Git para el núcleo y los adaptadores. v0.1.0 se distribuye mediante GitHub Releases.

```toml
[dependencies]
wickle = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
wickle-model-openai = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
wickle-model-router = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.1.0" }
```

## Ejecutar un agente

Define instrucciones, herramientas, modelo y límites en `AgentProfile`. Configura `AgentBindings` con el modelo, almacenamiento y políticas de tu aplicación; entrega un `RunRequest` junto con un `ExecutionContext` autenticado.

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

`start` devuelve un identificador de ejecución y `outcome` su resultado guardado. `Guarded::ApprovalRequired` indica que la aplicación debe obtener aprobación. El ejemplo recibe componentes previamente configurados; consulta la guía de agentes para prepararlos.

## Características

- **Entradas separadas:** el modelo aporta sus argumentos; los identificadores de confianza se inyectan desde entradas del sistema.
- **Persistencia:** resultados, eventos, esperas de aprobación o entrada, reanudación y recuperación explícitas.
- **Extensiones:** herramientas, ContextSource, Skills, Hooks y MCP stdio.
- **Límites:** llamadas al modelo, intentos de herramientas, correcciones y tiempo de ejecución.
- **Autorización:** políticas del Host y ámbitos de organización y espacio de trabajo.
- **Contexto y resultados:** artefactos, evidencias, compresión y validación de salidas.

## Proveedores y adaptadores

Conecta estos servicios mediante crates opcionales. Configura explícitamente versiones de modelo, despliegues, contratos API y opciones. Consulta las guías para conocer operaciones y limitaciones.

| Provider | Crate |
| --- | --- |
| OpenAI GPT | `wickle-model-openai` |
| Azure OpenAI / Microsoft Foundry | `wickle-model-azure-openai` |
| Anthropic Claude | `wickle-model-anthropic` |
| AWS Bedrock Claude | `wickle-model-bedrock` |
| Google Gemini API / AI Studio | `wickle-model-gemini` |
| Google Vertex AI Gemini | `wickle-model-vertex` |
| xAI Grok | `wickle-model-xai` |

Usa `wickle-state-sqlite` para persistencia local y `wickle-adapter-runtime` para componer extensiones. Los servicios de memoria y grafos se conectan como ContextSource o herramientas; un consumidor externo gestiona las escrituras posteriores.

## Documentación

- [Instalación](docs/installation.md)
- [Agentes, solicitudes y resultados](docs/agents.md)
- [Herramientas y entradas del sistema](docs/tool-inputs.md)
- [Contexto y memoria](docs/context-sources.md)
- [Skills](docs/skills.md)
- [Hooks](docs/hooks.md)
- [Enrutamiento](docs/model-routing.md)
- [Proveedores y compatibilidad](docs/model-providers.md)
- [SQLite y recuperación](docs/sqlite-state-store.md)
- [MCP](docs/mcp.md)
- [Artefactos](docs/artifacts.md)
- [Compresión del contexto](docs/context-compaction.md)
- [Validación de resultados](docs/verification.md)

## Licencia

[MIT](LICENSE) · MIT © EpsilonDelta. La serie 0.1 es una API inicial y puede cambiar.
