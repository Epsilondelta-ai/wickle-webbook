# Wickle

[English](README.md) | [한국어](README.ko.md) | [日本語](README.ja.md) | [简体中文](README.zh-CN.md) | [Español](README.es.md) | **Français** | [Deutsch](README.de.md) | [Русский](README.ru.md)

**Un moteur d’agents extensible pour Rust.**

Intégrez des agents à votre application à partir de profils, de modèles et d’outils. Wickle exécute la boucle de décisions et d’appels d’outils dans le même processus. L’application fournit les accès aux données, les identifiants et les autorisations.

<p align="center">
  <img src="assets/mascot/wickle.png" alt="Wickle" width="320" />
</p>

## Installation

Nécessite Rust 1.85 ou supérieur et Tokio. Utilisez le même tag Git pour le cœur et les adaptateurs. La version 0.2.0 est distribuée via GitHub Releases.

```toml
[dependencies]
wickle = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
wickle-model-openai = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
wickle-model-router = { git = "https://github.com/Epsilondelta-ai/wickle", tag = "v0.2.0" }
```

## Exécuter un agent

Définissez les instructions, les outils, le modèle et les limites dans `AgentProfile`. Configurez `AgentBindings` avec le modèle, le stockage et les politiques de l’application, puis transmettez un `RunRequest` et un `ExecutionContext` authentifié.

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

`start` renvoie un handle d’exécution et `outcome` le résultat enregistré. `Guarded::ApprovalRequired` demande une approbation dans l’application. Les composants de cet exemple sont configurés par l’application ; consultez le guide des agents.

Essayez un exemple sans identifiants de modèle avec `python3 scripts/check-package.py --consumer agent` depuis une copie des sources. Consultez le [démarrage rapide](docs/quickstart.md).

## Fonctionnalités

- **Entrées séparées :** seuls les arguments du modèle sont exposés ; les identifiants fiables proviennent des entrées système.
- **Persistance :** résultats, événements, attente d’approbation ou de saisie, reprise et récupération explicites.
- **Extensions :** outils, ContextSource, Skills, Hooks et MCP stdio.
- **Limites :** appels de modèles, tentatives d’outils, corrections et durée.
- **Autorisations :** politiques du Host et isolation des organisations et espaces de travail.
- **Contexte et sorties :** artefacts, preuves, compression et validation des résultats.

- **Adaptation des schémas :** conserve les contraintes originales lors de la traduction des schémas et arguments pour chaque fournisseur.
- **Options explicites :** Profile puis Run remplacent les valeurs de la liaison ; l’origine de chaque option est conservée.
- **Interruption et inspection :** associe un état applicatif aux arrêts récupérables et permet d’inspecter les étapes enregistrées en masquant les valeurs sensibles.

## Modèles et adaptateurs

Connectez ces services avec des crates facultatifs. Les versions, déploiements, contrats API et options restent explicites. Consultez les guides pour les opérations disponibles et leurs limites.

| Provider | Crate |
| --- | --- |
| OpenAI GPT | `wickle-model-openai` |
| Azure OpenAI / Microsoft Foundry | `wickle-model-azure-openai` |
| Anthropic Claude | `wickle-model-anthropic` |
| AWS Bedrock Claude | `wickle-model-bedrock` |
| Google Gemini API / AI Studio | `wickle-model-gemini` |
| Google Vertex AI Gemini | `wickle-model-vertex` |
| xAI Grok | `wickle-model-xai` |

Utilisez `wickle-state-sqlite` pour la persistance locale et `wickle-adapter-runtime` pour composer les extensions. La mémoire et les graphes se connectent comme ContextSource ou outils ; les écritures après exécution appartiennent à un consommateur externe.

## Documentation

- [Démarrage rapide](docs/quickstart.md)
- [Migration vers v0.2.0](docs/migration-v0.2.md)
- [Contrats d’outils par fournisseur](docs/provider-tool-schemas.md)
- [Politiques d’interruption](docs/interruption-policy.md)
- [Inspection des étapes enregistrées](docs/step-inspection.md)

- [Installation](docs/installation.md)
- [Agents, requêtes et résultats](docs/agents.md)
- [Outils et entrées système](docs/tool-inputs.md)
- [Contexte et mémoire](docs/context-sources.md)
- [Skills](docs/skills.md)
- [Hooks](docs/hooks.md)
- [Routage des modèles](docs/model-routing.md)
- [Configuration et compatibilité](docs/model-providers.md)
- [SQLite et récupération](docs/sqlite-state-store.md)
- [MCP](docs/mcp.md)
- [Artefacts](docs/artifacts.md)
- [Compression du contexte](docs/context-compaction.md)
- [Validation des sorties](docs/verification.md)

## Licence

[MIT](LICENSE) · MIT © EpsilonDelta. L’API de la série 0.2 est susceptible de changer.
