# Agent Entry Point

Read the [contribution guide](CONTRIBUTING.md) before making changes. Choose the relevant guidance below; do not assume current behavior is correct merely because it is implemented.

| Task | Required reading and review focus |
| --- | --- |
| Crates, dependencies, Manager/Worker call paths | [Architecture](docs/ARCHITECTURE.md): dependency direction and behavior owners |
| Scheduling, reload, cancellation, Providers, Hooks, storage | [Architecture](docs/ARCHITECTURE.md): state and completion boundaries; read callers and failure paths first |
| HTTP/JSON, configuration, CLI, environment variables, tunasync parity | [Compatibility](docs/COMPATIBILITY.md): external contracts and coordinated changes |
| Documentation, tests, CI, or delivery | [Contribution guide](CONTRIBUTING.md): validation scope and evidence; fixtures are not proof of upstream compatibility |
| Implementation, self-review, or independent review | [Review workflow](.agents/REVIEW.md): sole writer, findings, and correction cycle |

Record HEAD, branch, and staged/dirty/untracked state before starting, and preserve existing work. Keep each change focused on one purpose; do not bundle lint cleanup, dependency refreshes, or bulk formatting. External contract changes must update the relevant specifications, examples, and tests together.

Determine authorization from the current task: local edits, local commits, pushes, PRs, and deployment are separate actions. Tests must use isolated environments rather than deployed services, production databases, or real mirror synchronization. Commands, check entry points, and validation limits belong in the contribution guide; do not duplicate that workflow here.
