# Development Documentation

- [Architecture and implementation constraints](ARCHITECTURE.md): crate boundaries, behavior owners, state, and lifecycle.
- [Compatibility contracts](COMPATIBILITY.md): external interfaces, upstream baseline, fixtures, and evidence limits.
- [Contribution guide](../CONTRIBUTING.md): selecting validation, reporting results, and splitting commits.
- [Agent entry point](../AGENTS.md) and [review workflow](../.agents/REVIEW.md): task-specific reading and the correction cycle.
- [Repository skills](SKILLS.md): discovery, invocation boundaries, and behavioral validation cases.
- [User guides](guide/get_started.md): getting started, deployment, configuration and each feature.
- [Configuration examples](example/): check paths, mirror sources, and the runtime environment before use; these are not deployment certification.

## How to Read These Specifications

The initial source, manifest, test, and CI review for these documents used `decee897b1306d65016bdafcad744eeb75e0041a`. Later changes must update affected sections. “Current implementation” means verified by reading the source, not that behavioral tests ran during this documentation work.

- **Verified behavior** requires a specific revision, command, result, and environment. An existing test file only identifies intended coverage.
- **Current implementation** describes behavior traceable to source; runtime and platform evidence must be reported separately.
- **Constraint** is an accepted requirement for changes. It does not establish that all existing implementations satisfy it.
- **Open item** lacks sufficient evidence or awaits a decision or implementation. Do not present it as a supported capability.

Architecture documents retain durable boundaries and failure semantics; complete field lists, signatures, and local algorithms belong in source/API comments. Keep task progress, review findings, and validation records in contribution reports or PRs rather than creating duplicate specifications. The root README provides a quick entry point. Maintain project documentation in English; parallel bilingual copies are not required.
