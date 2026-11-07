---
name: hustsync-change
description: Investigate or implement substantive hustsync.rs behavior and architecture changes involving Manager/Worker state, crate boundaries, or external contracts. Use for scoped diagnosis or authorized implementation, not routine wording/link fixes, Git status queries, or standalone review requests.
---

# HustSync Change

Resolve the requested behavior or architecture question using the actual owner and call path. Keep one workflow for the task; the review phase belongs to that workflow, not a second implementation loop.

## Select the Mode and References

An investigation-only request stays read-only and returns evidence and open decisions. An implementation request authorizes changes within its stated scope; a proposed fix discovered during investigation does not. Skill selection grants no permission to commit, push, create a PR, deploy, or start additional agents.

Choose the applicable rows below before opening specifications. Locate headings first (for example, from the repository root, `rg -n '^## ' docs/ARCHITECTURE.md`), then read only the selected sections with bounded line ranges, including [before editing](../../REVIEW.md#a-before-editing). Do not batch-read entire architecture, compatibility, and index documents. Expand to another section only when the source trace reveals a relevant boundary. Follow specification links to current source and tests rather than treating the specification as proof of correctness.

| Change surface | Canonical reading | Decision to resolve |
| --- | --- | --- |
| Crate or dependency boundary | [Dependency boundaries](../../../docs/ARCHITECTURE.md#crate-dependency-boundaries) | Is this a production, development, or build edge, and does the exception apply under every target condition? |
| Scheduling, recovery, reload, Manager reachability | [Ownership](../../../docs/ARCHITECTURE.md#ownership-and-state), [startup and failure](../../../docs/ARCHITECTURE.md#startup-reload-and-failure) | Which persisted, runtime, scheduling, or protocol state changes, and which owner reconstructs it? |
| Commands, cancellation, Provider/Hook execution | [Command path](../../../docs/ARCHITECTURE.md#commands-execution-and-reporting), [cleanup](../../../docs/ARCHITECTURE.md#concurrency-retries-and-cleanup) | What does the response acknowledge, who waits for exit, and when is the permit released? |
| HTTP/JSON, configuration, CLI, environment variables, Providers, storage compatibility | [External contracts](../../../docs/COMPATIBILITY.md#external-contracts-and-current-scope), [coordinated changes](../../../docs/COMPATIBILITY.md#coordinated-changes-to-contracts) | Which callers and observable inputs/outputs must change together? |
| Claimed tunasync parity | [Baseline](../../../docs/COMPATIBILITY.md#goal-and-baseline), [fixture provenance](../../../docs/COMPATIBILITY.md#fixture-provenance-and-evidence-limits) | Is the evidence a local regression fixture or a comparison against an identified upstream revision? |

Read only the relevant [open behavior questions](../../../docs/ARCHITECTURE.md#open-behavior-questions-and-impact). They are investigation leads, not permission to fix unrelated behavior or evidence that a desired guarantee already holds.

## Trace Before Changing

Trace from the request's entry point through its state owner to reporting and recovery. For a command, include CLI encoding when applicable, Manager forwarding/prewrites, Worker queue changes, actor execution, and result delivery. For configuration, distinguish loading/validation from the runtime consumer and reload scope.

State the requested behavior and the smallest change boundary that preserves unrelated behavior. If changing an external contract is necessary, describe old/new behavior and apply the canonical coordinated-change checklist above. Do not infer upstream compatibility from a fixture, process exit from HTTP success, or full shutdown from joining actors.

## Implement and Close the Same Task

For authorized implementation, apply the [self-review and correction workflow](../../REVIEW.md#b-initial-implementer-self-review). Keep its A–E process authoritative rather than reproducing it in task notes. Use independent reviewers only when applicable and authorized; if unavailable, complete the available self-review and report the missing independent gate.

Choose checks from [validation guidance](../../../CONTRIBUTING.md#choose-the-smallest-sufficient-validation-set). Match evidence to the changed boundary: manifests, fixtures, component HTTP, real CLI/process execution, and platform behavior prove different things. Use isolated fixtures and disposable services; examples with live mirror sources are not test inputs.

In investigation mode, finish with source-backed findings, their impact, unresolved decisions, and checks performed or omitted. In implementation mode, finish against the [completion criteria](../../REVIEW.md#e-completion-criteria), identifying synchronized contracts, validation limits, and the exact authorized delivery state. Do not silently turn an investigation into implementation or an implementation into publication.
