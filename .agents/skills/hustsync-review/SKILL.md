---
name: hustsync-review
description: Review a proposed or existing hustsync.rs change when the user requests code or contract review, or the task requires an independent review phase. Read-only by default. Not for ordinary implementation self-review, wording/link fixes, or Git status queries.
---

# HustSync Review

Review the requested candidate against its behavior contract and actual implementation. Stay in the review phase of the existing task; do not launch another implementation workflow or recursively restart this review.

## Bind the Review

Establish the requested scope, baseline, candidate diff, and relevant validation evidence. Choose the applicable rows below before opening specifications. Locate headings first (for example, from the repository root, `rg -n '^## ' docs/ARCHITECTURE.md`), then use bounded line ranges to read the selected sections and [independent review requirements](../../REVIEW.md#c-independent-review). Do not batch-read entire architecture, compatibility, and index documents; expand only when the candidate or source trace exposes another relevant boundary. Distinguish staged, unstaged, and untracked content so conclusions identify what was reviewed. For a design without a diff, state that the review is of the proposal, not an implementation.

The default is read-only. Findings do not authorize fixes, staging, commits, pushes, PR changes, deployment, or additional agents. Return accepted corrections to the task's designated writer. If the user also authorizes repair, retain the existing writer and correction workflow rather than editing from the reviewer role.

## Select the Review Lens

Use the assigned correctness or simplification lens. If working alone, state the actual review scope and lack of independent reviewers; do not claim two independent passes. Reviewer roles and the correction cycle remain defined in [the review workflow](../../REVIEW.md#d-corrections-and-follow-up-review).

| Candidate surface | Read | Review question |
| --- | --- | --- |
| Dependency or crate changes | [Dependency boundaries](../../../docs/ARCHITECTURE.md#crate-dependency-boundaries) | Does the edge preserve ownership, including dev/build, aliases, optional dependencies, and target conditions? |
| State or storage changes | [Ownership and state](../../../docs/ARCHITECTURE.md#ownership-and-state), [startup/reload](../../../docs/ARCHITECTURE.md#startup-reload-and-failure) | Are persisted, runtime, schedule, and protocol states distinguished through failure and recovery? |
| Commands or process lifecycle | [Command completion](../../../docs/ARCHITECTURE.md#commands-execution-and-reporting), [cleanup ownership](../../../docs/ARCHITECTURE.md#concurrency-retries-and-cleanup) | Do acceptance, process exit, persistence, and permit release have evidence for the claimed boundary? |
| HTTP/JSON, config, CLI, env, Provider compatibility | [Contract scope](../../../docs/COMPATIBILITY.md#external-contracts-and-current-scope), [synchronization requirements](../../../docs/COMPATIBILITY.md#coordinated-changes-to-contracts) | Do implementation, consumers, documentation, examples, and tests express the same change? |
| Compatibility or test claims | [Upstream baseline](../../../docs/COMPATIBILITY.md#goal-and-baseline), [fixture evidence](../../../docs/COMPATIBILITY.md#fixture-provenance-and-evidence-limits), [validation limits](../../../CONTRIBUTING.md#choose-the-smallest-sufficient-validation-set) | Does the evidence actually exercise this revision, interface, platform, and runtime path? |

For correctness, independently inspect the affected source, callers, manifests, tests, and CI conditions. A static assertion or existing test name is not runtime acceptance. Check relevant [open items](../../../docs/ARCHITECTURE.md#open-behavior-questions-and-impact) without expanding the review into an unrelated repair campaign.

For simplification, check whether each new layer or rule has a current owner and purpose. Prefer one canonical contract with links over copied checklists. Remove complexity only when the proposal preserves the same behavior and evidence boundary; smaller code is not sufficient if it weakens lifecycle or compatibility guarantees.

## Report and Recheck

Use the canonical review order and finding fields in the independent review requirements above. Separate required corrections from optional suggestions; state when there are no findings. Report what you actually checked and what remains unverified.

Follow [correction and follow-up review](../../REVIEW.md#d-corrections-and-follow-up-review) for dispositions, changed candidates, and adjacent effects. A later edit invalidates conclusions about affected content; unchanged content does not require another ceremonial pass. Local delivery, if separately authorized, follows [delivery guidance](../../../CONTRIBUTING.md#validation-records-and-local-delivery) and does not grant the reviewer Git write ownership.
