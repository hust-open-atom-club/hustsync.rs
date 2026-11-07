# Implementation and Review Workflow

This workflow applies to agent implementation and project review. Small text corrections still require self-review; independent review may be scoped to the risk. For substantive changes involving multiple crates, external contracts, scheduling/storage/process lifecycle, or governance rules, use a coordinator, a sole implementer, an independent correctness reviewer, and an independent simplification reviewer. These roles may run sequentially when resources are limited. Do not describe the implementer's self-review as independent review; report unavailable roles explicitly.

## A. Before Editing

The implementer states the problem, scope, affected contracts, behavior owner, and validation plan; records HEAD, branch, and worktree state; and reads the latest implementation, callers, and existing tests. List specifications, examples, and tests that must change together, separating facts, inferences, and open questions. The coordinator maintains file ownership and unresolved items rather than writing a second set of specifications.

## B. Initial Implementer Self-Review

Reread the complete diff, including new files. Check for omitted requirements and scope violations; consistency between source, specifications, commands, and CI; accurate links/paths, terminology, and examples; and evidence for validation claims. Remove duplicate rules and unrelated changes before handing the work to independent reviewers.

## C. Independent Review

The correctness reviewer independently reads the final diff, source, manifests, and relevant tests/CI, checking descriptions, contracts, commands, platform claims, and check logic. The simplification reviewer checks rule ownership, duplication, reading cost, necessity, and maintenance burden. Reviewers are read-only: they do not edit files, stage, or commit. Return findings to the implementer. Each review presents its conclusions in this order:

1. Purpose and scope of the change.
2. Whether the requirement and approach are reasonable.
3. Concrete findings: file and location, trigger or contradictory evidence, actual impact, severity, and the smallest correction.
4. Validation performed and its limitations.

Distinguish required fixes from optional suggestions. Explicitly state when there are no findings; do not invent findings to satisfy the format or report only “passed.”

## D. Corrections and Follow-Up Review

The coordinator consolidates findings. The implementer marks each as “fixed,” “no change needed” with evidence, or “out of scope; follow up separately.” A product behavior issue does not automatically authorize expanding a governance task. After corrections, self-review the complete final diff again. The original reviewers verify closure and adjacent effects; substantive structural or contract changes require renewed review of affected areas. Changes made after review invalidate the previous approval for those changes. Avoid unproductive cycles: follow-up review should be driven by actual edits, unresolved findings, or new risks.

When commits are authorized, the coordinator states each commit's purpose, title, files/hunks, dependency order, and validation evidence. The correctness reviewer checks that intermediate states remain complete; the simplification reviewer checks for excessive fragmentation or mixed purposes. The sole implementer performs Git writes and verifies the staged candidate and final tree under the [contribution guide](../CONTRIBUTING.md).

## E. Completion Criteria

All required fixes within scope are closed; significant disagreements have evidence-backed dispositions; necessary checks pass or specific reasons for non-completion are recorded; and documentation, examples, and checks match the final changes. Assess each task requirement rather than substituting “it compiles” or “the reviewer found no issues.” The final record includes changed scope, validation and limitations, finding dispositions, follow-up items, and separate local and remote delivery states.
