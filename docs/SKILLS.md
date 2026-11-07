# Repository Skills

The repository provides two task-specific skills under `.agents/skills/`:

- [hustsync-change](../.agents/skills/hustsync-change/SKILL.md) supports substantive behavior/architecture investigation and authorized implementation.
- [hustsync-review](../.agents/skills/hustsync-review/SKILL.md) supports requested reviews and applicable independent review phases, read-only by default.

The skills route decisions to existing architecture, compatibility, contribution, and review specifications. They do not replace [AGENTS.md](../AGENTS.md), define a second review process, or grant action/delegation permissions. A task has one implementation/review cycle, not mutually recursive skill workflows. Ordinary wording/link corrections and Git status questions do not need the full skills.

## Discovery and Fallback

Codex documents repository discovery from the working directory up to the repository root in [Where Codex loads local skills](https://learn.chatgpt.com/docs/build-skills#where-codex-loads-local-skills). For hosts supporting repository skills, `.agents/skills/<name>/SKILL.md` supplies YAML `name` and `description` metadata for selection and a body loaded when the skill is used. This repository does not disable implicit selection or require a user-global installation. Explicit requests can name `$hustsync-change` or `$hustsync-review`.

Discovery walks toward the repository root; do not assume a session at the root recursively discovers skills placed only inside a crate. If the host cannot refresh its inventory, verify changes in a fresh session. Removing or renaming a skill file does not prove that an existing conversation has discarded instructions already loaded.

Host support, discovery, selection, and execution are different claims. Confirm the skills in the host's available-skills inventory from this checkout; file existence alone is insufficient. If they are absent, follow AGENTS.md and open the applicable SKILL.md by its repository-relative link. Report that as direct-file fallback, not successful automatic discovery or invocation. Do not alter global configuration to make a probe pass.

## Maintaining and Testing the Skills

Keep frontmatter names equal to directory names, descriptions selective, and relative file/heading links valid. Markdown links resolve relative to the containing file; links in each SKILL.md are relative to that skill's directory, while shell examples run from the repository root. Retain one authoritative definition for each project rule. When a referenced heading or trigger scope changes, update the skill and exercise the affected cases below. No installer, generated UI metadata, or dedicated test framework is required.

Record the repository revision/candidate, host/version, working directory, prompt, observed skill selection/body reads, permitted actions, result, and limitations in the task's validation report. Separate these evidence levels:

| Evidence | What to observe | What it does not prove |
| --- | --- | --- |
| Static | Valid YAML name/description, complete English instructions, resolvable relative file/heading links | Host discovery or useful decisions |
| Discovery | The native host lists each repository skill from the intended checkout | Implicit selection or execution |
| Implicit selection | A task prompt without a skill name causes the host to select/read the appropriate skill | Reliable selection for every phrasing |
| Explicit invocation | A named skill is selected/read for a realistic request | Implicit selection |
| Execution | The resulting source trace, review, or isolated edit respects scope and uses the relevant contract | Production, cross-platform, or upstream compatibility acceptance |

Use fresh sessions for selection checks so previous explicit invocation does not contaminate the result. The prompts below are behavioral probes, not wording snapshots. Use read-only cases against the checkout; permit edits only in an isolated disposable copy for the implementation case. Do not run live synchronization or services.

| Prompt | Expected routing and observable boundary |
| --- | --- |
| “Investigate whether Worker reload applies changed global concurrency. Do not edit.” | Change skill; trace loading and runtime ownership, report evidence without edits |
| “Use $hustsync-change to analyze whether repeated Disable waits for process exit. Read-only.” | Explicit change skill; distinguish response and process completion, preserve read-only scope |
| “Review this diff for changes to MirrorStatus serialization; do not fix it.” | Review skill; inspect producer/consumer and fixture evidence, return findings without edits |
| “Use $hustsync-review to review the current governance diff for maintainability.” | Explicit review skill; assess duplication and rule ownership without starting implementation |
| “Fix this broken documentation link only.” | Narrow edit under AGENTS.md; do not start a substantive change/review workflow |
| “Which branch is checked out, and is the worktree clean?” | Direct read-only Git inspection; neither skill is needed |
| “In this disposable copy, add documentation for a proposed CLI contract change; do not change behavior or commit.” | Change skill; trace the contract, make only the authorized documentation edit, and report validation limits |

A failed or unavailable host probe remains a limitation; passing static validation or a direct-file exercise must not be relabeled as native invocation success.
