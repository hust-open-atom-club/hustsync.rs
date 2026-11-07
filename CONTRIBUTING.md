# Contributing

Keep each change focused on one reviewable problem. Start with the [documentation index](docs/README.md), then read the specifications for the architecture and external contracts affected by the change. The same rules apply to agents. Preserve existing work; do not add unrelated lint cleanup, refactoring, lockfile refreshes, or bulk formatting.

## From Problem to Review

Describe the problem, scope, behavior owner, contracts to preserve, and documentation/examples/tests that must change together. Read the current implementation and its callers, distinguishing facts, inferences, and open questions. The [review workflow](.agents/REVIEW.md) is the single definition of self-review, independent review, finding disposition, and completion criteria. A successful build does not replace behavioral acceptance.

## Choose the Smallest Sufficient Validation Set

Run all commands from the repository root. Rust/Cargo must support the current edition 2024 and manifests. Lightweight checks use Bun 1.3.14, which CI pins; no npm dependencies need to be installed. For documentation-only changes, check links/paths, commands, terminology, claims, and the diff without starting a full build or test suite.

```sh
bun script/check-architecture.ts
bun test script/tests/check-architecture.test.ts
git diff --check
# After staging, check the actual patch to be delivered:
git diff --cached --check
```

The checker uses structured dependency data from `cargo metadata --offline --locked --no-deps --format-version 1` and TOML lint settings to enforce the [architecture boundaries](docs/ARCHITECTURE.md). It does not download dependencies, compile, or run mirror jobs. It checks all target-specific dependency declarations without filtering by the host platform. Isolated fixtures cover allowed and forbidden dependencies and missing lint inheritance. This gate does not run Clippy or establish behavioral correctness, link validity, or compatibility.

`.github/workflows/governance.yml` runs only the two lightweight checks above on PRs, pushes to main, and manual dispatch. The existing [Clippy workflow](.github/workflows/clippy.yml) remains `workflow_dispatch` only, with its responsibility and command unchanged. Passing governance checks does not mean existing lint issues have been resolved.

For behavior changes, select tests by ownership. These are optional entry points, not a checklist to run for every change:

| Change | Relevant validation entry point | Limitations |
| --- | --- | --- |
| Wire JSON | `cargo test --locked -p hustsync-internal --test roundtrip` | Current fixture regression, not upstream interoperability |
| Configuration loading/validation | `cargo test --locked -p hustsync-config-parser` | Also inspect the daemon's actual loading path |
| Manager HTTP | `cargo test --locked -p hustsync-manager --test contract_get --test contract_workers --test contract_manager_cmd --test contract_status_file` | Uses isolated redb; does not validate the ctl binary |
| Worker commands | `cargo test --locked -p hustsync-worker --test contract_cmd` | Select additional Provider/Hook tests and lifecycle scenarios for the change |
| Component integration | `cargo test --locked -p hustsync-manager --test e2e_inproc` | An HTTP client drives the components; the real CLI is not covered |

Read test prerequisites before running them. Redis tests execute only when `REDIS_URL` is set and issue `FLUSHDB`: point them only at a dedicated, disposable database, never a deployed database. Without that variable, do not report Redis as validated. Provider/Hook tests may require a Unix shell, permissions, and fake processes. `docs/example/test_worker.toml` contains a real mirror source and is not an automated validation input. New checkers need isolated tests for passing and representative rejection paths.

For Rust code changes, run relevant tests and formatting checks for the modified files. When lint is needed, record the actual command and result, and report existing issues separately; do not suppress or broadly exempt them to obtain a pass. Classify a failure as pre-existing only after reproducing it on the baseline under comparable conditions. Mark unrun checks as not run. A platform listed in CI configuration is not evidence of completed validation on that platform.

## Validation Records and Local Delivery

Use the [template](.github/pull_request_template.md) in contribution reports or PRs to record the baseline and final revision (or candidate diff if uncommitted), commands/results, environment, evidence level, unverified scope, and finding dispositions. Distinguish static checks, unit/integration tests, real CLI/process execution, cross-platform checks, and deployment. Do not extend evidence from one level to another.

Once local commits are authorized, split them by independently understandable, reversible purpose and follow recent commit title style. Specifications and their entry points should take effect together. Deliver check scripts, isolated fixtures, and CI as a complete feature, leaving no broken links or references to commands that do not yet exist. Use judgment for ordinary commit boundaries rather than splitting mechanically by file or review round.

Before committing, recheck HEAD, branch, worktree/index, and authorized paths; stage specific changes and read the complete staged diff. Do not include existing user work or bypass hooks. After committing, verify the actual content, order, and final tree against the reviewed candidate, and state whether validation covered the final tree or intermediate commits. Report incomplete checks and hook blockers explicitly; do not modify unrelated code to force a pass. Commits, pushes, PRs, and deployment each require the corresponding authorization.
