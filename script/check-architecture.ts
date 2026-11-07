#!/usr/bin/env bun
/** Manifest-only enforcement of docs/ARCHITECTURE.md; no build or network. */
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";

const PREFIX = "hustsync-";
const NORMAL: Record<string, readonly string[]> = {
  "hustsync-config-parser": [],
  "hustsync-internal": [],
  "hustsync-manager": [`${PREFIX}config-parser`, `${PREFIX}internal`],
  "hustsync-worker": [`${PREFIX}config-parser`, `${PREFIX}internal`],
  hustsync: [
    `${PREFIX}config-parser`, `${PREFIX}internal`, `${PREFIX}manager`, `${PREFIX}worker`,
  ],
  hustsynctl: [`${PREFIX}internal`],
};

interface Dependency {
  name: string;
  rename: string | null;
  kind: "dev" | "build" | null;
  target: string | null;
}
interface Package {
  id: string;
  name: string;
  manifest_path: string;
  dependencies: Dependency[];
}
interface Metadata {
  packages: Package[];
  workspace_members: string[];
  workspace_root: string;
}

export function checkArchitecture(root: string): string[] {
  const result = Bun.spawnSync([
    "cargo", "metadata", "--offline", "--locked", "--no-deps", "--format-version", "1",
    "--manifest-path", join(root, "Cargo.toml"),
  ], { cwd: root });
  if (result.exitCode !== 0) {
    throw new Error(`cargo metadata failed:\n${result.stderr.toString()}`);
  }
  const metadata = JSON.parse(result.stdout.toString()) as Metadata;
  const ids = new Set(metadata.workspace_members);
  const members = metadata.packages.filter((pkg) => ids.has(pkg.id));
  const names = new Set(members.map((pkg) => pkg.name));
  const errors: string[] = [];
  for (const expected of Object.keys(NORMAL)) {
    if (!names.has(expected)) errors.push(`missing workspace member: ${expected}`);
  }
  const workspace = Bun.TOML.parse(
    readFileSync(join(metadata.workspace_root, "Cargo.toml"), "utf8"),
  ) as { workspace?: { lints?: Record<string, unknown> } };
  if (!workspace.workspace?.lints || Object.keys(workspace.workspace.lints).length === 0) {
    errors.push("workspace must define shared lints");
  }
  for (const pkg of members) {
    if (!(pkg.name in NORMAL)) {
      errors.push(`unclassified workspace member: ${pkg.name}; update the architecture contract`);
      continue;
    }
    const manifest = Bun.TOML.parse(readFileSync(pkg.manifest_path, "utf8")) as {
      lints?: { workspace?: boolean };
    };
    if (manifest.lints?.workspace !== true) {
      errors.push(`${pkg.name}: must inherit [lints] workspace = true`);
    }
    for (const dependency of pkg.dependencies) {
      // Metadata reports the package name separately from its local alias and
      // includes optional and target-specific declarations without host filtering.
      if (!names.has(dependency.name)) continue;
      const allowed = dependency.kind === "build" ? [] : NORMAL[pkg.name];
      const integrationException = dependency.kind === "dev"
        && pkg.name === "hustsync-manager" && dependency.name === "hustsync-worker";
      if (!allowed.includes(dependency.name) && !integrationException) {
        const alias = dependency.rename ? ` (alias ${dependency.rename})` : "";
        errors.push(
          `${pkg.name} -> ${dependency.name}${alias}: forbidden ${dependency.kind ?? "normal"}`
          + ` dependency [target: ${dependency.target ?? "all"}]`,
        );
      }
    }
  }
  return errors;
}

if (import.meta.main) {
  try {
    const root = resolve(process.argv[2] ?? join(dirname(import.meta.path), ".."));
    const errors = checkArchitecture(root);
    if (errors.length > 0) {
      for (const error of errors) console.error(`FAIL: ${error}`);
      process.exitCode = 1;
    } else {
      console.log("PASS: workspace dependency boundaries and lint inheritance (manifest checks only)");
    }
  } catch (error) {
    console.error(`FAIL: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}
