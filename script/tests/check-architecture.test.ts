import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { checkArchitecture } from "../check-architecture";

const roots: string[] = [];
const members = [
  "hustsync", "hustsynctl", "hustsync-config-parser", "hustsync-internal",
  "hustsync-manager", "hustsync-worker",
];

// Real Cargo metadata over isolated, dependency-free fixture workspaces. No
// changes to the checkout, registry access, compilation, or external services.
function fixture(): string {
  const root = mkdtempSync("/tmp/hustsync-architecture-");
  roots.push(root);
  writeFileSync(join(root, "Cargo.toml"), `[workspace]\nresolver = "3"\nmembers = [${members.map((name) => `"${name}"`).join(", ")}]\n[workspace.lints.rust]\nunsafe_code = "warn"\n`);
  for (const name of members) {
    mkdirSync(join(root, name, "src"), { recursive: true });
    writeFileSync(join(root, name, "src/lib.rs"), "");
    writeFileSync(join(root, name, "Cargo.toml"), `[package]\nname = "${name}"\nversion = "0.1.0"\nedition = "2024"\n[lints]\nworkspace = true\n`);
  }
  return root;
}

function append(root: string, member: string, toml: string) {
  const path = join(root, member, "Cargo.toml");
  writeFileSync(path, `${readFileSync(path, "utf8")}\n${toml}\n`);
}

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

test("accepts production boundaries, renamed target dependency and Manager dev exception", () => {
  const root = fixture();
  append(root, "hustsync", '[dependencies]\nhustsync-worker = { path = "../hustsync-worker" }');
  append(root, "hustsync-manager", '[dependencies]\nhustsync-internal = { path = "../hustsync-internal" }\n[dev-dependencies]\nhustsync-worker = { path = "../hustsync-worker" }');
  append(root, "hustsynctl", '[target.\'cfg(windows)\'.dependencies]\nwire = { package = "hustsync-internal", path = "../hustsync-internal", optional = true }');
  expect(checkArchitecture(root)).toEqual([]);
});

test("rejects Manager to Worker in production even behind foreign target and alias", () => {
  const root = fixture();
  append(root, "hustsync-manager", '[target.\'cfg(windows)\'.dependencies]\nengine = { package = "hustsync-worker", path = "../hustsync-worker", optional = true }');
  expect(checkArchitecture(root)).toEqual([
    'hustsync-manager -> hustsync-worker (alias engine): forbidden normal dependency [target: cfg(windows)]',
  ]);
});

test("rejects reverse foundational dependency", () => {
  const root = fixture();
  append(root, "hustsync-internal", '[dependencies]\nhustsync-manager = { path = "../hustsync-manager" }');
  expect(checkArchitecture(root)[0]).toContain("forbidden normal dependency");
});

test("does not extend the Manager dev exception to Worker", () => {
  const root = fixture();
  append(root, "hustsync-worker", '[dev-dependencies]\nhustsync-manager = { path = "../hustsync-manager" }');
  expect(checkArchitecture(root)[0]).toContain("forbidden dev dependency");
});

test("rejects internal build edges even when the production edge is allowed", () => {
  const root = fixture();
  append(root, "hustsync-manager", '[target.\'cfg(unix)\'.build-dependencies]\nhustsync-internal = { path = "../hustsync-internal" }');
  expect(checkArchitecture(root)[0]).toContain("forbidden build dependency [target: cfg(unix)]");
});

test("rejects missing lint inheritance", () => {
  const root = fixture();
  const path = join(root, "hustsynctl/Cargo.toml");
  writeFileSync(path, readFileSync(path, "utf8").replace("[lints]\nworkspace = true\n", ""));
  expect(checkArchitecture(root)).toEqual(["hustsynctl: must inherit [lints] workspace = true"]);
});

test("rejects unclassified members and missing expected members", () => {
  const root = fixture();
  const path = join(root, "hustsynctl/Cargo.toml");
  writeFileSync(path, readFileSync(path, "utf8").replace('name = "hustsynctl"', 'name = "unclassified"'));
  expect(checkArchitecture(root)).toEqual([
    "missing workspace member: hustsynctl",
    "unclassified workspace member: unclassified; update the architecture contract",
  ]);
});

test("invalid TOML/metadata fails closed", () => {
  const root = fixture();
  append(root, "hustsynctl", "not valid toml");
  expect(() => checkArchitecture(root)).toThrow("cargo metadata failed");
});

test("CLI returns a failing exit code for a forbidden edge", () => {
  const root = fixture();
  append(root, "hustsync-manager", '[dependencies]\nhustsync-worker = { path = "../hustsync-worker" }');
  const result = Bun.spawnSync([
    process.execPath, join(import.meta.dir, "../check-architecture.ts"), root,
  ]);
  expect(result.exitCode).toBe(1);
  expect(result.stderr.toString()).toContain("forbidden normal dependency");
});
