#!/usr/bin/env python3
"""Verify that the hand-maintained command table in src/commands/core.rs is
signature-compatible with the vendored redis-rs fork's implement_commands!
table.

For every method in the fork's table this checks that our table has an entry
with the same name, the same generic parameters (names, bounds, and order —
turbofish compatibility), and the same argument list. Extra entries on our
side are also flagged (they would silently diverge from the parity contract).

Exit code 0 = parity holds; 1 = divergence (differences printed).
Run from the crate root; the fork is resolved via `cargo metadata`
(or pass its commands/mod.rs path as argv[1]).
"""
import json
import os
import re
import subprocess
import sys


def resolve_fork_mod_rs() -> str:
    if len(sys.argv) > 1:
        return sys.argv[1]
    meta = json.loads(
        subprocess.check_output(["cargo", "metadata", "--format-version", "1"])
    )
    manifests = [p["manifest_path"] for p in meta["packages"] if p["name"] == "redis"]
    if not manifests:
        sys.exit("could not resolve the `redis` package via cargo metadata")
    return os.path.join(os.path.dirname(manifests[0]), "src", "commands", "mod.rs")


def norm_sig(gen: str, args: str):
    """Normalized (generics, args) for comparison."""
    gens = tuple(
        tuple(x.strip() for x in g.split(":", 1))
        for g in filter(None, (p.strip() for p in gen.split(",")))
    )
    parts, depth, cur = [], 0, ""
    for ch in args:
        if ch in "(<[":
            depth += 1
        elif ch in ")>]":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    arglist = tuple(
        (a.split(":", 1)[0].strip(), " ".join(a.split(":", 1)[1].split()))
        for a in parts
    )
    return gens, arglist


def parse_fork(path: str):
    src = open(path).read()
    start = src.index("implement_commands! {")
    body = src[start : re.search(r"^\}", src[start:], re.M).start() + start]
    out = {}
    lines = body.split("\n")
    li, n = 0, len(lines)
    while li < n:
        stripped = lines[li].strip()
        if not stripped.startswith("fn "):
            li += 1
            continue
        sig = lines[li]
        while "{" not in sig:
            li += 1
            sig += " " + lines[li].strip()
        li += 1
        depth = sig.count("{") - sig.count("}")
        while depth > 0:
            depth += lines[li].count("{") - lines[li].count("}")
            li += 1
        sig1 = " ".join(sig.split())
        m = re.match(r"fn\s+([a-z_0-9]+)\s*(?:<([^>]*)>)?\s*\((.*?)\)\s*\{", sig1)
        out[m.group(1)] = norm_sig(m.group(2) or "", m.group(3).strip())
    return out


def parse_ours(path: str):
    src = open(path).read()
    start = src.index("implement_glide_commands! {")
    body = src[start : re.search(r"^\}", src[start:], re.M).start() + start]
    out = {}
    for m in re.finditer(
        r"fn\s+([a-z_0-9]+)\s*(?:<([^>]*)>)?\s*\(([^;]*?)\)\s*;", body, re.S
    ):
        out[m.group(1)] = norm_sig(m.group(2) or "", " ".join(m.group(3).split()))
    return out


def main():
    fork = parse_fork(resolve_fork_mod_rs())
    ours = parse_ours("src/commands/core.rs")
    problems = []
    for name, sig in fork.items():
        if name not in ours:
            problems.append(f"MISSING in ours: {name}")
        elif ours[name] != sig:
            problems.append(f"SIGNATURE DIFF {name}:\n  fork: {sig}\n  ours: {ours[name]}")
    for name in ours:
        if name not in fork:
            problems.append(f"EXTRA in ours (not in fork table): {name}")
    if problems:
        print(f"PARITY VIOLATIONS ({len(problems)}):")
        for p in problems:
            print(" -", p)
        sys.exit(1)
    print(f"parity OK: {len(fork)} methods match the fork table exactly")


if __name__ == "__main__":
    main()
