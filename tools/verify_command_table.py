#!/usr/bin/env python3
"""Verify GLIDE's command table (src/commands/core.rs) against the vendored
fork's implement_commands! table (redis-rs fork, v0.25.2 — predating the
upstream license change).

For every method in the fork's table this checks that our table has an entry
with the same name, the same generic parameters (names, bounds, and order —
turbofish compatibility), and the same argument list. Extra entries on our
side are also flagged (they would silently diverge from the parity contract).

The scan-iterator methods (`scan`/`scan_match`/`hscan`/... — defined in the
macro *body* on both sides, not as table entries) are verified too, against
the fork's definitions in commands/macros.rs: same generics and arguments,
present in both the async and blocking flavors.

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
        if not m:
            sys.exit(
                "unparseable fork table entry (did the fork's table style "
                f"change on a rev bump?):\n  {sig1[:160]}"
            )
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


# The scan-family methods live in the macro *body* (both here and in the
# fork), so the table parsers above never see them. GLIDE's scan methods
# DELIBERATELY deviate from the fork in receiver (&self vs &mut self) and
# return type (GLIDE-owned iterators on the owned-send path — see
# src/commands/scan.rs); what must stay in lockstep with the fork is the
# method NAMES, the generic parameters (minus GLIDE's added lifetimes/Send
# bounds), and the argument lists.
SCAN_RE = re.compile(
    r"fn\s+((?:[hsz])?scan(?:_match)?)\s*<([^>]*)>\s*\(([^)]*)\)", re.S
)


def _norm_scan_generics(gen: str) -> tuple:
    """Generic params normalized: lifetimes dropped, `Send`/lifetime bounds
    (GLIDE's deliberate additions) stripped, `redis::` qualifiers removed."""
    out = []
    for part in filter(None, (p.strip() for p in gen.split(","))):
        if part.startswith("'"):
            continue  # lifetime param (GLIDE-side addition)
        name, _, bounds = part.partition(":")
        kept = [
            b.strip().replace("redis::", "")
            for b in bounds.split("+")
            if b.strip() and b.strip() != "Send" and not b.strip().startswith("'")
        ]
        out.append((name.strip(), " + ".join(kept)))
    return tuple(out)


def parse_scan_methods(src: str):
    """name -> list of normalized (generics, args) — one element per trait
    flavor (blocking + async) the method is defined in. `self` receivers,
    `redis::` qualifiers, and GLIDE's added lifetime/Send bounds are
    normalized away."""
    out = {}
    for m in SCAN_RE.finditer(src):
        name, gen, args = m.group(1), m.group(2), m.group(3)
        args = ",".join(a for a in args.split(",") if "self" not in a)
        args = " ".join(args.replace("redis::", "").split())
        out.setdefault(name, []).append(
            (_norm_scan_generics(gen), norm_sig("", args)[1])
        )
    return out


def check_scan_methods(fork_mod_rs: str, problems: list):
    macros_rs = os.path.join(os.path.dirname(fork_mod_rs), "macros.rs")
    if not os.path.exists(macros_rs):
        sys.exit(f"fork macros.rs not found next to the table: {macros_rs}")
    fork = parse_scan_methods(open(macros_rs).read())
    ours = parse_scan_methods(open("src/commands/core.rs").read())
    for name, sigs in fork.items():
        f, o = set(sigs), set(ours.get(name, []))
        if not o:
            problems.append(f"MISSING scan method in ours: {name}")
        elif o != f:
            problems.append(
                f"SCAN SIGNATURE DIFF {name}:\n  fork: {sorted(f)}\n  ours: {sorted(o)}"
            )
        elif len(ours[name]) != 2:
            problems.append(
                f"scan method {name} defined {len(ours[name])}x in ours "
                "(expected exactly 2: async + blocking flavors)"
            )
    for name in ours:
        if name not in fork:
            problems.append(f"EXTRA scan method in ours (not in fork): {name}")


def main():
    fork_mod_rs = resolve_fork_mod_rs()
    fork = parse_fork(fork_mod_rs)
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
    check_scan_methods(fork_mod_rs, problems)
    if problems:
        print(f"PARITY VIOLATIONS ({len(problems)}):")
        for p in problems:
            print(" -", p)
        sys.exit(1)
    print(
        f"parity OK: {len(fork)} methods match the fork table exactly; "
        "scan iterators match the fork's macro definitions"
    )


if __name__ == "__main__":
    main()
