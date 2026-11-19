"""Check net growth of tracked test files on a pull request."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys

LIMIT = 400
TEST_PATHS = [
    ":(glob)**/tests/**",
    ":(glob)**/test/**",
    ":(glob)**/conftest.py",
    ":(glob)**/test_*.py",
    ":(glob)**/*_test.py",
    ":(glob)**/*.test.*",
    ":(glob)**/*.spec.*",
]


def blob_lines(mode: bytes, oid: bytes) -> int | None:
    # Gitlinks point at another repository. Only blobs belong to this tree.
    if mode in {b"000000", b"160000"}:
        return None
    with subprocess.Popen(["git", "cat-file", "blob", oid], stdout=subprocess.PIPE) as process:
        assert process.stdout is not None
        lines = 0
        binary = False
        tail = b""
        while chunk := process.stdout.read(65536):
            binary = binary or b"\0" in chunk
            lines += chunk.count(b"\n")
            tail = chunk[-1:]
        if process.wait():
            raise subprocess.CalledProcessError(process.returncode, process.args)
    return None if binary else lines + (tail not in {b"", b"\n"})


def content_counts(base: str, merge: str, path: bytes) -> tuple[int, int]:
    command = [
        "git",
        "--literal-pathspecs",
        "diff",
        "--no-renames",
        "--no-ext-diff",
        "--no-textconv",
    ]
    target: list[str | bytes] = [base, merge, "--", path]
    raw = subprocess.check_output([*command, "--raw", "--no-abbrev", "-z", *target])
    header = raw.split(b"\0", 1)[0]
    old_mode, new_mode, old_id, new_id, _status = header[1:].split()
    if old_id == new_id:
        return 0, 0
    old = blob_lines(old_mode, old_id)
    new = blob_lines(new_mode, new_id)
    if old is None or new is None:
        return new or 0, old or 0
    # --numstat still emits "-" for a path marked -diff even with --text.
    # The forced-text patch for these two blobs has a single file header.
    patch = subprocess.check_output([*command, "--no-color", "--text", "-U0", old_id, new_id, "--"])
    hunks = patch.partition(b"\n@@ ")[2]
    added, deleted = hunks.count(b"\n+"), hunks.count(b"\n-")
    if added - deleted != new - old:
        raise ValueError("Git patch counts disagree with the tracked text blobs")
    return added, deleted


def count_test_lines(base: str, merge: str) -> tuple[int, int]:
    # Disable renames so moves across the test boundary count on their test side.
    flags = ["--no-renames", "--no-ext-diff", "--no-textconv", "--numstat", "-z"]
    diff = subprocess.check_output(["git", "diff", *flags, base, merge, "--", *TEST_PATHS])
    if not diff:
        return 0, 0
    records = [row.split(b"\t", 2) for row in diff.split(b"\0")[:-1]]
    paths = b"".join(path + b"\0" for _, _, path in records)
    attributes = subprocess.check_output(
        ["git", "check-attr", "-z", "--stdin", "diff"], input=paths
    )
    additions = deletions = 0
    for (added, deleted, path), attr in zip(records, attributes.split(b"\0")[2::3], strict=True):
        # A binary marker can hide a text side; an explicit diff attribute can
        # also force binary bytes to appear as added or deleted source lines.
        if added == b"-" or attr != b"unspecified":
            plus, minus = content_counts(base, merge, path)
        else:
            plus, minus = int(added), int(deleted)
        additions += plus
        deletions += minus
    return additions, deletions


def check_budget(added: int, deleted: int, body: str) -> None:
    net = added - deleted
    print(f"Test lines: +{added} / -{deleted}, net {net:+d} (allowance +{LIMIT}).")
    if net <= LIMIT:
        return
    visible = re.sub(r"<!--.*?(?:-->|\Z)", "", body, flags=re.DOTALL)
    for line in visible.splitlines():
        label, _, reason = line.strip().partition(":")
        if label.casefold() == "test-line budget" and reason.strip().casefold() not in {
            "",
            "n/a",
            "none",
            "todo",
            "<reason>",
        }:
            print("The PR body records a test-line budget justification.")
            return
    raise SystemExit(
        "Consolidate overlapping tests or add 'Test-line budget: <reason>' to the PR "
        "description explaining which contracts require the extra coverage."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base", help="the main commit used for integration")
    parser.add_argument("integrated", nargs="?", default="HEAD", help="the integrated Git tree")
    args = parser.parse_args()
    added, deleted = count_test_lines(args.base, args.integrated)
    check_budget(added, deleted, sys.stdin.read())
