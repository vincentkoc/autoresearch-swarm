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


def count_test_lines(base: str, merge: str) -> tuple[int, int]:
    # Across the test boundary a move is an addition or a deletion. Within
    # the boundary the two counts offset each other.
    diff = subprocess.check_output(
        ["git", "diff", "--no-renames", "--numstat", "-z", base, merge, "--", *TEST_PATHS]
    )
    additions = deletions = 0
    for record in diff.split(b"\0"):
        if record:
            added, deleted, _ = record.split(b"\t", 2)
            if added != b"-":  # Binary fixtures have no text lines.
                additions += int(added)
                deletions += int(deleted)
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
