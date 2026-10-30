"""POSIX-only start barrier. Executed directly with isolated Python, before project code."""

from __future__ import annotations

import os
import select
import sys


def main() -> int:
    descriptor = int(sys.argv[1])
    hold_seconds = float(sys.argv[2])
    command = sys.argv[3:]
    os.set_inheritable(descriptor, False)
    readable, _, _ = select.select([descriptor], [], [], hold_seconds)
    allowed = bool(readable) and os.read(descriptor, 1) == b"1"
    os.close(descriptor)
    if not allowed:
        return 125
    try:
        os.execvpe(command[0], command, os.environ)
    except OSError as error:
        # Do not dump the environment or full argument list on an execution failure.
        print(f"Cannot execute command: {error.strerror}", file=sys.stderr)
        return 127


if __name__ == "__main__":
    raise SystemExit(main())
