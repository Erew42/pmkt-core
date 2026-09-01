from __future__ import annotations

import sys
from collections.abc import Sequence

_HELP_ARGS = {"-h", "--help"}
_OPTIONAL_FEATURE_IMPORT_ROOTS = frozenset(
    {
        "duckdb",
        "pandas",
        "pyarrow",
        "websockets",
    }
)

_MINIMAL_HELP = """Usage: pmkt [OPTIONS] COMMAND [ARGS]...

Polymarket data utilities.

The base install can show this top-level help without optional feature
dependencies. Install the relevant extra before running feature commands, for
example pmkt[data], pmkt[storage], or pmkt[streaming].
"""


def main(argv: Sequence[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        from pmkt.cli.app import main as app_main
    except ModuleNotFoundError as exc:
        optional_root = _optional_import_root(exc)
        if optional_root is None:
            raise
        if _is_top_level_help(args):
            sys.stdout.write(_MINIMAL_HELP)
            return
        sys.stderr.write(
            "pmkt command execution requires optional feature dependencies. "
            f"Missing import root: {optional_root}. Install the relevant pmkt extra "
            "and retry.\n"
        )
        raise SystemExit(2) from exc
    if argv is None:
        app_main()
        return
    original_argv = sys.argv
    sys.argv = ["pmkt", *args]
    try:
        app_main()
    finally:
        sys.argv = original_argv


def _is_top_level_help(args: Sequence[str]) -> bool:
    return not args or args[0] in _HELP_ARGS


def _optional_import_root(exc: ModuleNotFoundError) -> str | None:
    name = getattr(exc, "name", None)
    if not name:
        return None
    root = name.split(".", 1)[0]
    if root in _OPTIONAL_FEATURE_IMPORT_ROOTS:
        return root
    return None
