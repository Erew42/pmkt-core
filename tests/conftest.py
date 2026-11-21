from __future__ import annotations

import shutil
import sys
from collections.abc import Generator
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
ROOT_PATH = str(ROOT)
SRC_PATH = str(SRC)

if sys.path[0] != SRC_PATH:
    try:
        sys.path.remove(SRC_PATH)
    except ValueError:
        pass
    sys.path.insert(0, SRC_PATH)

if ROOT_PATH not in sys.path:
    sys.path.insert(1, ROOT_PATH)


_PHASE_FAILED_KEY = pytest.StashKey[dict[str, bool]]()


def _cleanup_passing_tmp_path(item: pytest.Item) -> bool:
    """Remove a test's ``tmp_path`` right after teardown when no phase failed."""
    phase_failed = item.stash.get(_PHASE_FAILED_KEY, None)
    if _PHASE_FAILED_KEY in item.stash:
        del item.stash[_PHASE_FAILED_KEY]
    if not phase_failed or any(phase_failed.values()):
        return False
    funcargs = getattr(item, "funcargs", None)
    if not isinstance(funcargs, dict):
        return False
    tmp_path = funcargs.get("tmp_path")
    if not isinstance(tmp_path, Path) or not tmp_path.exists():
        return False
    shutil.rmtree(tmp_path, ignore_errors=True)
    return not tmp_path.exists()


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[object]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    item.stash.setdefault(_PHASE_FAILED_KEY, {})[report.when] = bool(report.failed)
    if report.when == "teardown":
        _cleanup_passing_tmp_path(item)
    return report
