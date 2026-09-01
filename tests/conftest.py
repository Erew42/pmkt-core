from __future__ import annotations

import sys
from pathlib import Path


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
