from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"


class _CollectionTimeVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.nodes: list[ast.AST] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        return None

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        return None

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return None

    def generic_visit(self, node: ast.AST) -> None:
        self.nodes.append(node)
        super().generic_visit(node)


def _collection_time_nodes(tree: ast.Module) -> list[ast.AST]:
    visitor = _CollectionTimeVisitor()
    visitor.visit(tree)
    return visitor.nodes


def _is_sys_modules(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "modules"
        and isinstance(node.value, ast.Name)
        and node.value.id == "sys"
    )


def _is_sys_modules_purge_node(node: ast.AST) -> bool:
    if isinstance(node, ast.Delete) and any(
        isinstance(target, ast.Subscript) and _is_sys_modules(target.value)
        for target in node.targets
    ):
        return True
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"clear", "pop", "popitem"}
        and _is_sys_modules(node.func.value)
    )


def _local_purge_helpers(tree: ast.Module) -> set[str]:
    helpers: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
            _is_sys_modules_purge_node(child) for child in ast.walk(node)
        ):
            helpers.add(node.name)
    return helpers


def _calls_local_purge_helper(node: ast.AST, helper_names: set[str]) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in helper_names
    )


def test_tests_do_not_purge_pmkt_modules_during_collection() -> None:
    offenders: list[str] = []
    for path in sorted(TESTS.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        helper_names = _local_purge_helpers(tree)
        if any(
            _is_sys_modules_purge_node(node) or _calls_local_purge_helper(node, helper_names)
            for node in _collection_time_nodes(tree)
        ):
            offenders.append(path.relative_to(ROOT).as_posix())

    assert offenders == []
