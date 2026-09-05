from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

import pmkt
from pmkt import provenance


SHA = "a" * 40


def _embedded(tmp_path: Path, **overrides) -> Path:
    package = tmp_path / "site-packages" / "pmkt"
    package.mkdir(parents=True)
    package_file = package / "__init__.py"
    package_file.touch()
    (package / "_build_info.json").write_text(json.dumps({
        "format": 1, "distribution": "pmkt", "commit": SHA,
        "version": "0.1.1", "dirty": False, **overrides,
    }))
    return package_file


def test_loaded_source_identity_is_independent_of_caller(tmp_path, monkeypatch):
    before = provenance.implementation_identity(pmkt.__file__, "pmkt")
    monkeypatch.chdir(tmp_path)
    after = provenance.implementation_identity(pmkt.__file__, "pmkt")
    assert before == after
    assert after.commit and after.version == "0.1.1"
    assert isinstance(after.dirty, bool)
    assert "source-git" in after.evidence_source


def test_embedded_identity_ignores_enclosing_git(tmp_path):
    subprocess.run(["git", "init", str(tmp_path)], check=True, capture_output=True)
    package_file = _embedded(tmp_path)
    identity = provenance.implementation_identity(package_file, "pmkt")
    assert (identity.commit, identity.dirty, identity.version) == (SHA, False, "0.1.1")
    assert identity.evidence_source == "embedded-build"


def test_unknown_archive_does_not_inherit_parent_git(tmp_path):
    subprocess.run(["git", "init", str(tmp_path)], check=True, capture_output=True)
    root = tmp_path / "archive"
    package = root / "src" / "pmkt"
    package.mkdir(parents=True)
    (root / "pyproject.toml").write_text('[project]\nname = "pmkt"\nversion = "0.1.1"\n')
    identity = provenance.implementation_identity(package / "__init__.py", "pmkt")
    assert identity.commit is None and identity.dirty is None
    assert identity.evidence_source == "unknown"


@pytest.mark.parametrize("conflict", ["commit", "version"])
def test_conflicting_distribution_evidence_is_an_error(tmp_path, monkeypatch, conflict):
    package_file = _embedded(tmp_path)

    class Distribution:
        files = [Path("pmkt/__init__.py")]
        version = "9.0" if conflict == "version" else "0.1.1"

        def locate_file(self, _entry):
            return package_file

        def read_text(self, _name):
            return json.dumps({"vcs_info": {"vcs": "git", "commit_id": "b" * 40 if conflict == "commit" else SHA}})

    monkeypatch.setattr(provenance.importlib.metadata, "distribution", lambda _name: Distribution())
    identity = provenance.implementation_identity(package_file, "pmkt")
    with pytest.raises(provenance.ProvenanceError, match=f"conflicting {conflict}"):
        identity.require_consistent()


def test_foreign_distribution_cannot_identify_shadowing_package(tmp_path):
    package = tmp_path / "old" / "pmkt"
    package.mkdir(parents=True)
    identity = provenance.implementation_identity(package / "__init__.py", "pmkt")
    assert identity.commit is None and identity.version is None
    assert provenance.implementation_requirements(package / "__init__.py", "pmkt") == ()


def test_source_commit_and_dirty_state_are_observed(tmp_path):
    root = tmp_path / "checkout"
    package = root / "src" / "pmkt"
    package.mkdir(parents=True)
    (package / "__init__.py").touch()
    (root / "pyproject.toml").write_text('[project]\nname = "pmkt"\nversion = "0.1.1"\n')
    for args in (("init",), ("add", "."), ("-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "fixture")):
        subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)
    clean = provenance.implementation_identity(package / "__init__.py", "pmkt")
    assert clean.commit and clean.dirty is False
    (package / "__init__.py").write_text("# changed\n")
    dirty = provenance.implementation_identity(package / "__init__.py", "pmkt")
    assert dirty.commit == clean.commit and dirty.dirty is True


def test_manifest_extra_cannot_fabricate_an_unknown_commit(tmp_path, monkeypatch):
    from pmkt.data import manifests

    monkeypatch.setattr(manifests, "implementation_identity", lambda *_args: provenance.ImplementationIdentity("pmkt", "/unknown"))
    manifest = manifests.build_run_manifest(
        run_id="unknown", run_dir=tmp_path, started_at_utc="2026-05-26T00:00:00Z",
        ended_at_utc="2026-05-26T00:01:00Z", status="success", command="fixture",
        dataset_paths={}, schema_versions={}, row_counts={},
        extra={"pmkt_core_commit": SHA, "pmkt_core_dirty": False},
    )
    assert manifest["pmkt_core_commit"] is None
    assert manifest["pmkt_core_dirty"] is None


def test_archive_build_identity_stays_unknown_and_rejects_conflicts(tmp_path):
    from runpy import run_path

    hooks = run_path(str(Path(__file__).resolve().parents[1] / "build_support.py"))
    build = hooks["build_identity"]
    root = tmp_path / "archive"
    package = root / "src" / "pmkt"
    package.mkdir(parents=True)
    subprocess.run(["git", "init", str(tmp_path)], check=True, capture_output=True)
    build.__globals__["ROOT"] = root
    unknown = build("pmkt", "0.1.1")
    assert unknown["commit"] is None and unknown["dirty"] is None
    (package / "_build_info.json").write_text(json.dumps({**unknown, "version": "0.1.0"}))
    with pytest.raises(ValueError, match="conflicts"):
        build("pmkt", "0.1.1")
