"""The production guards (ADR-018): Python module and shell library behave the same."""

import pathlib
import subprocess

import pytest

import prod_guard

REPO = pathlib.Path(__file__).resolve().parent.parent


def test_python_guard_refuses_another_checkout(monkeypatch, tmp_path):
    monkeypatch.setenv("QP_PROD_DIR", str(tmp_path))
    with pytest.raises(SystemExit) as exc:
        prod_guard.require_prod_worktree("x.py")
    assert "not" in str(exc.value) and str(tmp_path) in str(exc.value)


def test_python_guard_accepts_the_production_worktree(monkeypatch):
    monkeypatch.setenv("QP_PROD_DIR", str(REPO))
    prod_guard.require_prod_worktree("x.py")


def test_python_ack_is_required_even_in_production(monkeypatch):
    monkeypatch.setenv("QP_PROD_DIR", str(REPO))
    monkeypatch.delenv("QP_PROD_TESTS", raising=False)
    with pytest.raises(SystemExit, match="QP_PROD_TESTS=1"):
        prod_guard.require_prod_ack("x.py", "kills things")
    monkeypatch.setenv("QP_PROD_TESTS", "1")
    prod_guard.require_prod_ack("x.py", "kills things")


def shell(call, prod_dir, ack=None):
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": "/nonexistent",
        "QP_PROD_DIR": str(prod_dir),
    }
    if ack:
        env["QP_PROD_TESTS"] = ack
    script = f"set -euo pipefail; source scripts/lib_prod.sh; {call}; echo PASSED"
    return subprocess.run(
        ["bash", "-c", script], cwd=REPO, env=env, capture_output=True, text=True
    )


@pytest.mark.parametrize("call", ["require_prod_worktree", "require_prod_ack effect"])
def test_shell_guard_refuses_another_checkout(call, tmp_path):
    out = shell(call, tmp_path, ack="1")
    assert out.returncode == 2 and "PASSED" not in out.stdout
    assert "not the" in out.stderr


def test_shell_guard_refuses_a_missing_production_dir(tmp_path):
    out = shell("require_prod_worktree", tmp_path / "missing")
    assert out.returncode == 2


def test_shell_ack(tmp_path):
    assert shell("require_prod_worktree", REPO).returncode == 0
    refused = shell("require_prod_ack 'kills things'", REPO)
    assert refused.returncode == 2 and "QP_PROD_TESTS=1" in refused.stderr
    assert shell("require_prod_ack 'kills things'", REPO, ack="1").returncode == 0
