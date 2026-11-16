"""Shared fixtures: every test gets an isolated data directory.

The tests import the real FastAPI app and drive it with TestClient;
the underlying code is the same code the two Docker replicas run.
"""
from __future__ import annotations

import io
import os
import socket
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "scripts" / "serve.py"

import site  # noqa: E402
_USER_SITE = site.getusersitepackages()


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setenv("ARCHIVE_DATA_DIR", str(root))
    # Ensure no capacity limit leaks in from the outer environment;
    # capacity-focused tests opt in via the capacity_limit fixture.
    monkeypatch.delenv("ARCHIVE_CAPACITY_BYTES", raising=False)

    # config captured module constants at import time; repoint them.
    from app import config
    monkeypatch.setattr(config, "DATA_DIR", root.resolve())
    monkeypatch.setattr(config, "BLOB_DIR", root.resolve() / "blobs")
    monkeypatch.setattr(config, "QUARANTINE_DIR", root.resolve() / "quarantine")
    monkeypatch.setattr(config, "TMP_DIR", root.resolve() / "tmp")
    monkeypatch.setattr(config, "DB_PATH", root.resolve() / "archives.db")
    monkeypatch.setattr(config, "CAPACITY_LIMIT_BYTES", None)
    monkeypatch.setattr(config, "HEARTBEAT_INTERVAL_SECONDS", 0.2)
    monkeypatch.setattr(config, "NODE_TTL_SECONDS", 2.0)
    config.ensure_layout()

    # Fresh per-thread connections pointed at the new DB path: close the
    # whole process-wide pool, including worker-thread locals.
    from app import db, storage
    db.reset_connections()
    storage.SIMULATE_UNLINK_FAILURE = False
    storage.STAGE_STALL_SECONDS = 0.0
    storage.CRASH_HOOK = None
    db.init_db()
    yield root.resolve()


@pytest.fixture
def client(data_dir):
    from app.api import app
    with TestClient(app) as c:
        yield c


@pytest.fixture
def capacity_limit(data_dir, monkeypatch):
    """Set an admission budget; usage: ``def test_x(capacity_limit): ...``
    with the default limit, or call ``capacity_limit(1234)`` explicitly."""
    from app import config

    def _set(limit: int):
        monkeypatch.setattr(config, "CAPACITY_LIMIT_BYTES", int(limit))
        return int(limit)

    return _set


def make_zip(files: dict[str, bytes]) -> bytes:
    """Build a real ZIP archive entirely in memory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


@pytest.fixture
def zip_factory():
    return make_zip


def upload(client, batch_id: str, body: bytes):
    return client.post(
        f"/batches/{batch_id}/archive",
        files={"file": (f"{batch_id}.zip", body, "application/zip")},
    )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_healthy(url: str, proc: subprocess.Popen) -> None:
    for _ in range(100):
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early: {proc.stderr.read()!r}")
        try:
            r = httpx.get(f"{url}/healthz", timeout=1)
            if r.status_code == 200:
                return
        except httpx.TransportError:
            time.sleep(0.1)
    raise RuntimeError(f"server at {url} never became healthy")


@pytest.fixture
def two_instances(data_dir):
    """Two real OS processes sharing one data directory, no limit."""
    yield from _spawn_cluster(data_dir, 2, extra_env={})


def _spawn_cluster(data_dir: Path, n: int, extra_env: dict[str, str]):
    ports = [_free_port() for _ in range(n)]
    env_base = dict(os.environ, ARCHIVE_DATA_DIR=str(data_dir))
    procs = []
    for i, port in enumerate(ports):
        env = dict(
            env_base,
            ARCHIVE_TEST_PORT=str(port),
            ENABLE_SHUTDOWN_ENDPOINT="1",
            INSTANCE_NAME=f"test-node-{i}",
            ARCHIVE_HEARTBEAT_SECONDS="0.5",
            ARCHIVE_NODE_TTL_SECONDS="5",
            PYTHONPATH=os.pathsep.join([str(ROOT), _USER_SITE]),
        )
        env.update(extra_env)
        procs.append(subprocess.Popen(
            [sys.executable, str(SERVER)],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        ))
    urls = [f"http://127.0.0.1:{p}" for p in ports]
    try:
        for url, proc in zip(urls, procs):
            _wait_healthy(url, proc)
        yield urls
    finally:
        for url in urls:
            try:
                httpx.post(f"{url}/internal/shutdown", timeout=5)
            except httpx.HTTPError:
                pass
        for proc in procs:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except PermissionError:
                    pass
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
        for proc in procs:
            err = proc.stderr.read() if proc.stderr else ""
            if "Traceback" in err:
                raise AssertionError("server traceback:\n" + err[-4000:])


@pytest.fixture
def limited_cluster(data_dir):
    """Spawn processes sharing a volume with a small capacity budget.

    Returns a factory: call it once for the main two-instance cluster,
    and again to attach extra processes (e.g. a restarting third
    instance).  Every spawned process is cleaned up at teardown.
    """
    spawned: list[tuple[subprocess.Popen, str]] = []

    def _factory(limit: int, n: int = 2, stall: float = 0.0):
        ports = [_free_port() for _ in range(n)]
        env_base = dict(os.environ, ARCHIVE_DATA_DIR=str(data_dir))
        procs = []
        for i, port in enumerate(ports):
            env = dict(
                env_base,
                ARCHIVE_TEST_PORT=str(port),
                ENABLE_SHUTDOWN_ENDPOINT="1",
                INSTANCE_NAME=f"test-node-{len(spawned)}-{i}",
                ARCHIVE_HEARTBEAT_SECONDS="0.5",
                ARCHIVE_NODE_TTL_SECONDS="5",
                ARCHIVE_CAPACITY_BYTES=str(limit),
                PYTHONPATH=os.pathsep.join([str(ROOT), _USER_SITE]),
            )
            if stall:
                env["ARCHIVE_TEST_STAGE_STALL_SECONDS"] = str(stall)
            procs.append(subprocess.Popen(
                [sys.executable, str(SERVER)],
                cwd=str(ROOT),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            ))
        urls = [f"http://127.0.0.1:{p}" for p in ports]
        for url, proc in zip(urls, procs):
            _wait_healthy(url, proc)
        spawned.extend(zip(procs, urls))
        return urls

    yield _factory

    for _, url in spawned:
        try:
            httpx.post(f"{url}/internal/shutdown", timeout=5)
        except httpx.HTTPError:
            pass
    for proc, _ in spawned:
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except PermissionError:
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
    for proc, _ in spawned:
        err = proc.stderr.read() if proc.stderr else ""
        if "Traceback" in err:
            raise AssertionError("server traceback:\n" + err[-4000:])
