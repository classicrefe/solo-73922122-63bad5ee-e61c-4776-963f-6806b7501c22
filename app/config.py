"""Runtime configuration.

All state lives under a single data directory so that several API
instances can share one volume (see docker-compose.yml).
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path

DATA_DIR = Path(os.environ.get("ARCHIVE_DATA_DIR", "/data")).resolve()

# A whole batch upload must fit in 2 MiB.
MAX_UPLOAD_BYTES = 2 * 1024 * 1024


def _parse_capacity_limit() -> int | None:
    """Archive capacity admission limit (bytes of originals).

    Configured at startup via ``ARCHIVE_CAPACITY_BYTES``.  Existing
    originals and in-flight reservations count against it; staged upload
    parts in ``tmp/`` do not.  When unset the service runs without a
    capacity limit.
    """
    raw = os.environ.get("ARCHIVE_CAPACITY_BYTES")
    if raw is None or raw.strip() == "":
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"ARCHIVE_CAPACITY_BYTES must be an integer, got {raw!r}"
        ) from exc
    if value < 0:
        raise RuntimeError(
            f"ARCHIVE_CAPACITY_BYTES must be >= 0, got {value}"
        )
    return value


CAPACITY_LIMIT_BYTES: int | None = _parse_capacity_limit()

# Stable identity of this process while it lives.  Heartbeats under this
# id protect its in-flight reservations from being reaped by another
# instance's startup reconciliation.
INSTANCE_ID = (
    os.environ.get("INSTANCE_NAME")
    or f"pid-{os.getpid()}-{uuid.uuid4().hex[:8]}"
)

# Heartbeat cadence and the grace period after which another instance is
# considered dead and its reservations orphaned.  Overridable for tests.
HEARTBEAT_INTERVAL_SECONDS = float(
    os.environ.get("ARCHIVE_HEARTBEAT_SECONDS", "2")
)
NODE_TTL_SECONDS = float(os.environ.get("ARCHIVE_NODE_TTL_SECONDS", "30"))

# Sub-directories of the shared volume.
BLOB_DIR = DATA_DIR / "blobs"             # content-addressed originals (sha256)
QUARANTINE_DIR = DATA_DIR / "quarantine"  # blobs being removed by the GC
TMP_DIR = DATA_DIR / "tmp"                # in-flight upload parts
DB_PATH = DATA_DIR / "archives.db"


def ensure_layout() -> None:
    for directory in (DATA_DIR, BLOB_DIR, QUARANTINE_DIR, TMP_DIR):
        directory.mkdir(parents=True, exist_ok=True)
