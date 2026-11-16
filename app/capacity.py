"""Capacity admission: byte budget for deduplicated originals.

Accounting model
----------------
* **occupied** - every row in ``blobs``, including rows stuck in
  ``deleting``.  Quota is handed back only once the physical original has
  actually been unlinked and its row removed; a failed delete keeps
  occupying the budget.
* **reserved** - rows in ``capacity_reservations`` whose content hash has
  no blob row yet (an upload that passed admission and is still in
  flight).  Reservations are keyed by SHA-256, so identical content
  uploaded concurrently is charged exactly once.
* **committed** - the union of both sets, one charge per distinct hash.
  Staged parts under ``tmp/`` never count until admission succeeds.

Admission is decided inside the upload's ``BEGIN IMMEDIATE``
transaction, so two instances sharing the volume cannot both pass the
same headroom check: when the budget is full, exactly one of two
different-content uploads wins and the other is rejected with HTTP 507
without consuming a batch number.

Orphan recovery
---------------
A reservation belongs to the node that created it.  Each node refreshes
a heartbeat row in ``nodes``; on restart, reconciliation releases
reservations when any of the following holds:

* a blob row now covers the hash (the upload committed; the reservation
  is redundant, the charge lives on as an occupied original),
* the owning node's heartbeat is missing or older than the TTL,
* the owning node is alive but its staged ``tmp/`` part is gone, which
  means the upload failed before committing.

A reservation whose owner is still heartbeating and whose part still
exists is never touched - another instance's live upload keeps its
budget.
"""
from __future__ import annotations

import logging
import threading
from typing import Any

from . import config, db

logger = logging.getLogger("capacity")


class CapacityExceeded(Exception):
    """Raised inside the upload transaction when the byte budget is full."""

    def __init__(self, limit: int, committed: int, requested: int) -> None:
        self.limit = limit
        self.committed = committed
        self.requested = requested
        self.available = max(0, limit - committed)
        super().__init__(
            f"capacity limit {limit} bytes reached: "
            f"{committed} committed, {requested} more requested, "
            f"{self.available} available"
        )


# One charge per distinct content hash across blobs and reservations.
_UNION_SQL = """
SELECT COALESCE(SUM(size_bytes), 0) AS bytes
  FROM (
        SELECT sha256, MIN(size_bytes) AS size_bytes
          FROM (
                SELECT sha256, size_bytes FROM blobs
                UNION ALL
                SELECT sha256, size_bytes FROM capacity_reservations
               )
         GROUP BY sha256
       )
"""


def capacity_usage(conn: Any) -> dict[str, int | None]:
    """Return limit/occupied/reserved/available byte counters."""
    occupied = conn.execute(
        "SELECT COALESCE(SUM(size_bytes), 0) AS b FROM blobs"
    ).fetchone()["b"]
    committed = conn.execute(_UNION_SQL).fetchone()["bytes"]
    reserved = max(0, committed - occupied)
    limit = config.CAPACITY_LIMIT_BYTES
    available = None if limit is None else max(0, limit - committed)
    return {
        "capacity_limit_bytes": limit,
        "occupied_bytes": occupied,
        "reserved_bytes": reserved,
        "available_bytes": available,
    }


def reserve(
    conn: Any, sha256: str, size_bytes: int, tmp_name: str
) -> bool:
    """Charge ``size_bytes`` for ``sha256`` inside the open write txn.

    Returns ``True`` when a new reservation row was created and
    ``False`` when the content is already paid for (a blob row or an
    existing reservation covers it), so identical content is never
    double-charged.  Raises :class:`CapacityExceeded` when admitting the
    new content would exceed the configured limit.
    """
    already = conn.execute(
        "SELECT 1 FROM blobs WHERE sha256 = ? "
        "UNION ALL "
        "SELECT 1 FROM capacity_reservations WHERE sha256 = ? "
        "LIMIT 1",
        (sha256, sha256),
    ).fetchone()
    if already is not None:
        return False

    if config.CAPACITY_LIMIT_BYTES is not None:
        committed = conn.execute(_UNION_SQL).fetchone()["bytes"]
        if committed + size_bytes > config.CAPACITY_LIMIT_BYTES:
            raise CapacityExceeded(
                config.CAPACITY_LIMIT_BYTES, committed, size_bytes
            )

    conn.execute(
        "INSERT INTO capacity_reservations "
        "(sha256, size_bytes, node_id, tmp_name) VALUES (?, ?, ?, ?)",
        (sha256, size_bytes, config.INSTANCE_ID, tmp_name),
    )
    return True


def release_reservation(sha256: str) -> None:
    """Drop one reservation (best effort; the content is now a blob or
    the upload gave up)."""
    with db.transaction() as conn:
        conn.execute(
            "DELETE FROM capacity_reservations WHERE sha256 = ?", (sha256,)
        )


def release_for_node(node_id: str) -> int:
    """Release every reservation owned by this node (startup cleanup).

    A freshly started process has no live upload yet, so rows left under
    its identity by a previous incarnation - containers reuse the
    INSTANCE_NAME - are orphans by definition.
    """
    with db.transaction() as conn:
        cur = conn.execute(
            "DELETE FROM capacity_reservations WHERE node_id = ?", (node_id,)
        )
        return cur.rowcount


# --------------------------------------------------------------------------- #
# Node heartbeats
# --------------------------------------------------------------------------- #

def _ttl_modifier() -> str:
    return f"-{config.NODE_TTL_SECONDS} seconds"


def heartbeat() -> None:
    """Upsert this process's liveness row."""
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO nodes (node_id) VALUES (?) "
            "ON CONFLICT(node_id) DO UPDATE SET "
            "last_heartbeat = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
            (config.INSTANCE_ID,),
        )


def deregister_node() -> None:
    """Remove this process's heartbeat row on graceful shutdown."""
    try:
        with db.transaction() as conn:
            conn.execute(
                "DELETE FROM nodes WHERE node_id = ?", (config.INSTANCE_ID,)
            )
    except Exception:  # pragma: no cover - shutdown must never raise
        logger.exception("failed to deregister node %s", config.INSTANCE_ID)


class HeartbeatThread:
    """Background daemon refreshing this node's liveness row."""

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        heartbeat()  # become live immediately, before serving requests
        self._thread = threading.Thread(
            target=self._run, name="capacity-heartbeat", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(config.HEARTBEAT_INTERVAL_SECONDS):
            try:
                heartbeat()
            except Exception:  # pragma: no cover - keep the thread alive
                logger.exception("heartbeat failed")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        deregister_node()


# --------------------------------------------------------------------------- #
# Orphan reaping (startup reconciliation + opportunistic maintenance)
# --------------------------------------------------------------------------- #

def reap_orphan_reservations() -> list[str]:
    """Release reservations whose owner/upload no longer exists.

    A reservation is released only when at least one of these is true:

    * the content now has a blob row (committed successfully - the blob
      keeps occupying the budget, so no capacity is actually freed for a
      successful shared original),
    * the owning node is not heartbeating (crashed / restarted),
    * the node is alive but its staged tmp part disappeared (the upload
      failed before its transaction committed).

    Reservations of a still-live node whose part is present are kept.
    """
    released: list[str] = []
    with db.transaction() as conn:
        rows = conn.execute(
            "SELECT sha256, node_id, tmp_name FROM capacity_reservations"
        ).fetchall()
        if not rows:
            # Still prune stale node rows so the nodes table stays small.
            conn.execute(
                "DELETE FROM nodes WHERE node_id != ? AND "
                "last_heartbeat < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)",
                (config.INSTANCE_ID, _ttl_modifier()),
            )
            return released

        live_nodes = {
            r["node_id"]
            for r in conn.execute(
                "SELECT node_id FROM nodes "
                "WHERE last_heartbeat >= "
                "strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)",
                (_ttl_modifier(),),
            )
        }
        covered = {
            r["sha256"] for r in conn.execute("SELECT sha256 FROM blobs")
        }
        for row in rows:
            sha = row["sha256"]
            if sha in covered:
                reason = "committed"
            elif row["node_id"] not in live_nodes:
                reason = "dead-node"
            elif not (config.TMP_DIR / row["tmp_name"]).exists():
                reason = "missing-tmp"
            else:
                continue  # live in-flight upload on another instance
            conn.execute(
                "DELETE FROM capacity_reservations WHERE sha256 = ?", (sha,)
            )
            released.append(sha)
            logger.info("released orphan reservation %s (%s)", sha, reason)

        conn.execute(
            "DELETE FROM nodes WHERE node_id != ? AND "
            "last_heartbeat < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)",
            (config.INSTANCE_ID, _ttl_modifier()),
        )
    return released
