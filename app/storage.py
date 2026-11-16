"""Content-addressed blob storage and crash-safe garbage collection.

Layout on the shared volume::

    blobs/<sha256>            one physical copy per distinct archive
    quarantine/<sha256>       blob moved out of blobs/ while being removed
    tmp/part-<uuid>           in-flight upload bodies

Garbage collection is split into two phases so it is safe across two
instances and across crashes:

* **mark** - every blob with no active batch reference is recorded in
  ``gc_queue``; files on disk that no blob row knows about are reaped.
* **sweep** - each queued blob is re-checked.  If it has gained a
  reference again (a new upload reused the hash) it is kept/restored.
  Otherwise the row is flipped to ``deleting``, the bytes move to
  quarantine and the blob row is deleted inside a second transaction;
  the quarantined file is unlinked afterwards.  A crash between steps
  leaves a ``deleting`` row or a quarantine file that
  :func:`reconcile` resumes on restart.

Upload ordering (also crash-safe):

1. stream + validate + hash into a ``tmp/`` part,
2. BEGIN IMMEDIATE, validate batch rules, insert/claim the blob row,
3. COMMIT, then atomically rename the part into ``blobs/`` if missing.

The row always exists before the file appears, so the mark reaper can
never mistake a fresh upload for an orphan, and every upload guarantees
the physical bytes for the row it claims.
"""
from __future__ import annotations

import hashlib
import io
import logging
import os
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from . import capacity, config, db

logger = logging.getLogger("storage")

# Test hooks (never enabled in production):
# * CRASH_HOOK - GC hard-exits (os._exit) at the named checkpoint.
# * SIMULATE_UNLINK_FAILURE - physical deletions fail so the blob row
#   stays in 'deleting' and keeps occupying the capacity budget; a later
#   GC run retries and finally frees the quota.
# * STAGE_STALL_SECONDS - sleep after writing a tmp part, emulating a
#   slow single-file write so tests can prove unrelated work is not
#   blocked.
CRASH_HOOK: str | None = None
SIMULATE_UNLINK_FAILURE = False
STAGE_STALL_SECONDS = 0.0


class UploadError(ValueError):
    """Raised for client-side upload problems (size, ZIP validity)."""


def _fsync_dir(path: Path) -> None:
    """Durably persist a rename/unlink inside a directory."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _unlink(path: Path) -> None:
    """Unlink with an injectable failure mode for quota tests.

    On failure the caller must leave its blob row in place, so the
    content keeps occupying the capacity budget until a later sweep
    actually removes the bytes.
    """
    if SIMULATE_UNLINK_FAILURE:
        raise OSError(f"simulated unlink failure for {path}")
    path.unlink(missing_ok=True)


def blob_path(sha256: str) -> Path:
    return config.BLOB_DIR / sha256


def quarantine_path(sha256: str) -> Path:
    return config.QUARANTINE_DIR / sha256


# --------------------------------------------------------------------------- #
# Upload intake
# --------------------------------------------------------------------------- #

def read_and_validate(stream: BinaryIO, max_bytes: int) -> tuple[bytes, str]:
    """Read an upload body, enforcing the size cap and ZIP validity.

    The body is buffered in memory: 2 MiB is small enough and keeps the
    hashing / zip checks simple before anything touches the shared volume.
    Archive entries are never extracted, so a malicious ZIP cannot write
    outside the volume.
    """
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = stream.read(64 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise UploadError(f"upload exceeds {max_bytes} bytes")
        chunks.append(chunk)
    body = b"".join(chunks)
    if not body:
        raise UploadError("upload body is empty")
    if not zipfile.is_zipfile(io.BytesIO(body)):
        raise UploadError("uploaded content is not a valid ZIP archive")
    return body, hashlib.sha256(body).hexdigest()


def stage_tmp(body: bytes) -> Path:
    """Durably write the body to a temp part (never scanned by the reaper)."""
    config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp = config.TMP_DIR / f"part-{uuid.uuid4().hex}"
    with open(tmp, "wb") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())
    if STAGE_STALL_SECONDS:
        # Test-only pause emulating a slow disk write.  The reservation
        # is not taken until the DB transaction, so this stall must not
        # block any unrelated request.
        time.sleep(STAGE_STALL_SECONDS)
    return tmp


def publish(tmp: Path, sha256: str) -> None:
    """Move a staged part into blobs/ if the bytes are not already there."""
    config.BLOB_DIR.mkdir(parents=True, exist_ok=True)
    target = blob_path(sha256)
    if not target.exists():
        os.replace(tmp, target)
        _fsync_dir(config.BLOB_DIR)
    else:
        tmp.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# Garbage collection
# --------------------------------------------------------------------------- #

@dataclass
class GcReport:
    reaped_unknown_files: list[str] = field(default_factory=list)
    marked: list[str] = field(default_factory=list)
    deleted_blobs: list[str] = field(default_factory=list)
    # Blobs whose physical delete failed: rows stay in 'deleting' and the
    # capacity budget stays charged until a later sweep succeeds.
    delete_failed_blobs: list[str] = field(default_factory=list)
    restored_blobs: list[str] = field(default_factory=list)
    swept_queued: int = 0


def move_back_from_quarantine(sha256: str) -> None:
    """Return quarantined bytes to blobs/ (best effort, idempotent)."""
    qpath = quarantine_path(sha256)
    bpath = blob_path(sha256)
    if qpath.exists():
        if bpath.exists():
            # An upload already put fresh identical bytes back.
            try:
                _unlink(qpath)
            except OSError:
                logger.warning(
                    "could not remove duplicate quarantine file %s", qpath
                )
        else:
            os.replace(qpath, bpath)
            _fsync_dir(config.BLOB_DIR)


def _mark(report: GcReport) -> None:
    conn = db.get_conn()

    # Reap physical files that no blob row accounts for.  With the upload
    # ordering (row committed before the rename) these can only be crash
    # leftovers from older releases or manually placed junk.
    known = {row["sha256"] for row in conn.execute("SELECT sha256 FROM blobs")}
    on_disk = {p.name for p in config.BLOB_DIR.iterdir() if p.is_file()}
    for name in sorted(on_disk - known):
        try:
            _unlink(config.BLOB_DIR / name)
        except OSError:
            # Leave the file for a later sweep; one stubborn file must
            # not block reclamation of the rest.
            logger.warning("could not reap unknown file %s", name)
            continue
        report.reaped_unknown_files.append(name)

    # Queue every ready blob without an active batch reference.
    rows = conn.execute(
        """
        SELECT b.sha256
          FROM blobs b
         WHERE b.status = 'ready'
           AND NOT EXISTS (
                 SELECT 1 FROM batches t
                  WHERE t.sha256 = b.sha256 AND t.status = 'active'
               )
        """
    ).fetchall()
    if rows:
        with db.transaction():
            conn.executemany(
                "INSERT OR IGNORE INTO gc_queue (sha256) VALUES (?)",
                [(row["sha256"],) for row in rows],
            )
        report.marked = [row["sha256"] for row in rows]


def _finish_phase_two(sha256: str, report: GcReport) -> None:
    """Actually remove the bytes, then delete the blob row.

    References are re-checked inside a transaction: a concurrently
    committed upload reusing this hash forces a restore instead of
    deletion.  The blob row is deleted *after* the physical unlink
    succeeds, so a failed delete leaves the row in ``deleting`` and the
    content keeps occupying the capacity budget; a later sweep retries.
    If an upload re-created a *new* ``ready`` row while the old one sat
    in ``deleting``, the bytes are kept in quarantine for that upload to
    republish rather than unlinked.
    """
    conn = db.get_conn()
    with db.transaction():
        referenced = conn.execute(
            "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' "
            "LIMIT 1",
            (sha256,),
        ).fetchone()
        if referenced:
            conn.execute(
                "UPDATE blobs SET status = 'ready', quarantine_path = NULL "
                "WHERE sha256 = ?",
                (sha256,),
            )
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
            restored = True
            live_ready = False
        else:
            live_ready = conn.execute(
                "SELECT 1 FROM blobs WHERE sha256 = ? AND status = 'ready' "
                "LIMIT 1",
                (sha256,),
            ).fetchone()
            restored = False

    if restored:
        move_back_from_quarantine(sha256)
        report.restored_blobs.append(sha256)
        return

    qpath = quarantine_path(sha256)
    bpath = blob_path(sha256)

    if live_ready:
        # An upload racing us already committed a replacement row.
        # Keep the quarantined bytes: the upload moves them back (or
        # republishes identical bytes from its own staged part).
        if bpath.exists():
            try:
                _unlink(qpath)
            except OSError:
                # Harmless duplicate cleanup failure; the live ready row
                # is what matters now.
                pass
        report.restored_blobs.append(sha256)
        return

    # No references and no replacement row: delete the actual bytes.
    # Both locations are covered because a crash may leave a copy in
    # either blobs/ or quarantine/.
    try:
        _unlink(qpath)
        _unlink(bpath)
    except OSError:
        # Delete failed: keep the 'deleting' row so the quota stays
        # charged; leave the blob queued for a later sweep to retry.
        logger.warning(
            "physical delete failed for %s; quota stays reserved", sha256
        )
        report.delete_failed_blobs.append(sha256)
        return

    _fsync_dir(config.QUARANTINE_DIR)
    _fsync_dir(config.BLOB_DIR)

    # Bytes confirmed gone: now release the ledger row (and the budget).
    with db.transaction():
        # Re-check under the lock: a same-hash upload may have committed
        # while we were unlinking.
        live = conn.execute(
            "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' "
            "LIMIT 1",
            (sha256,),
        ).fetchone()
        if live:
            size_row = conn.execute(
                "SELECT size_bytes FROM blobs WHERE sha256 = ? LIMIT 1",
                (sha256,),
            ).fetchone()
            size_bytes = size_row["size_bytes"] if size_row else 0
            conn.execute(
                "UPDATE blobs SET status = 'ready', quarantine_path = NULL "
                "WHERE sha256 = ?",
                (sha256,),
            )
            if size_row is None:
                conn.execute(
                    "INSERT INTO blobs (sha256, size_bytes, status) "
                    "VALUES (?, ?, 'ready')",
                    (sha256, size_bytes),
                )
            conn.execute(
                "DELETE FROM gc_queue WHERE sha256 = ?", (sha256,)
            )
            need_restore = True
        else:
            conn.execute("DELETE FROM blobs WHERE sha256 = ?", (sha256,))
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
            need_restore = False

    if need_restore:
        move_back_from_quarantine(sha256)
        report.restored_blobs.append(sha256)
        return
    report.deleted_blobs.append(sha256)


def _sweep_one(sha256: str, report: GcReport) -> None:
    conn = db.get_conn()
    row = conn.execute(
        "SELECT status, quarantine_path FROM blobs WHERE sha256 = ?",
        (sha256,),
    ).fetchone()

    if row is None:
        # Another instance already finished phase two.
        with db.transaction():
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
        report.swept_queued += 1
        return

    if row["status"] == "deleting":
        # Resume after a crash between the move and the row deletion.
        if CRASH_HOOK == "phase2":
            os._exit(2)
        before_failed = len(report.delete_failed_blobs)
        _finish_phase_two(sha256, report)
        # On a physical delete failure the gc_queue row survives so the
        # next sweep retries; do not count it as fully swept.
        if len(report.delete_failed_blobs) == before_failed:
            report.swept_queued += 1
        return

    # status == 'ready': re-check references before touching the bytes.
    referenced = conn.execute(
        "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' LIMIT 1",
        (sha256,),
    ).fetchone()
    if referenced:
        with db.transaction():
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
        report.restored_blobs.append(sha256)
        return

    bpath = blob_path(sha256)
    if not bpath.exists():
        # Bytes already gone (crash/reaper); finish the bookkeeping.
        _finish_phase_two(sha256, report)
        report.swept_queued += 1
        return

    # Phase one: flip the row to 'deleting', then move bytes to quarantine.
    with db.transaction():
        conn.execute(
            "UPDATE blobs SET status = 'deleting', quarantine_path = ? "
            "WHERE sha256 = ? AND status = 'ready'",
            (str(quarantine_path(sha256)), sha256),
        )
    if CRASH_HOOK == "phase1":
        os._exit(2)
    qpath = quarantine_path(sha256)
    try:
        os.replace(bpath, qpath)
    except FileNotFoundError:
        # Another instance won the race and moved the bytes first.
        # Re-read the row and let the idempotent branches below decide
        # whether to finish the deletion or restore a re-referenced blob.
        row = conn.execute(
            "SELECT status FROM blobs WHERE sha256 = ?", (sha256,)
        ).fetchone()
        if row is None:
            with db.transaction():
                conn.execute(
                    "DELETE FROM gc_queue WHERE sha256 = ?", (sha256,)
                )
            report.swept_queued += 1
            return
        _sweep_one(sha256, report)
        return
    _fsync_dir(config.QUARANTINE_DIR)

    if CRASH_HOOK == "phase2":
        os._exit(2)
    before_failed = len(report.delete_failed_blobs)
    _finish_phase_two(sha256, report)
    if len(report.delete_failed_blobs) == before_failed:
        report.swept_queued += 1


def run_gc() -> GcReport:
    """Mark unreferenced originals and sweep everything queued."""
    report = GcReport()
    config.ensure_layout()
    _mark(report)

    conn = db.get_conn()
    queued = [r["sha256"] for r in conn.execute("SELECT sha256 FROM gc_queue")]
    for sha256 in queued:
        _sweep_one(sha256, report)
    return report


# --------------------------------------------------------------------------- #
# Startup reconciliation (crash recovery)
# --------------------------------------------------------------------------- #

def _restore_blob_row(sha256: str) -> None:
    """Mark a deleting blob live again and return its bytes."""
    conn = db.get_conn()
    with db.transaction():
        conn.execute(
            "UPDATE blobs SET status = 'ready', quarantine_path = NULL "
            "WHERE sha256 = ?",
            (sha256,),
        )
        conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha256,))
    move_back_from_quarantine(sha256)


def reconcile() -> GcReport:
    """Resume interrupted GC work after a process exit/restart."""
    config.ensure_layout()

    # 0. This is a fresh process: any reservations left under its own
    #    node id belong to a previous incarnation and are orphans.
    capacity.release_for_node(config.INSTANCE_ID)

    conn = db.get_conn()
    ttl_modifier = f"-{config.NODE_TTL_SECONDS} seconds"
    live_nodes = {
        r["node_id"]
        for r in conn.execute(
            "SELECT node_id FROM nodes "
            "WHERE last_heartbeat >= "
            "strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)",
            (ttl_modifier,),
        )
    }
    # Reservations still owned by live uploads: their tmp parts must not
    # be touched by this instance's reconciliation.
    protected_parts = {
        r["tmp_name"]
        for r in conn.execute(
            "SELECT tmp_name FROM capacity_reservations WHERE node_id IN "
            "(SELECT node_id FROM nodes "
            " WHERE last_heartbeat >= "
            " strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?))",
            (ttl_modifier,),
        )
    }

    # 1. In-flight upload parts never became rows; drop them - unless a
    #    referenced blob is missing its bytes (crash between COMMIT and
    #    publish), in which case the part is the only surviving copy and
    #    must be moved into place so an active batch never lacks its file.
    #    Parts belonging to another instance's live in-flight upload are
    #    never touched.
    tmp_parts = list(config.TMP_DIR.glob("part-*"))
    if tmp_parts:
        missing = {
            r["sha256"]
            for r in db.get_conn().execute(
                "SELECT b.sha256 FROM blobs b "
                "WHERE b.status = 'ready' "
                "AND EXISTS (SELECT 1 FROM batches t "
                "            WHERE t.sha256 = b.sha256 AND t.status = 'active')"
            )
        }
        missing = {h for h in missing if not blob_path(h).exists()}
        for part in tmp_parts:
            if part.name in protected_parts:
                continue
            rescued = False
            if missing:
                digest = hashlib.sha256(part.read_bytes()).hexdigest()
                if digest in missing:
                    os.replace(part, blob_path(digest))
                    _fsync_dir(config.BLOB_DIR)
                    missing.discard(digest)
                    rescued = True
            if not rescued:
                try:
                    part.unlink(missing_ok=True)
                except OSError:
                    logger.warning("could not remove orphan tmp part %s", part)

    # 1b. Release reservations whose upload crashed, whose node died, or
    #     whose content already committed.  Live uploads on other
    #     instances keep their budget.
    capacity.reap_orphan_reservations()

    conn = db.get_conn()

    # 2. Blobs stuck in 'deleting': requeue; restore any that are
    #    referenced again, otherwise let the sweep finish them.
    deleting = [
        r["sha256"]
        for r in conn.execute(
            "SELECT sha256 FROM blobs WHERE status = 'deleting'"
        )
    ]
    for sha256 in deleting:
        conn.execute(
            "INSERT OR IGNORE INTO gc_queue (sha256) VALUES (?)", (sha256,)
        )
        referenced = conn.execute(
            "SELECT 1 FROM batches WHERE sha256 = ? AND status = 'active' "
            "LIMIT 1",
            (sha256,),
        ).fetchone()
        if referenced:
            _restore_blob_row(sha256)

    # 3. Quarantine files without a deleting row (unlink missed by crash).
    tracked_q = {
        r["sha256"]
        for r in conn.execute(
            "SELECT sha256 FROM blobs WHERE quarantine_path IS NOT NULL"
        )
    }
    for qfile in config.QUARANTINE_DIR.iterdir():
        if qfile.is_file() and qfile.name not in tracked_q:
            try:
                _unlink(qfile)
            except OSError:
                logger.warning(
                    "could not remove orphan quarantine file %s", qfile
                )

    # 4. Continue the normal mark/sweep pipeline.
    return run_gc()
