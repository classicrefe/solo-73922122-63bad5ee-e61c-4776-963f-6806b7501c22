"""Capacity admission tests: budget accounting, 507 rejection, release.

These cover the accounting rules with the in-process TestClient;
cross-process admission semantics live in test_capacity_concurrency.py.
"""
from __future__ import annotations

import hashlib
import sqlite3

from conftest import make_zip, upload


def _summary(client) -> dict:
    return client.get("/storage/summary").json()


def test_boundary_exact_fit_is_accepted_one_byte_over_rejected(
    client, capacity_limit
):
    body = make_zip({"a": b"boundary" * 10})
    capacity_limit(len(body))  # budget equals the archive size exactly

    r = upload(client, "EXACT", body)
    assert r.status_code == 200, r.text
    s = _summary(client)
    assert s["available_bytes"] == 0

    # One more distinct byte of content no longer fits.
    other = make_zip({"b": b"boundary" * 10 + b"!"})
    assert upload(client, "OVER", other).status_code == 507
    # Identical content still reuses for free even with zero headroom.
    assert upload(client, "DUP", body).status_code == 200


def test_no_limit_configured_runs_unlimited(client):
    body = make_zip({"a": b"x" * 500})
    assert upload(client, "B1", body).status_code == 200
    s = _summary(client)
    assert s["capacity_limit_bytes"] is None
    assert s["available_bytes"] is None
    assert s["occupied_bytes"] == len(body)
    assert s["reserved_bytes"] == 0


def test_occupied_counts_existing_blobs_and_rejects_new_content(client,
                                                                capacity_limit):
    body1 = make_zip({"a": b"1" * 100})
    body2 = make_zip({"b": "2" * 400})
    capacity_limit(len(body1) + 10)

    assert upload(client, "B1", body1).status_code == 200
    s = _summary(client)
    assert s["capacity_limit_bytes"] == len(body1) + 10
    assert s["occupied_bytes"] == len(body1)
    assert s["reserved_bytes"] == 0
    assert s["available_bytes"] == 10

    # body2 cannot fit: rejected, batch number not consumed.
    r = upload(client, "B2", body2)
    assert r.status_code == 507
    detail = r.json()["detail"]
    assert detail["requested_bytes"] == len(body2)
    assert detail["available_bytes"] == 10

    # No batch row, no blob row, no reservation left behind.
    assert client.get("/batches/B2").status_code == 404
    s = _summary(client)
    assert s["occupied_bytes"] == len(body1)
    assert s["reserved_bytes"] == 0
    assert s["blob_rows"] == 1
    assert client.get("/batches").json()["total"] == 1


def test_identical_content_charged_once_even_under_tight_limit(
    client, capacity_limit
):
    body = make_zip({"shared": b"same-bytes" * 20})
    capacity_limit(len(body))  # budget fits exactly one copy

    assert upload(client, "B1", body).status_code == 200
    # Same content for a second batch must not be double-charged.
    r2 = upload(client, "B2", body)
    assert r2.status_code == 200
    r3 = upload(client, "B3", body)
    assert r3.status_code == 200

    s = _summary(client)
    assert s["occupied_bytes"] == len(body)
    assert s["available_bytes"] == 0
    assert s["blob_rows"] == 1
    assert s["active_batches"] == 3


def test_over_limit_still_allows_download_reuse_and_retire(
    client, capacity_limit
):
    body1 = make_zip({"a": b"alpha" * 30})
    body2 = make_zip({"b": b"beta-different" * 30})
    # Budget admits exactly one of them (not both at once); after
    # retiring body1 the freed budget is enough for body2.
    limit = max(len(body1), len(body2))
    capacity_limit(limit)

    assert upload(client, "B1", body1).status_code == 200
    rejected = upload(client, "B2", body2)
    assert rejected.status_code == 507

    # Existing content stays fully operable while full.
    got = client.get("/batches/B1/archive")
    assert got.status_code == 200
    assert got.content == body1
    assert upload(client, "B1", body1).status_code == 200  # reuse
    assert client.post("/batches/B1/retire").status_code == 200
    # Retirement + GC actually handed the budget back.
    s = _summary(client)
    assert s["occupied_bytes"] == 0
    assert s["available_bytes"] == limit
    # Now the previously rejected content fits and is accepted.
    assert upload(client, "B2", body2).status_code == 200


def test_retire_does_not_refund_until_physical_delete(
    client, capacity_limit, data_dir
):
    from app import storage

    body = make_zip({"a": b"held" * 50})
    capacity_limit(len(body) + 1)
    assert upload(client, "B1", body).status_code == 200
    digest = hashlib.sha256(body).hexdigest()

    storage.SIMULATE_UNLINK_FAILURE = True
    r = client.post("/batches/B1/retire")
    assert r.status_code == 200
    assert r.json()["gc"]["delete_failed_blobs"] == [digest]

    s = _summary(client)
    # Delete failed: row stays in 'deleting', budget stays charged.
    assert s["occupied_bytes"] == len(body)
    assert s["available_bytes"] == 1
    assert s["blob_rows"] == 1
    assert s["queued_for_gc"] == 1

    # A new distinct content still cannot be admitted.
    other = make_zip({"b": "zzz" * 50})
    assert upload(client, "B2", other).status_code == 507

    # Once deletes succeed again, a GC sweep finishes the job and frees
    # the budget; the new content then fits.
    storage.SIMULATE_UNLINK_FAILURE = False
    report = client.post("/gc/run").json()
    assert digest in report["deleted_blobs"]
    s = _summary(client)
    assert s["occupied_bytes"] == 0
    assert s["blob_rows"] == 0
    assert upload(client, "B2", other).status_code == 200


def test_failed_upload_leaves_no_reservation_and_frees_budget(
    client, capacity_limit
):
    body = make_zip({"a": b"ok"})
    capacity_limit(10_000)
    # A non-ZIP upload never reaches admission.
    r = client.post(
        "/batches/BAD/archive",
        files={"file": ("b.zip", b"not a zip at all", "application/zip")},
    )
    assert r.status_code == 400
    # Inspect the ledger through the real DB path.
    from app import config
    db = sqlite3.connect(config.DB_PATH)
    try:
        assert db.execute(
            "SELECT COUNT(*) FROM capacity_reservations"
        ).fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0
    finally:
        db.close()

    assert upload(client, "B1", body).status_code == 200


def test_orphan_reservation_released_on_restart(data_dir, capacity_limit):
    from fastapi.testclient import TestClient

    from app import config
    from app.api import app

    body = make_zip({"a": b"restart" * 40})
    capacity_limit(len(body) + 5)
    with TestClient(app) as c:
        assert upload(c, "B1", body).status_code == 200
        # Simulate an upload that reserved capacity and then hard-died:
        # reservation row present, owning node absent from the heartbeat
        # table, tmp part gone.
        db = sqlite3.connect(config.DB_PATH)
        db.execute(
            "INSERT INTO capacity_reservations "
            "(sha256, size_bytes, node_id, tmp_name) VALUES (?, ?, ?, ?)",
            ("deadbeef" * 8, 999, "vanished-node", "part-vanished"),
        )
        db.commit()
        db.close()

    # A fresh process must reap the orphan reservation and regain budget.
    with TestClient(app) as c:
        s = c.get("/storage/summary").json()
        assert s["reserved_bytes"] == 0
        assert s["occupied_bytes"] == len(body)
        db = sqlite3.connect(config.DB_PATH)
        try:
            assert db.execute(
                "SELECT COUNT(*) FROM capacity_reservations"
            ).fetchone()[0] == 0
        finally:
            db.close()


def test_restart_releases_only_orphans_not_live_shared_content(
    data_dir, capacity_limit
):
    from fastapi.testclient import TestClient

    from app.api import app

    body = make_zip({"a": b"persistent" * 30})
    capacity_limit(len(body))
    with TestClient(app) as c:
        assert upload(c, "B1", body).status_code == 200
        digest = hashlib.sha256(body).hexdigest()

    # Restart: the committed shared original must keep its charge;
    # nothing should be reaped and the batch stays downloadable.
    with TestClient(app) as c:
        s = c.get("/storage/summary").json()
        assert s["occupied_bytes"] == len(body)
        assert s["available_bytes"] == 0
        assert c.get("/batches/B1/archive").content == body
        # Same-content reuse still works without extra budget.
        assert upload(c, "B2", body).status_code == 200
        assert digest in [
            b["sha256"] for b in c.get("/batches").json()["batches"]
        ]


def test_summary_reports_limit_occupied_reserved_available(
    client, capacity_limit
):
    from app import capacity, db as dbmod

    b1 = make_zip({"a": b"1" * 50})
    capacity_limit(10_000)
    assert upload(client, "B1", b1).status_code == 200

    # Inject an in-flight reservation for unseen content and verify the
    # union accounting (one charge per hash) in the summary.
    from app import config
    with dbmod.transaction() as conn:
        conn.execute(
            "INSERT INTO capacity_reservations "
            "(sha256, size_bytes, node_id, tmp_name) VALUES (?, ?, ?, ?)",
            ("a" * 64, 300, config.INSTANCE_ID + "-foreign", "part-x"),
        )
    s = _summary(client)
    assert s["capacity_limit_bytes"] == 10_000
    assert s["occupied_bytes"] == len(b1)
    assert s["reserved_bytes"] == 300
    assert s["available_bytes"] == 10_000 - len(b1) - 300

    # Reaper removes the fake reservation (unknown node => dead-node).
    released = capacity.reap_orphan_reservations()
    assert "a" * 64 in released
    s = _summary(client)
    assert s["reserved_bytes"] == 0


def test_reservation_and_blob_same_hash_charged_once(client, capacity_limit):
    from app import db as dbmod

    body = make_zip({"a": b"union" * 30})
    digest = hashlib.sha256(body).hexdigest()
    capacity_limit(10_000)
    assert upload(client, "B1", body).status_code == 200

    # A stale reservation for an already-committed hash must not add a
    # second charge and must be reaped as redundant.
    with dbmod.transaction() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO capacity_reservations "
            "(sha256, size_bytes, node_id, tmp_name) VALUES (?, ?, ?, ?)",
            (digest, len(body) + 123, "other-node", "part-y"),
        )
    s = _summary(client)
    assert s["occupied_bytes"] == len(body)
    assert s["reserved_bytes"] == 0  # covered by the blob row

    from app import capacity
    assert digest in capacity.reap_orphan_reservations()


def test_slow_tmp_write_does_not_block_other_requests(
    client, capacity_limit, monkeypatch
):
    """A single slow upload's disk pause must not serialize intake."""
    import threading
    import time

    from app import storage

    capacity_limit(1_000_000)
    slow_body = make_zip({"slow": b"S" * 100})
    fast_body = make_zip({"fast": b"F" * 100})

    real_stage = storage.stage_tmp
    state = {"slow_part": None}

    def slow_stage(body):
        path = real_stage(body)
        # Pause only the slow upload's part, identified after staging.
        if body is slow_body:
            state["slow_part"] = path
            time.sleep(2.5)
        return path

    monkeypatch.setattr(storage, "stage_tmp", slow_stage)

    result = {}

    def slow_upload():
        result["slow"] = upload(client, "SLOW", slow_body).status_code

    t = threading.Thread(target=slow_upload)
    t.start()
    time.sleep(0.4)  # let the slow upload enter its staging pause

    started = time.monotonic()
    result["list"] = client.get("/batches").status_code
    result["health"] = client.get("/healthz").status_code
    result["fast"] = upload(client, "FAST", fast_body).status_code
    elapsed = time.monotonic() - started
    t.join()

    assert result == {
        "slow": 200, "list": 200, "health": 200, "fast": 200,
    }
    # The fast work finished while the slow upload was still paused.
    assert elapsed < 2.0
    assert _summary(client)["reserved_bytes"] == 0


def test_slow_gc_delete_does_not_block_unrelated_intake(
    client, capacity_limit, monkeypatch
):
    """A stalled physical delete inside GC must not block other batches."""
    import threading
    import time

    from app import storage

    capacity_limit(1_000_000)
    doomed_body = make_zip({"d": b"D" * 100})
    other_body = make_zip({"o": b"O" * 100})
    assert upload(client, "DOOM", doomed_body).status_code == 200
    assert upload(client, "KEEP", other_body).status_code == 200

    real_unlink = storage._unlink

    def slow_unlink(path):
        # Pause only the doomed blob's quarantine removal.
        if "quarantine" in str(path):
            time.sleep(2.5)
        return real_unlink(path)

    monkeypatch.setattr(storage, "_unlink", slow_unlink)

    result = {}

    def retire():
        result["retire"] = client.post("/batches/DOOM/retire").status_code

    t = threading.Thread(target=retire)
    t.start()
    time.sleep(0.4)  # GC should now be inside its slow delete

    started = time.monotonic()
    result["list"] = client.get("/batches").status_code
    result["download"] = client.get("/batches/KEEP/archive").status_code
    result["upload"] = upload(
        client, "MORE", make_zip({"m": b"M" * 50})
    ).status_code
    elapsed = time.monotonic() - started
    t.join()

    assert result["retire"] == 200
    assert result["list"] == 200
    assert result["download"] == 200
    assert result["upload"] == 200
    # None of the unrelated operations waited out the GC pause.
    assert elapsed < 2.0

