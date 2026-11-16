"""Cross-process capacity admission tests.

Two (or more) real uvicorn processes share one volume and one capacity
budget - the same topology as the Docker Compose deployment.  These
prove the admission rule holds across instances, not just between
threads in one process.
"""
from __future__ import annotations

import hashlib
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from conftest import make_zip


def _upload(url: str, batch_id: str, body: bytes):
    with httpx.Client(timeout=30) as c:
        return c.post(
            f"{url}/batches/{batch_id}/archive",
            files={"file": ("b.zip", body, "application/zip")},
        )


def test_concurrent_different_content_never_exceeds_limit(limited_cluster):
    # Budget admits exactly three distinct archives.
    bodies = [make_zip({f"f{i}": bytes([i]) * 400}) for i in range(8)]
    sizes = [len(b) for b in bodies]
    limit = sum(sizes[:3])
    urls = limited_cluster(limit)

    def job(i):
        url = urls[i % 2]
        r = _upload(url, f"C{i:02d}", bodies[i])
        return r.status_code

    with ThreadPoolExecutor(max_workers=8) as pool:
        statuses = list(pool.map(job, range(8)))

    accepted = [i for i, s in enumerate(statuses) if s == 200]
    rejected = [i for i, s in enumerate(statuses) if s == 507]
    assert len(accepted) == 3
    assert len(rejected) == 5
    assert set(statuses) <= {200, 507}

    s = httpx.get(f"{urls[0]}/storage/summary", timeout=10).json()
    assert s["occupied_bytes"] == sum(sizes[i] for i in accepted)
    assert s["occupied_bytes"] <= limit
    assert s["available_bytes"] == limit - s["occupied_bytes"]
    assert s["reserved_bytes"] == 0
    assert s["blob_rows"] == 3
    # Rejected batch numbers were never consumed.
    listing = httpx.get(f"{urls[0]}/batches", timeout=10).json()
    assert listing["total"] == 3
    assert {b["batch_id"] for b in listing["batches"]} == {
        f"C{i:02d}" for i in accepted
    }


def test_concurrent_identical_content_charged_once_across_instances(
    limited_cluster, data_dir
):
    body = make_zip({"same": b"dedup-across-nodes" * 50})
    urls = limited_cluster(len(body))  # room for exactly one copy

    n = 10
    with ThreadPoolExecutor(max_workers=n) as pool:
        statuses = list(pool.map(
            lambda i: _upload(urls[i % 2], f"D{i:02d}", body).status_code,
            range(n),
        ))
    assert statuses == [200] * n

    assert len(list((data_dir / "blobs").iterdir())) == 1
    s = httpx.get(f"{urls[1]}/storage/summary", timeout=10).json()
    assert s["occupied_bytes"] == len(body)
    assert s["available_bytes"] == 0
    assert s["reserved_bytes"] == 0
    assert s["active_batches"] == n


def test_capacity_then_retire_frees_budget_for_waiting_content(
    limited_cluster
):
    first = make_zip({"first": b"AAAA" * 200})
    second = make_zip({"second": b"BBBB" * 200})
    # Budget admits exactly one archive, not both simultaneously.
    urls = limited_cluster(max(len(first), len(second)))

    assert _upload(urls[0], "FULL", first).status_code == 200
    assert _upload(urls[1], "WAIT", second).status_code == 507

    # Over-limit operations still work on both instances.
    assert httpx.get(
        f"{urls[1]}/batches/FULL/archive", timeout=10
    ).status_code == 200
    assert httpx.post(
        f"{urls[0]}/batches/FULL/retire", timeout=15
    ).status_code == 200

    s = httpx.get(f"{urls[0]}/storage/summary", timeout=10).json()
    assert s["occupied_bytes"] == 0
    assert s["available_bytes"] == max(len(first), len(second))
    # The previously rejected number is reusable now (it was never taken).
    r = _upload(urls[1], "WAIT", second)
    assert r.status_code == 200, r.text


def test_stalled_single_upload_does_not_block_unrelated_work(
    limited_cluster
):
    """A slow tmp write must not serialize unrelated requests."""
    small = make_zip({"k": b"v"})
    other = make_zip({"other": b"unrelated-bytes" * 10})
    # Limit generous enough for both; inject a 3-second staging stall.
    urls = limited_cluster(1_000_000, stall=3.0)

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=3) as pool:
        slow_fut = pool.submit(_upload, urls[0], "SLOW", small)
        # Give the slow upload a moment to enter its staging pause.
        time.sleep(0.5)
        list_fut = pool.submit(
            lambda: httpx.get(f"{urls[1]}/batches", timeout=10).status_code
        )
        up_fut = pool.submit(_upload, urls[1], "FAST", other)
        dl_fut = pool.submit(
            lambda: httpx.get(f"{urls[0]}/healthz", timeout=10).status_code
        )
        slow_status = slow_fut.result().status_code
        list_status = list_fut.result()
        up_status = up_fut.result().status_code
        dl_status = dl_fut.result()
    elapsed = time.monotonic() - started

    assert slow_status == 200
    assert list_status == 200
    assert up_status == 200
    assert dl_status == 200
    # The unrelated upload/query finished well before the 3 s stall
    # elapsed, proving no head-of-line serialization.
    assert elapsed < 6.0

    s = httpx.get(f"{urls[0]}/storage/summary", timeout=10).json()
    assert s["active_batches"] == 2
    assert s["reserved_bytes"] == 0


def test_reconcile_keeps_another_live_upload_reservation(
    limited_cluster, data_dir
):
    """A restarting instance must not reap a live node's reservation."""
    import sqlite3

    body = make_zip({"a": b"anchor" * 40})
    limit = 1_000_000
    urls = limited_cluster(limit)
    assert _upload(urls[0], "ANCHOR", body).status_code == 200

    # Find the live node id of instance A and plant an in-flight
    # reservation that still owns an existing tmp part - i.e. a live
    # upload in the middle of staging/committing.
    db = sqlite3.connect(data_dir / "archives.db")
    live_node = db.execute(
        "SELECT node_id FROM nodes ORDER BY last_heartbeat LIMIT 1"
    ).fetchone()[0]
    part = data_dir / "tmp" / "part-live-upload"
    part.write_bytes(b"staged-bytes-of-a-live-upload")
    db.execute(
        "INSERT INTO capacity_reservations "
        "(sha256, size_bytes, node_id, tmp_name) VALUES (?, ?, ?, ?)",
        ("e" * 64, 256, live_node, part.name),
    )
    db.commit()
    db.close()

    # A brand-new third instance reconciles on startup.
    third = limited_cluster(limit, n=1)
    s = httpx.get(f"{third[0]}/storage/summary", timeout=15).json()
    # The live reservation survives and still reserves budget.
    assert s["reserved_bytes"] == 256
    assert part.exists()
    db = sqlite3.connect(data_dir / "archives.db")
    try:
        row = db.execute(
            "SELECT node_id FROM capacity_reservations WHERE sha256 = ?",
            ("e" * 64,),
        ).fetchone()
        assert row is not None and row[0] == live_node
    finally:
        db.close()


def test_stalled_cleanup_does_not_block_unrelated_intake(limited_cluster):
    """GC work (inline with retire) must not block unrelated requests."""
    doomed = make_zip({"doomed": b"D" * 500})
    other = make_zip({"other": b"E" * 300})
    urls = limited_cluster(1_000_000)

    assert _upload(urls[0], "DOOM", doomed).status_code == 200
    assert _upload(urls[1], "KEEP", other).status_code == 200

    # Retire on instance A while instance B keeps serving other work.
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=4) as pool:
        retire_f = pool.submit(
            lambda: httpx.post(
                f"{urls[0]}/batches/DOOM/retire", timeout=15
            ).status_code
        )
        f1 = pool.submit(
            lambda: httpx.get(f"{urls[1]}/batches", timeout=10).status_code
        )
        f2 = pool.submit(
            lambda: httpx.get(
                f"{urls[0]}/batches/KEEP/archive", timeout=10
            ).status_code
        )
        f3 = pool.submit(
            lambda: _upload(urls[1], "MORE",
                            make_zip({"m": b"M" * 100})).status_code
        )
        assert retire_f.result() == 200
        assert f1.result() == 200
        assert f2.result() == 200
        assert f3.result() == 200
    assert time.monotonic() - started < 10
    s = httpx.get(f"{urls[1]}/storage/summary", timeout=10).json()
    assert s["reserved_bytes"] == 0
    assert s["active_batches"] == 2


def test_orphan_reservation_released_after_process_restart(
    limited_cluster, data_dir
):
    import sqlite3

    body = make_zip({"a": b"reserved-then-crashed" * 20})
    limit = len(body) + 1000
    urls = limited_cluster(limit)
    assert _upload(urls[0], "ALIVE", body).status_code == 200

    # Plant a reservation owned by a node that never registered a
    # heartbeat, with no tmp part: exactly the crash signature.
    db = sqlite3.connect(data_dir / "archives.db")
    db.execute(
        "INSERT INTO capacity_reservations "
        "(sha256, size_bytes, node_id, tmp_name) VALUES (?, ?, ?, ?)",
        ("c" * 64, 500, "crashed-node", "part-crashed"),
    )
    db.commit()
    db.close()

    # Start a third process against the same volume; its startup
    # reconciliation must reap the dead reservation, not the live blob.
    third = limited_cluster(limit, n=1)
    s = httpx.get(f"{third[0]}/storage/summary", timeout=15).json()
    assert s["reserved_bytes"] == 0
    assert s["occupied_bytes"] == len(body)
    db = sqlite3.connect(data_dir / "archives.db")
    try:
        assert db.execute(
            "SELECT COUNT(*) FROM capacity_reservations"
        ).fetchone()[0] == 0
    finally:
        db.close()
    # Live content remains downloadable through the new instance.
    r = httpx.get(
        f"{third[0]}/batches/ALIVE/archive", timeout=10
    )
    assert r.status_code == 200
    assert hashlib.sha256(r.content).hexdigest() == \
        hashlib.sha256(body).hexdigest()
