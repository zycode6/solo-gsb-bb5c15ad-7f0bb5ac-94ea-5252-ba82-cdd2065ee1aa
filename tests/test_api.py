"""API 行为测试：租约独占/过期、幂等上报、提交与状态查询。"""
import os
import tempfile
import threading
from datetime import datetime, timezone

# 必须在导入 app 之前指定测试数据库
os.environ["UPLOAD_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")
os.environ["DEFAULT_LEASE_TTL_SECONDS"] = "300"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import get_conn  # noqa: E402
from app.main import app  # noqa: E402

client = TestClient(app)


def make_session(total_chunks=3, file_digest="f" * 64):
    resp = client.post("/sessions", json={
        "total_chunks": total_chunks,
        "file_digest": file_digest,
        "digest_algorithm": "sha256",
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def lease(session_id, index, ttl=None):
    body = {} if ttl is None else {"lease_ttl_seconds": ttl}
    return client.post(f"/sessions/{session_id}/chunks/{index}/lease", json=body)


def fail(session_id, index, token):
    return client.post(
        f"/sessions/{session_id}/chunks/{index}/fail",
        json={"lease_token": token},
    )


def complete(session_id, index, token, digest):
    return client.post(
        f"/sessions/{session_id}/chunks/{index}/complete",
        json={"lease_token": token, "chunk_digest": digest},
    )


def test_create_session_and_status():
    s = make_session(total_chunks=4)
    assert s["status"] == "active"
    assert s["total_chunks"] == 4

    st = client.get(f"/sessions/{s['id']}").json()
    assert st["completed_chunks"] == 0
    assert st["missing_chunks"] == [0, 1, 2, 3]
    assert [c["status"] for c in st["chunks"]] == ["pending"] * 4


def test_lease_is_exclusive_until_expiry():
    s = make_session()
    r1 = lease(s["id"], 0)
    assert r1.status_code == 200
    token1 = r1.json()["lease_token"]
    assert token1

    # 同一分片在租约有效期内不可再次领取
    r2 = lease(s["id"], 0)
    assert r2.status_code == 409

    # 手动让租约过期后，可被重新领取
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = 0",
        (s["id"],),
    )
    get_conn().commit()
    r3 = lease(s["id"], 0)
    assert r3.status_code == 200
    assert r3.json()["lease_token"] != token1


def test_complete_requires_valid_lease_and_token():
    s = make_session()
    # 未领取租约直接上报
    assert complete(s["id"], 0, "no-such-token", "a" * 64).status_code == 409

    token = lease(s["id"], 0).json()["lease_token"]
    # 令牌不匹配
    assert complete(s["id"], 0, "wrong-token", "a" * 64).status_code == 409
    # 正确令牌
    assert complete(s["id"], 0, token, "a" * 64).status_code == 200


def test_expired_lease_cannot_complete():
    s = make_session()
    token = lease(s["id"], 0).json()["lease_token"]
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = 0",
        (s["id"],),
    )
    get_conn().commit()
    assert complete(s["id"], 0, token, "a" * 64).status_code == 409


def test_complete_is_idempotent():
    s = make_session()
    token = lease(s["id"], 1).json()["lease_token"]
    digest = "b" * 64

    r1 = complete(s["id"], 1, token, digest)
    assert r1.status_code == 200
    assert r1.json()["idempotent_replay"] is False

    # 相同令牌 + 相同摘要：幂等成功，completed_at 不变
    r2 = complete(s["id"], 1, token, digest)
    assert r2.status_code == 200
    assert r2.json()["idempotent_replay"] is True
    assert r2.json()["completed_at"] == r1.json()["completed_at"]

    # 相同令牌但摘要不同：冲突
    assert complete(s["id"], 1, token, "c" * 64).status_code == 409
    # 已完成分片不可再领取租约
    assert lease(s["id"], 1).status_code == 409


def test_finalize_blocked_until_all_chunks_done():
    s = make_session(total_chunks=3)
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200

    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 409
    assert r.json()["missing_chunks"] == [1, 2]

    for i in (1, 2):
        t = lease(s["id"], i).json()["lease_token"]
        assert complete(s["id"], i, t, str(i) * 64).status_code == 200

    ok = client.post(f"/sessions/{s['id']}/finalize")
    assert ok.status_code == 200
    body = ok.json()
    assert body["status"] == "finalized"
    assert body["file_digest"] == "f" * 64
    # 清单按分片序号升序汇总
    assert [m["chunk_index"] for m in body["manifest"]] == [0, 1, 2]
    assert [m["chunk_digest"] for m in body["manifest"]] == [str(i) * 64 for i in range(3)]

    # 重复提交幂等
    again = client.post(f"/sessions/{s['id']}/finalize")
    assert again.status_code == 200
    assert again.json()["manifest"] == body["manifest"]

    # 已提交会话不可再领取租约
    assert lease(s["id"], 0).status_code == 409

    st = client.get(f"/sessions/{s['id']}").json()
    assert st["status"] == "finalized"
    assert st["missing_chunks"] == []
    assert st["completed_chunks"] == 3


def test_unknown_session_and_chunk():
    assert client.get("/sessions/nope").status_code == 404
    s = make_session()
    assert lease(s["id"], 99).status_code == 404
    assert complete(s["id"], 99, "t", "d").status_code == 404


def test_validation():
    assert client.post("/sessions", json={"total_chunks": 0, "file_digest": "x"}).status_code == 422
    assert client.post("/sessions", json={"total_chunks": 1}).status_code == 422


# ---------------------------------------------------------------- 依赖调度

def make_session_with_deps(total_chunks, dependencies):
    resp = client.post("/sessions", json={
        "total_chunks": total_chunks,
        "file_digest": "f" * 64,
        "digest_algorithm": "sha256",
        "dependencies": {str(k): v for k, v in dependencies.items()},
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def ready(session_id):
    resp = client.get(f"/sessions/{session_id}/ready")
    assert resp.status_code == 200, resp.text
    return resp.json()


def expire_lease(session_id, index):
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = ?",
        (session_id, index),
    )
    get_conn().commit()


def create_session_raw(payload):
    return client.post("/sessions", json={"file_digest": "f" * 64, **payload})


def test_dependencies_rejected_when_invalid():
    # 键越界
    assert create_session_raw({"total_chunks": 3, "dependencies": {"3": [0]}}).status_code == 422
    assert create_session_raw({"total_chunks": 3, "dependencies": {"-1": [0]}}).status_code == 422
    # 值越界
    assert create_session_raw({"total_chunks": 3, "dependencies": {"1": [3]}}).status_code == 422
    assert create_session_raw({"total_chunks": 3, "dependencies": {"1": [-2]}}).status_code == 422
    # 自依赖
    assert create_session_raw({"total_chunks": 3, "dependencies": {"1": [1]}}).status_code == 422
    # 环：二元环与三元环
    assert create_session_raw(
        {"total_chunks": 3, "dependencies": {"0": [1], "1": [0]}}).status_code == 422
    assert create_session_raw(
        {"total_chunks": 3, "dependencies": {"0": [1], "1": [2], "2": [0]}}).status_code == 422
    # 非数值键
    assert create_session_raw({"total_chunks": 3, "dependencies": {"x": [0]}}).status_code == 422
    # 合法依赖（含重复值，服务端去重）可创建
    assert create_session_raw(
        {"total_chunks": 3, "dependencies": {"2": [0, 1, 1]}}).status_code == 201


def test_lease_blocked_until_dependencies_complete():
    s = make_session_with_deps(3, {2: [0, 1]})

    # 前置全部未完成：409 + 按序缺失前置
    r = lease(s["id"], 2)
    assert r.status_code == 409
    assert r.json()["missing_dependencies"] == [0, 1]

    # 完成一个前置后仍被阻塞，缺失列表随之缩小
    t0 = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    r = lease(s["id"], 2)
    assert r.status_code == 409
    assert r.json()["missing_dependencies"] == [1]

    # 前置全部完成后无需额外操作即可领取
    t1 = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 1, t1, "1" * 64).status_code == 200
    assert lease(s["id"], 2).status_code == 200

    # 无依赖的分片不受影响
    s2 = make_session()
    assert lease(s2["id"], 2).status_code == 200


def test_status_exposes_dependencies():
    s = make_session_with_deps(3, {2: [0, 1]})
    st = client.get(f"/sessions/{s['id']}").json()
    assert [c["dependencies"] for c in st["chunks"]] == [[], [], [0, 1]]

    # 未提供依赖的会话：每个分片 dependencies 为空
    st2 = client.get(f"/sessions/{make_session()['id']}").json()
    assert [c["dependencies"] for c in st2["chunks"]] == [[], [], []]


def test_ready_endpoint_lists_ready_and_blocked():
    s = make_session_with_deps(4, {2: [0], 3: [1, 2]})

    # 初始：0、1 可领取；2 等 0，3 等 1 和 2
    r = ready(s["id"])
    assert r["ready_chunks"] == [0, 1]
    assert r["blocked_chunks"] == [
        {"chunk_index": 2, "missing_dependencies": [0]},
        {"chunk_index": 3, "missing_dependencies": [1, 2]},
    ]

    # 持有有效租约的分片不再出现在 ready 中
    t0 = lease(s["id"], 0).json()["lease_token"]
    assert ready(s["id"])["ready_chunks"] == [1]

    # 完成 0 后 2 解锁
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    r = ready(s["id"])
    assert r["ready_chunks"] == [1, 2]
    assert r["blocked_chunks"] == [{"chunk_index": 3, "missing_dependencies": [1, 2]}]

    # 链式推进：完成 1、2 后 3 解锁
    t1 = lease(s["id"], 1).json()["lease_token"]
    assert complete(s["id"], 1, t1, "1" * 64).status_code == 200
    t2 = lease(s["id"], 2).json()["lease_token"]
    assert complete(s["id"], 2, t2, "2" * 64).status_code == 200
    r = ready(s["id"])
    assert r["ready_chunks"] == [3]
    assert r["blocked_chunks"] == []


def test_ready_endpoint_treats_expired_lease_as_claimable():
    s = make_session_with_deps(2, {1: [0]})

    lease(s["id"], 0)
    assert ready(s["id"])["ready_chunks"] == []

    # 租约过期后恢复为可领取
    expire_lease(s["id"], 0)
    assert ready(s["id"])["ready_chunks"] == [0]

    # 无依赖会话：全部 pending 分片均可领取，无阻塞
    s2 = make_session()
    r = ready(s2["id"])
    assert r["ready_chunks"] == [0, 1, 2]
    assert r["blocked_chunks"] == []


def test_ready_unknown_session():
    assert client.get("/sessions/nope/ready").status_code == 404


def test_finalize_with_dependencies():
    s = make_session_with_deps(2, {1: [0]})
    # 被依赖阻塞时无法完成后续分片，提交自然列出缺失
    r = client.post(f"/sessions/{s['id']}/finalize")
    assert r.status_code == 409
    assert r.json()["missing_chunks"] == [0, 1]

    for i in (0, 1):
        t = lease(s["id"], i).json()["lease_token"]
        assert complete(s["id"], i, t, str(i) * 64).status_code == 200
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200


# ---------------------------------------------------------------- 会话级并发上限与批量领取

def make_capped_session(total_chunks=5, max_active_leases=2, dependencies=None):
    payload = {
        "total_chunks": total_chunks,
        "file_digest": "f" * 64,
        "digest_algorithm": "sha256",
        "max_active_leases": max_active_leases,
    }
    if dependencies is not None:
        payload["dependencies"] = {str(k): v for k, v in dependencies.items()}
    resp = client.post("/sessions", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


def batch_lease(session_id, count, ttl=None):
    body = {"count": count}
    if ttl is not None:
        body["lease_ttl_seconds"] = ttl
    return client.post(f"/sessions/{session_id}/leases/batch", json=body)


def test_create_session_persists_max_active_leases():
    s = make_capped_session(max_active_leases=3)
    assert s["max_active_leases"] == 3

    st = client.get(f"/sessions/{s['id']}").json()
    assert st["max_active_leases"] == 3
    assert st["active_leases"] == 0

    # 缺省 max_active_leases：上限与槽位均为 null（保持旧行为）
    plain = make_session()
    plain_st = client.get(f"/sessions/{plain['id']}").json()
    assert plain_st["max_active_leases"] is None
    assert plain_st["active_leases"] == 0


def test_invalid_max_active_leases_rejected():
    assert create_session_raw(
        {"total_chunks": 3, "max_active_leases": 0}).status_code == 422
    assert create_session_raw(
        {"total_chunks": 3, "max_active_leases": -1}).status_code == 422


def test_batch_lease_claims_in_index_order():
    s = make_capped_session(total_chunks=5, max_active_leases=10)
    r = batch_lease(s["id"], 3, ttl=120)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["requested"] == 3
    assert [g["chunk_index"] for g in body["granted"]] == [0, 1, 2]
    assert len({g["lease_token"] for g in body["granted"]}) == 3  # 令牌各自不同
    assert body["active"] == 3
    assert body["blocked"] == 0
    assert body["max_active_leases"] == 10
    assert body["remaining_slots"] == 7
    # 同批租约采用统一时长，过期时间一致
    expires = {g["expires_at"] for g in body["granted"]}
    assert len(expires) == 1

    # 第二次批量领取跳过已占用分片，按序号继续向后领取
    r2 = batch_lease(s["id"], 10)
    assert [g["chunk_index"] for g in r2.json()["granted"]] == [3, 4]
    assert r2.json()["active"] == 5
    assert r2.json()["remaining_slots"] == 5

    # 状态查询中的有效租约数同步更新
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["active_leases"] == 5


def test_batch_lease_respects_remaining_slots():
    s = make_capped_session(total_chunks=6, max_active_leases=2)
    # 首批受槽位限制，只发 2 个，尽管请求 5 个、可领取 6 个
    r = batch_lease(s["id"], 5)
    body = r.json()
    assert [g["chunk_index"] for g in body["granted"]] == [0, 1]
    assert body["active"] == 2
    assert body["remaining_slots"] == 0
    assert body["blocked"] == 0
    # 槽位被占满且仍有未完成分片等待：next_expiry 给出最早过期时间
    assert body["next_expiry"] is not None
    expiry = datetime.fromisoformat(body["next_expiry"])
    assert expiry > datetime.now(timezone.utc)

    # 槽位占满：返回空列表与汇总，不新增租约
    r2 = batch_lease(s["id"], 3)
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["granted"] == []
    assert body2["active"] == 2
    assert body2["remaining_slots"] == 0
    assert body2["blocked"] == 0
    assert body2["next_expiry"] == body["next_expiry"]

    # 释放一个分片后，批量领取只占用新腾出的 1 个槽位
    t0 = next(g["lease_token"] for g in body["granted"] if g["chunk_index"] == 0)
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    r3 = batch_lease(s["id"], 3)
    assert [g["chunk_index"] for g in r3.json()["granted"]] == [2]
    assert r3.json()["active"] == 2
    assert r3.json()["remaining_slots"] == 0


def test_batch_lease_treats_expired_leases_as_free():
    s = make_capped_session(total_chunks=3, max_active_leases=2)
    assert len(batch_lease(s["id"], 2).json()["granted"]) == 2
    # 槽位占满
    assert batch_lease(s["id"], 1).json()["granted"] == []

    # 一个租约过期：同时释放分片资格与并发槽位
    expire_lease(s["id"], 0)
    r = batch_lease(s["id"], 2)
    body = r.json()
    assert [g["chunk_index"] for g in body["granted"]] == [0]
    assert body["active"] == 2  # 过期租约不计入，新发放后重新占满


def test_batch_lease_skips_blocked_dependencies():
    # 0 可领取；1 依赖 0；2 无依赖；3 依赖 1
    s = make_capped_session(total_chunks=4, max_active_leases=10,
                            dependencies={1: [0], 3: [1]})
    r = batch_lease(s["id"], 10)
    body = r.json()
    # 仅前置已完成（此处为无依赖）的 0、2 可领取，按序号升序
    assert [g["chunk_index"] for g in body["granted"]] == [0, 2]
    assert body["blocked"] == 2  # 1、3 被依赖阻塞

    # 完成 0 后，1 解锁可领取；3 仍被 1 阻塞
    t0 = next(g["lease_token"] for g in body["granted"] if g["chunk_index"] == 0)
    assert complete(s["id"], 0, t0, "0" * 64).status_code == 200
    r2 = batch_lease(s["id"], 10)
    body2 = r2.json()
    assert [g["chunk_index"] for g in body2["granted"]] == [1]
    assert body2["blocked"] == 1

    # 无依赖图、全部分片已被领取/完成：空列表 + blocked=0、next_expiry=null 时也正常
    s2 = make_capped_session(total_chunks=1, max_active_leases=5)
    assert len(batch_lease(s2["id"], 1).json()["granted"]) == 1
    tail = batch_lease(s2["id"], 1).json()
    assert tail["granted"] == []
    assert tail["blocked"] == 0
    assert tail["active"] == 1
    assert tail["next_expiry"] is not None


def test_batch_lease_without_cap_behaves_like_unlimited():
    # 旧语义会话（max_active_leases 缺省）：批量领取不受并发上限约束
    s = make_session(total_chunks=4)
    r = batch_lease(s["id"], 10)
    body = r.json()
    assert [g["chunk_index"] for g in body["granted"]] == [0, 1, 2, 3]
    assert body["max_active_leases"] is None
    assert body["remaining_slots"] is None
    assert batch_lease(s["id"], 1).json()["granted"] == []


def test_single_lease_enforces_session_cap():
    # 单片接口不得绕过会话并发上限
    s = make_capped_session(total_chunks=4, max_active_leases=2)
    assert lease(s["id"], 0).status_code == 200
    assert lease(s["id"], 1).status_code == 200
    r = lease(s["id"], 2)
    assert r.status_code == 409
    assert r.json()["max_active_leases"] == 2
    assert r.json()["active_leases"] == 2

    # 租约过期后单片领取同样把过期租约视为空闲槽位
    expire_lease(s["id"], 0)
    assert lease(s["id"], 2).status_code == 200


def test_batch_and_single_leases_compete_atomically_under_concurrency():
    s = make_capped_session(total_chunks=10, max_active_leases=4)
    results: list[list[int]] = []
    results_lock = threading.Lock()
    barrier = threading.Barrier(4)

    def worker(kind: str):
        barrier.wait()  # 尽量同时发起，迫使临界区串行化资格计算与写入
        if kind == "batch":
            granted = [g["chunk_index"]
                       for g in batch_lease(s["id"], 3).json()["granted"]]
        else:
            r = lease(s["id"], 0)  # 具体序号无关，只关心是否占用到唯一分片
            granted = []
            if r.status_code == 200:
                # 单片接口只能拿到 0；若已被批量领取则应为 409
                granted = [0]
        with results_lock:
            results.append(granted)

    threads = [
        threading.Thread(target=worker, args=("batch",)),
        threading.Thread(target=worker, args=("batch",)),
        threading.Thread(target=worker, args=("single",)),
        threading.Thread(target=worker, args=("batch",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    claimed = [idx for granted in results for idx in granted]
    # 不重复分配
    assert len(claimed) == len(set(claimed))
    # 不突破并发上限
    assert len(claimed) <= 4
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["active_leases"] == len(claimed)
    assert st["active_leases"] <= 4


def test_batch_lease_request_validation():
    s = make_session()
    assert client.post(
        f"/sessions/{s['id']}/leases/batch", json={"count": 0}).status_code == 422
    assert client.post(
        f"/sessions/{s['id']}/leases/batch", json={"count": -1}).status_code == 422
    assert client.post(
        f"/sessions/{s['id']}/leases/batch", json={}).status_code == 422
    assert client.post(
        "/sessions/nope/leases/batch", json={"count": 1}).status_code == 404


def test_batch_lease_blocked_after_finalize():
    s = make_capped_session(total_chunks=1, max_active_leases=2)
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200
    assert batch_lease(s["id"], 1).status_code == 409


def test_migration_adds_max_active_leases_to_legacy_database(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy.db"
    conn = sqlite3.connect(legacy)
    # 旧版本 SCHEMA：sessions 无 max_active_leases 列
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
            file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL, finalized_at TEXT
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
            lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
            PRIMARY KEY (session_id, chunk_index)
        );
        CREATE TABLE chunk_dependencies (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            depends_on INTEGER NOT NULL,
            PRIMARY KEY (session_id, chunk_index, depends_on)
        );
        INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm,
                              status, created_at)
            VALUES ('legacy-sid', 2, 'dig', 'sha256', 'active', '2026-01-01T00:00:00+00:00');
        INSERT INTO chunks (session_id, chunk_index) VALUES ('legacy-sid', 0), ('legacy-sid', 1);
        """
    )
    conn.commit()
    conn.close()

    # 通过独立的连接对象复用迁移逻辑（不影响进程级测试连接）
    import app.db as db

    migrated = sqlite3.connect(legacy)
    db.run_migrations(migrated)
    cols = {r[1] for r in migrated.execute("PRAGMA table_info(sessions)").fetchall()}
    assert "max_active_leases" in cols
    # 旧会话上限为 NULL：迁移幂等且保持“不限并发”的旧行为
    row = migrated.execute(
        "SELECT max_active_leases FROM sessions WHERE id = 'legacy-sid'"
    ).fetchone()
    assert row[0] is None
    db.run_migrations(migrated)  # 再次执行不报错
    migrated.close()


# ---------------------------------------------------------------- 失败上报与退避重试

def make_retry_session(total_chunks=2, base=5, max_=300, **kw):
    payload = {
        "total_chunks": total_chunks,
        "file_digest": "f" * 64,
        "digest_algorithm": "sha256",
        "base_retry_seconds": base,
        "max_retry_seconds": max_,
    }
    payload.update(kw)
    resp = client.post("/sessions", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


def test_retry_settings_defaults_and_persistence():
    s = make_session()
    assert s["base_retry_seconds"] == 5
    assert s["max_retry_seconds"] == 300
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["base_retry_seconds"] == 5
    assert st["max_retry_seconds"] == 300
    # 分片初始无失败计数与冷却
    c = st["chunks"][0]
    assert c["failure_count"] == 0
    assert c["retry_after"] is None


def test_invalid_retry_settings_rejected():
    # 非正数
    assert create_session_raw(
        {"total_chunks": 3, "base_retry_seconds": 0}).status_code == 422
    assert create_session_raw(
        {"total_chunks": 3, "max_retry_seconds": -5}).status_code == 422
    # 上限小于基数
    assert create_session_raw(
        {"total_chunks": 3, "base_retry_seconds": 100,
         "max_retry_seconds": 99}).status_code == 422
    # 相等是允许的
    assert create_session_raw(
        {"total_chunks": 3, "base_retry_seconds": 60,
         "max_retry_seconds": 60}).status_code == 201


def test_fail_requires_valid_lease_token():
    s = make_retry_session()
    # 未领取租约直接上报失败
    assert fail(s["id"], 0, "no-such-token").status_code == 409

    token = lease(s["id"], 0, ttl=3600).json()["lease_token"]
    # 令牌不匹配
    assert fail(s["id"], 0, "wrong-token").status_code == 409
    # 正确令牌：成功，计数 1，返回冷却结束时刻
    r = fail(s["id"], 0, token)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "cooling"
    assert body["failure_count"] == 1
    retry_at = datetime.fromisoformat(body["retry_after"])
    assert retry_at > datetime.now(timezone.utc)

    # 租约已被原子清除：原令牌不可再上报失败
    assert fail(s["id"], 0, token).status_code == 409

    # 租约过期后上报失败同样被拒绝
    s2 = make_retry_session()
    t2 = lease(s2["id"], 0).json()["lease_token"]
    expire_lease(s2["id"], 0)
    assert fail(s2["id"], 0, t2).status_code == 409


def test_fail_on_completed_or_finalized_chunk_rejected():
    s = make_retry_session()
    token = lease(s["id"], 0, ttl=3600).json()["lease_token"]
    assert complete(s["id"], 0, token, "a" * 64).status_code == 200
    # 已完成分片不可再上报失败
    assert fail(s["id"], 0, token).status_code == 409

    token1 = lease(s["id"], 1, ttl=3600).json()["lease_token"]
    assert complete(s["id"], 1, token1, "b" * 64).status_code == 200
    assert client.post(f"/sessions/{s['id']}/finalize").status_code == 200
    # 已提交会话中的（已完成）分片再上报失败仍被拒绝
    assert fail(s["id"], 0, token).status_code == 409


def clear_cooldown(session_id, index):
    get_conn().execute(
        "UPDATE chunks SET retry_after = '2000-01-01T00:00:00+00:00'"
        " WHERE session_id = ? AND chunk_index = ?",
        (session_id, index),
    )
    get_conn().commit()


def test_exponential_backoff_delay_sequence():
    s = make_retry_session(total_chunks=1, base=10, max_=100)
    sid = s["id"]

    def fail_and_expect_delay(delay_seconds):
        token = lease(sid, 0, ttl=3600).json()["lease_token"]
        r = fail(sid, 0, token)
        assert r.status_code == 200, r.text
        body = r.json()
        retry_at = datetime.fromisoformat(body["retry_after"])
        now = datetime.now(timezone.utc)
        delta = (retry_at - now).total_seconds()
        assert delay_seconds - 2 <= delta <= delay_seconds + 2
        return body["failure_count"]

    # 10, 20, 40, 80, 100（上限截断）, 100 ...
    assert fail_and_expect_delay(10) == 1
    clear_cooldown(sid, 0)
    assert fail_and_expect_delay(20) == 2
    clear_cooldown(sid, 0)
    assert fail_and_expect_delay(40) == 3
    clear_cooldown(sid, 0)
    assert fail_and_expect_delay(80) == 4
    clear_cooldown(sid, 0)
    assert fail_and_expect_delay(100) == 5
    clear_cooldown(sid, 0)
    assert fail_and_expect_delay(100) == 6


def test_cooling_chunk_not_claimable_single_or_batch():
    s = make_retry_session(total_chunks=3, base=100, max_=300)
    sid = s["id"]
    token = lease(sid, 0, ttl=3600).json()["lease_token"]
    r = fail(sid, 0, token)
    assert r.status_code == 200
    retry_at = r.json()["retry_after"]

    # 单片领取：409 + retry_after / failure_count
    r2 = lease(sid, 0)
    assert r2.status_code == 409
    assert r2.json()["detail"] == "chunk is in retry backoff"
    assert r2.json()["retry_after"] == retry_at
    assert r2.json()["failure_count"] == 1

    # 批量领取跳过冷却分片，只发其它可领取分片
    rb = batch_lease(sid, 10)
    assert [g["chunk_index"] for g in rb.json()["granted"]] == [1, 2]

    # /ready：冷却分片既不在 ready 也不在 blocked，而在 cooling_chunks
    rr = ready(sid)
    assert rr["ready_chunks"] == []
    assert rr["blocked_chunks"] == []
    assert rr["cooling_chunks"] == [
        {"chunk_index": 0, "retry_after": retry_at, "failure_count": 1}]

    # 冷却结束后单片与批量均可再次领取，且失败计数已递增保留
    clear_cooldown(sid, 0)
    assert lease(sid, 0, ttl=3600).status_code == 200
    st = client.get(f"/sessions/{sid}").json()
    assert st["chunks"][0]["failure_count"] == 1
    assert st["chunks"][0]["retry_after"] is None


def test_status_reports_cooling_state():
    s = make_retry_session(total_chunks=1, base=100)
    sid = s["id"]
    token = lease(sid, 0, ttl=3600).json()["lease_token"]
    r = fail(sid, 0, token)
    retry_at = r.json()["retry_after"]

    st = client.get(f"/sessions/{sid}").json()
    c = st["chunks"][0]
    assert c["status"] == "cooling"
    assert c["retry_after"] == retry_at
    assert c["failure_count"] == 1

    # 冷却结束后读时恢复为 pending，retry_after 清空，计数保留
    clear_cooldown(sid, 0)
    st2 = client.get(f"/sessions/{sid}").json()
    c2 = st2["chunks"][0]
    assert c2["status"] == "pending"
    assert c2["retry_after"] is None
    assert c2["failure_count"] == 1


def test_complete_clears_backoff_but_keeps_failure_count():
    s = make_retry_session(total_chunks=1, base=100)
    sid = s["id"]
    # 失败一次进入冷却
    token = lease(sid, 0, ttl=3600).json()["lease_token"]
    assert fail(sid, 0, token).status_code == 200

    # 冷却结束后重新领取并完成
    clear_cooldown(sid, 0)
    token2 = lease(sid, 0, ttl=3600).json()["lease_token"]
    digest = "a" * 64
    rc = complete(sid, 0, token2, digest)
    assert rc.status_code == 200

    st = client.get(f"/sessions/{sid}").json()
    c = st["chunks"][0]
    assert c["status"] == "completed"
    assert c["retry_after"] is None       # 退避状态已清除
    assert c["failure_count"] == 1        # 失败计数保留
    assert c["chunk_digest"] == digest

    # 完成后不会再出现在 /ready 的冷却列表中
    rr = ready(sid)
    assert rr["cooling_chunks"] == []
    assert rr["ready_chunks"] == []


def test_batch_next_expiry_considers_backoff_end():
    # 无有效租约、仅冷却中的分片：next_expiry 取退避结束时间
    s = make_retry_session(total_chunks=2, base=100, max_=300)
    sid = s["id"]
    token = lease(sid, 0, ttl=3600).json()["lease_token"]
    rfail = fail(sid, 0, token)
    retry_at = rfail.json()["retry_after"]

    rb = batch_lease(sid, 10)
    body = rb.json()
    assert [g["chunk_index"] for g in body["granted"]] == [1]
    # 1 号租约 3600s 后过期，0 号约 100s 后退避结束 => 最早为退避结束
    assert body["next_expiry"] == retry_at

    # /ready 同样透出该时间
    assert ready(sid)["next_expiry"] == retry_at


def test_batch_next_expiry_picks_earlier_lease_expiry():
    # 一个分片持约 60s 后过期的租约，另一个分片在约 1000s 的长冷却中：
    # next_expiry 应取更早的租约过期时间，而非退避结束时间
    from datetime import timedelta
    s = make_retry_session(total_chunks=3, base=1000, max_=3600)
    sid = s["id"]
    token0 = lease(sid, 0, ttl=3600).json()["lease_token"]
    fail(sid, 0, token0)  # 0 进入约 1000s 冷却
    batch_lease(sid, 10)  # 领取 1、2（默认 TTL 300s）
    # 将 1 号租约提前到约 60s 后过期
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = ? WHERE session_id = ? AND chunk_index = 1",
        ((datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat(), sid),
    )
    get_conn().commit()

    body = batch_lease(sid, 10).json()
    assert body["granted"] == []
    next_at = datetime.fromisoformat(body["next_expiry"])
    now = datetime.now(timezone.utc)
    assert 55 < (next_at - now).total_seconds() < 70


def test_fail_unknown_session_and_chunk():
    assert fail("nope", 0, "t").status_code == 404
    s = make_retry_session()
    assert fail(s["id"], 99, "t").status_code == 404


def test_cooling_chunk_with_unmet_dependencies():
    # 1 号依赖 0 号：通过直接插入依赖行制造“前置未完成 + 处于冷却”的组合状态，
    # 验证批量不发放、ready 同时在 blocked 与 cooling 中列出
    s = make_retry_session(total_chunks=2, base=100, max_=300)
    sid = s["id"]
    token = lease(sid, 1, ttl=3600).json()["lease_token"]
    get_conn().execute(
        "INSERT INTO chunk_dependencies (session_id, chunk_index, depends_on)"
        " VALUES (?, 1, 0)",
        (sid,),
    )
    get_conn().commit()
    assert fail(sid, 1, token).status_code == 200

    rr = ready(sid)
    assert rr["ready_chunks"] == [0]
    assert rr["blocked_chunks"] == [{"chunk_index": 1, "missing_dependencies": [0]}]
    assert [c["chunk_index"] for c in rr["cooling_chunks"]] == [1]
    # 单片领取：依赖检查先于冷却检查，返回缺失依赖
    assert lease(sid, 1).status_code == 409
    # 批量领取只拿到 0
    assert [g["chunk_index"] for g in batch_lease(sid, 10).json()["granted"]] == [0]


def test_migration_adds_retry_columns_to_legacy_database(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy_retry.db"
    conn = sqlite3.connect(legacy)
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
            file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
            status TEXT NOT NULL DEFAULT 'active',
            max_active_leases INTEGER,
            created_at TEXT NOT NULL, finalized_at TEXT
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', lease_token TEXT,
            lease_expires_at TEXT, chunk_digest TEXT, completed_at TEXT,
            PRIMARY KEY (session_id, chunk_index)
        );
        INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm,
                              status, created_at)
            VALUES ('legacy-sid', 1, 'dig', 'sha256', 'active',
                    '2026-01-01T00:00:00+00:00');
        INSERT INTO chunks (session_id, chunk_index) VALUES ('legacy-sid', 0);
        """
    )
    conn.commit()
    conn.close()

    import app.db as db

    migrated = sqlite3.connect(legacy)
    db.run_migrations(migrated)  # 幂等执行多次
    db.run_migrations(migrated)

    session_cols = {r[1] for r in migrated.execute("PRAGMA table_info(sessions)").fetchall()}
    assert {"base_retry_seconds", "max_retry_seconds"} <= session_cols
    chunk_cols = {r[1] for r in migrated.execute("PRAGMA table_info(chunks)").fetchall()}
    assert {"failure_count", "retry_after"} <= chunk_cols

    # 旧会话回填默认退避参数
    row = migrated.execute(
        "SELECT base_retry_seconds, max_retry_seconds FROM sessions"
        " WHERE id = 'legacy-sid'"
    ).fetchone()
    assert row == (5, 300)
    # 旧分片失败计数为 0、无冷却
    row = migrated.execute(
        "SELECT failure_count, retry_after FROM chunks"
        " WHERE session_id = 'legacy-sid' AND chunk_index = 0"
    ).fetchone()
    assert row[0] == 0 and row[1] is None
    migrated.close()
