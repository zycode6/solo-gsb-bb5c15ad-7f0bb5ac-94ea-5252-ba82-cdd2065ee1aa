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

def fail(session_id, index, token):
    return client.post(
        f"/sessions/{session_id}/chunks/{index}/fail",
        json={"lease_token": token},
    )


def set_backoff(session_id, index, when):
    """直接写退避截止时间，避免真实等待。when 为 ISO 字符串或 None。"""
    get_conn().execute(
        "UPDATE chunks SET backoff_until = ? WHERE session_id = ? AND chunk_index = ?",
        (when, session_id, index),
    )
    get_conn().commit()


PAST = "2000-01-01T00:00:00+00:00"
FUTURE = "2999-01-01T00:00:00+00:00"


def make_retry_session(total_chunks=3, base_retry_seconds=5, max_retry_seconds=300,
                       max_active_leases=None):
    payload = {
        "total_chunks": total_chunks,
        "file_digest": "f" * 64,
        "base_retry_seconds": base_retry_seconds,
        "max_retry_seconds": max_retry_seconds,
    }
    if max_active_leases is not None:
        payload["max_active_leases"] = max_active_leases
    resp = client.post("/sessions", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


def lease_fail_retry(session_id, index):
    """领取→失败上报；返回 fail 响应（此时分片处于冷却中）。"""
    token = lease(session_id, index).json()["lease_token"]
    r = fail(session_id, index, token)
    assert r.status_code == 200, r.text
    return r.json()


def test_session_retry_defaults_persisted():
    s = make_session()
    assert s["base_retry_seconds"] == 5
    assert s["max_retry_seconds"] == 300
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["base_retry_seconds"] == 5
    assert st["max_retry_seconds"] == 300
    # 新分片失败计数为 0、无冷却
    assert st["chunks"][0]["failure_count"] == 0
    assert st["chunks"][0]["retry_after"] is None


def test_retry_params_validation():
    ok = {"total_chunks": 2, "file_digest": "f" * 64}
    assert client.post("/sessions", json={**ok, "base_retry_seconds": 0}).status_code == 422
    assert client.post("/sessions", json={**ok, "max_retry_seconds": 0}).status_code == 422
    assert client.post("/sessions", json={**ok, "base_retry_seconds": -1}).status_code == 422
    # max 必须不小于 base
    assert client.post(
        "/sessions", json={**ok, "base_retry_seconds": 10, "max_retry_seconds": 9}
    ).status_code == 422
    # 相等合法
    assert client.post(
        "/sessions", json={**ok, "base_retry_seconds": 10, "max_retry_seconds": 10}
    ).status_code == 201


def test_fail_requires_valid_lease_token():
    s = make_session()
    # 未持有租约
    assert fail(s["id"], 0, "no-such-token").status_code == 409
    # 令牌错误
    token = lease(s["id"], 0).json()["lease_token"]
    assert fail(s["id"], 0, "wrong-token").status_code == 409
    # 租约过期后不能上报失败
    expire_lease(s["id"], 0)
    assert fail(s["id"], 0, token).status_code == 409
    # 未知会话 / 分片
    assert fail("nope", 0, "t").status_code == 404
    assert fail(s["id"], 99, "t").status_code == 404


def test_fail_cannot_report_completed_or_finalized():
    s = make_session()
    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200
    assert fail(s["id"], 0, token).status_code == 409  # 已完成不可失败

    s2 = make_session(total_chunks=1)
    t = lease(s2["id"], 0).json()["lease_token"]
    assert complete(s2["id"], 0, t, "0" * 64).status_code == 200
    assert client.post(f"/sessions/{s2['id']}/finalize").status_code == 200
    assert fail(s2["id"], 0, t).status_code == 409  # 已提交会话只读


def test_fail_clears_lease_and_increments_failures_atomically():
    s = make_retry_session()

    body = lease_fail_retry(s["id"], 0)
    assert body["status"] == "pending"
    assert body["failure_count"] == 1
    assert body["retry_after_seconds"] == 5  # base × 2^0

    # 租约已被原子清除：冷却期间重新领取被拒，旧令牌不能再完成
    assert lease(s["id"], 0).status_code == 409
    assert complete(s["id"], 0, "whatever-token", "0" * 64).status_code == 409
    st = client.get(f"/sessions/{s['id']}").json()
    c0 = st["chunks"][0]
    assert c0["status"] == "pending"
    assert c0["lease_expires_at"] is None
    assert c0["failure_count"] == 1
    assert c0["retry_after"] is not None
    assert st["active_leases"] == 0

    # 第二次失败：计数 2，冷却 10 秒
    set_backoff(s["id"], 0, PAST)
    body2 = lease_fail_retry(s["id"], 0)
    assert body2["failure_count"] == 2
    assert body2["retry_after_seconds"] == 10
    assert datetime.fromisoformat(body2["retry_after"]) > datetime.now(timezone.utc)


def test_exponential_backoff_is_capped_at_max():
    s = make_retry_session(base_retry_seconds=2, max_retry_seconds=10)
    expected = [2, 4, 8, 10, 10]  # 2×2^(n-1)，封顶 10
    for n, delay in enumerate(expected, start=1):
        body = lease_fail_retry(s["id"], 0)
        assert body["failure_count"] == n
        assert body["retry_after_seconds"] == delay
        set_backoff(s["id"], 0, PAST)  # 冷却结束才能再次领取

    # 默认参数下第 7 次起封顶 300
    s2 = make_retry_session()
    delays = []
    for _ in range(7):
        delays.append(lease_fail_retry(s2["id"], 0)["retry_after_seconds"])
        set_backoff(s2["id"], 0, PAST)
    assert delays == [5, 10, 20, 40, 80, 160, 300]


def test_cooling_chunk_not_claimable_single_or_batch():
    s = make_retry_session(total_chunks=3)
    lease_fail_retry(s["id"], 0)  # 分片 0 进入冷却

    # 单片领取：409 且响应体带 failure_count / retry_after
    r = lease(s["id"], 0)
    assert r.status_code == 409
    assert r.json()["failure_count"] == 1
    assert r.json()["retry_after"] is not None

    # /ready：0 既不在 ready 也不在 blocked，而在 cooling_chunks
    rdy = ready(s["id"])
    assert rdy["ready_chunks"] == [1, 2]
    assert rdy["blocked_chunks"] == []
    assert len(rdy["cooling_chunks"]) == 1
    cooling0 = rdy["cooling_chunks"][0]
    assert cooling0["chunk_index"] == 0
    assert cooling0["failure_count"] == 1
    assert datetime.fromisoformat(cooling0["retry_after"]) > datetime.now(timezone.utc)

    # 批量领取跳过冷却分片
    body = batch_lease(s["id"], 10).json()
    assert [g["chunk_index"] for g in body["granted"]] == [1, 2]

    # 会话状态：冷却中的分片 pending + retry_after
    st = client.get(f"/sessions/{s['id']}").json()
    c0 = st["chunks"][0]
    assert c0["status"] == "pending"
    assert c0["failure_count"] == 1
    assert c0["retry_after"] is not None


def test_chunk_claimable_again_after_backoff_expires():
    s = make_retry_session(total_chunks=2)
    lease_fail_retry(s["id"], 0)
    assert ready(s["id"])["ready_chunks"] == [1]
    assert [g["chunk_index"] for g in batch_lease(s["id"], 10).json()["granted"]] == [1]

    # 冷却结束：单片、批量均可再次领取，/ready 冷却列表清空
    set_backoff(s["id"], 0, PAST)
    rdy = ready(s["id"])
    assert 0 in rdy["ready_chunks"]
    assert rdy["cooling_chunks"] == []
    assert lease(s["id"], 0).status_code == 200

    # 状态读时口径：过期退避不再报告 retry_after，但失败次数保留
    st = client.get(f"/sessions/{s['id']}").json()
    assert st["chunks"][0]["retry_after"] is None
    assert st["chunks"][0]["failure_count"] == 1


def test_complete_clears_backoff_but_keeps_failure_count():
    s = make_retry_session()
    lease_fail_retry(s["id"], 0)
    set_backoff(s["id"], 0, PAST)

    token = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token, "0" * 64).status_code == 200

    st = client.get(f"/sessions/{s['id']}").json()
    c0 = st["chunks"][0]
    assert c0["status"] == "completed"
    assert c0["retry_after"] is None       # 退避状态已清除
    assert c0["failure_count"] == 1        # 失败次数保留


def test_batch_next_expiry_is_min_of_lease_and_backoff():
    # 分片 0 失败进入冷却；分片 1 持有效租约（TTL 300）
    s = make_retry_session(total_chunks=2)
    lease_fail_retry(s["id"], 0)
    token1 = lease(s["id"], 1, ttl=300).json()["lease_token"]

    # 冷却截止推到很远：next_expiry 取租约过期时间
    set_backoff(s["id"], 0, FUTURE)
    body = batch_lease(s["id"], 10).json()
    assert body["granted"] == []
    st = client.get(f"/sessions/{s['id']}").json()
    lease_expiry = st["chunks"][1]["lease_expires_at"]
    assert datetime.fromisoformat(body["next_expiry"]) == datetime.fromisoformat(lease_expiry)

    # 租约过期时间推得比冷却更远：next_expiry 取退避结束时间
    get_conn().execute(
        "UPDATE chunks SET lease_expires_at = ? WHERE session_id = ? AND chunk_index = 1",
        (FUTURE, s["id"]),
    )
    get_conn().commit()
    backoff_end = "2027-01-01T00:00:00+00:00"
    set_backoff(s["id"], 0, backoff_end)
    body = batch_lease(s["id"], 10).json()
    assert datetime.fromisoformat(body["next_expiry"]) == datetime.fromisoformat(backoff_end)

    # 分片 1 完成：仅余冷却中的 0、无有效租约，next_expiry 即退避结束时间
    assert complete(s["id"], 1, token1, "1" * 64).status_code == 200
    body = batch_lease(s["id"], 10).json()
    assert body["granted"] == []
    assert datetime.fromisoformat(body["next_expiry"]) == datetime.fromisoformat(backoff_end)

    # 0 冷却结束后领取并完成：无未完成分片，也无任何等待事件，next_expiry 为 null
    set_backoff(s["id"], 0, PAST)
    token0 = lease(s["id"], 0).json()["lease_token"]
    assert complete(s["id"], 0, token0, "0" * 64).status_code == 200
    body = batch_lease(s["id"], 10).json()
    assert body["granted"] == []
    assert body["next_expiry"] is None


def test_failure_releases_concurrency_slot():
    s = make_retry_session(total_chunks=2, max_active_leases=1)
    lease_fail_retry(s["id"], 0)  # 失败清除租约：槽位释放，但 0 冷却中
    # 单片接口受冷却拦截
    assert lease(s["id"], 0).status_code == 409
    # 槽位已空：另一个分片可领取；批量同样只发 1
    assert lease(s["id"], 1).status_code == 200
    assert batch_lease(s["id"], 1).json()["granted"] == []


def test_migration_adds_failure_and_retry_columns_to_legacy_database(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy2.db"
    conn = sqlite3.connect(legacy)
    # 0001 之后、本次改动之前的旧 SCHEMA：已有 max_active_leases，缺退避相关列
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, total_chunks INTEGER NOT NULL,
            file_digest TEXT NOT NULL, digest_algorithm TEXT NOT NULL DEFAULT 'sha256',
            status TEXT NOT NULL DEFAULT 'active', max_active_leases INTEGER,
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
    db.run_migrations(migrated)
    session_cols = {r[1] for r in migrated.execute("PRAGMA table_info(sessions)").fetchall()}
    chunk_cols = {r[1] for r in migrated.execute("PRAGMA table_info(chunks)").fetchall()}
    assert {"base_retry_seconds", "max_retry_seconds"} <= session_cols
    assert {"failure_count", "backoff_until"} <= chunk_cols

    # 旧会话取默认退避参数；旧分片失败计数 0、无冷却
    row = migrated.execute(
        "SELECT base_retry_seconds, max_retry_seconds FROM sessions WHERE id = 'legacy-sid'"
    ).fetchone()
    assert tuple(row) == (5, 300)
    row = migrated.execute(
        "SELECT failure_count, backoff_until FROM chunks"
        " WHERE session_id = 'legacy-sid' AND chunk_index = 0"
    ).fetchone()
    assert tuple(row) == (0, None)

    db.run_migrations(migrated)  # 再次执行幂等不报错
    migrated.close()
