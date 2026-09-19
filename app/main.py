"""断点分片上传协调 API。

仅协调元数据：不接收、不保存任何文件内容。
- 创建会话：记录总分片数与期望文件摘要，可选提交分片依赖图（越界/自依赖/成环将被拒绝）
- 领取租约：同一分片同一时刻仅一个有效租约，过期后可重新领取；
  声明了依赖的分片须等前置分片全部完成后才可领取
- 上报完成：需携带匹配租约令牌与分片摘要，重复上报幂等
- 提交会话：仅当全部分片完成，返回按序分片清单，否则列出缺失分片
- 状态查询：会话与逐分片状态（含各分片依赖）
- 就绪查询：当前可领取的分片，以及被依赖阻塞的分片与各自缺失的前置项
"""
from __future__ import annotations

import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .db import DB_LOCK, get_conn, init_db
from .schemas import (
    BatchLeaseItem,
    BatchLeaseRequest,
    BatchLeaseResponse,
    BlockedChunk,
    ChunkStatus,
    CompleteRequest,
    CompleteResponse,
    CreateSessionRequest,
    FinalizeResponse,
    LeaseRequest,
    LeaseResponse,
    ManifestEntry,
    ReadyResponse,
    SessionResponse,
    SessionStatusResponse,
)

DEFAULT_LEASE_TTL_SECONDS = int(os.environ.get("DEFAULT_LEASE_TTL_SECONDS", "300"))
MAX_LEASE_TTL_SECONDS = int(os.environ.get("MAX_LEASE_TTL_SECONDS", "3600"))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()  # 启动时建库建表
    yield


app = FastAPI(
    title="Chunked Upload Coordinator",
    description="断点分片上传协调服务：只管理元数据（会话 / 租约 / 分片摘要），不接收文件内容。",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------- 工具函数

def get_session_or_404(session_id: str):
    row = get_conn().execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="session not found")
    return row


def get_chunk_or_404(session_id: str, chunk_index: int):
    row = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? AND chunk_index = ?",
        (session_id, chunk_index),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="chunk not found")
    return row


def lease_valid(chunk, now: datetime) -> bool:
    return (
        chunk["lease_expires_at"] is not None
        and parse_ts(chunk["lease_expires_at"]) > now
    )


def load_dependencies(session_id: str) -> dict[int, list[int]]:
    """读取会话的依赖图：chunk_index -> 升序前置分片列表。"""
    rows = get_conn().execute(
        "SELECT chunk_index, depends_on FROM chunk_dependencies"
        " WHERE session_id = ? ORDER BY chunk_index, depends_on",
        (session_id,),
    ).fetchall()
    deps: dict[int, list[int]] = {}
    for row in rows:
        deps.setdefault(row["chunk_index"], []).append(row["depends_on"])
    return deps


def incomplete_dependencies(session_id: str, chunk_index: int) -> list[int]:
    """返回该分片尚未完成的前置分片（升序）。"""
    rows = get_conn().execute(
        "SELECT d.depends_on FROM chunk_dependencies d"
        " JOIN chunks c ON c.session_id = d.session_id AND c.chunk_index = d.depends_on"
        " WHERE d.session_id = ? AND d.chunk_index = ? AND c.status != 'completed'"
        " ORDER BY d.depends_on",
        (session_id, chunk_index),
    ).fetchall()
    return [row["depends_on"] for row in rows]


def count_active_leases(session_id: str, now: datetime) -> int:
    """会话内当前有效（未过期）租约数；已完成分片与过期租约均不计入。"""
    return get_conn().execute(
        "SELECT COUNT(*) AS n FROM chunks"
        " WHERE session_id = ? AND status != 'completed'"
        " AND lease_expires_at IS NOT NULL AND lease_expires_at > ?",
        (session_id, now.isoformat()),
    ).fetchone()["n"]


def earliest_expiry(session_id: str) -> Optional[datetime]:
    """会话内当前有效租约中最早的过期时间；无有效租约时返回 None。"""
    row = get_conn().execute(
        "SELECT MIN(lease_expires_at) AS earliest FROM chunks"
        " WHERE session_id = ? AND status != 'completed' AND lease_expires_at IS NOT NULL",
        (session_id,),
    ).fetchone()
    if row is None or row["earliest"] is None:
        return None
    expires_at = parse_ts(row["earliest"])
    return expires_at if expires_at > utcnow() else None


def claimable_chunks(session_id: str, now: datetime) -> tuple[list[int], int]:
    """临界区内调用：返回（按序号升序的可领取分片列表, 被依赖阻塞的未完成分片数）。

    可领取 = 未完成、无有效租约（过期租约视为空闲）且前置分片全部完成；
    被阻塞 = 未完成且至少一个前置分片尚未完成（无论该分片当前是否持有有效租约）。
    """
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    completed = {r["chunk_index"] for r in rows if r["status"] == "completed"}
    deps = load_dependencies(session_id)
    ready: list[int] = []
    blocked = 0
    for row in rows:
        if row["status"] == "completed":
            continue
        chunk_index = row["chunk_index"]
        if any(d not in completed for d in deps.get(chunk_index, [])):
            blocked += 1
        elif not lease_valid(row, now):
            ready.append(chunk_index)
    return ready, blocked


def conflict(message: str, **extra) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": message, **extra})


# ---------------------------------------------------------------- 会话

@app.post(
    "/sessions",
    status_code=201,
    response_model=SessionResponse,
    tags=["sessions"],
    summary="创建上传会话",
)
def create_session(req: CreateSessionRequest):
    now = utcnow()
    session_id = uuid.uuid4().hex
    deps = req.dependencies or {}
    with DB_LOCK:
        conn = get_conn()
        conn.execute(
            "INSERT INTO sessions (id, total_chunks, file_digest, digest_algorithm, status,"
            " max_active_leases, created_at)"
            " VALUES (?, ?, ?, ?, 'active', ?, ?)",
            (session_id, req.total_chunks, req.file_digest, req.digest_algorithm,
             req.max_active_leases, now.isoformat()),
        )
        conn.executemany(
            "INSERT INTO chunks (session_id, chunk_index) VALUES (?, ?)",
            [(session_id, i) for i in range(req.total_chunks)],
        )
        conn.executemany(
            "INSERT INTO chunk_dependencies (session_id, chunk_index, depends_on) VALUES (?, ?, ?)",
            [(session_id, idx, dep) for idx, dep_list in deps.items() for dep in dep_list],
        )
        conn.commit()
    return SessionResponse(
        id=session_id,
        total_chunks=req.total_chunks,
        file_digest=req.file_digest,
        digest_algorithm=req.digest_algorithm,
        status="active",
        max_active_leases=req.max_active_leases,
        created_at=now,
    )


@app.get(
    "/sessions/{session_id}",
    response_model=SessionStatusResponse,
    tags=["sessions"],
    summary="查询会话状态",
)
def get_session_status(session_id: str):
    session = get_session_or_404(session_id)
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    deps = load_dependencies(session_id)
    now = utcnow()

    chunks: list[ChunkStatus] = []
    missing: list[int] = []
    for row in rows:
        status = row["status"]
        lease_expires_at = parse_ts(row["lease_expires_at"]) if row["lease_expires_at"] else None
        if status == "leased" and (lease_expires_at is None or lease_expires_at <= now):
            status = "pending"  # 租约已过期，读时按可领取展示
            lease_expires_at = None
        if row["status"] != "completed":
            missing.append(row["chunk_index"])
        chunks.append(
            ChunkStatus(
                chunk_index=row["chunk_index"],
                status=status,
                lease_expires_at=lease_expires_at,
                chunk_digest=row["chunk_digest"],
                completed_at=parse_ts(row["completed_at"]) if row["completed_at"] else None,
                dependencies=deps.get(row["chunk_index"], []),
            )
        )

    return SessionStatusResponse(
        id=session["id"],
        status=session["status"],
        total_chunks=session["total_chunks"],
        completed_chunks=session["total_chunks"] - len(missing),
        missing_chunks=missing,
        file_digest=session["file_digest"],
        digest_algorithm=session["digest_algorithm"],
        max_active_leases=session["max_active_leases"],
        active_leases=sum(
            1 for c in chunks
            if c.status == "leased" and c.lease_expires_at is not None
        ),
        created_at=parse_ts(session["created_at"]),
        finalized_at=parse_ts(session["finalized_at"]) if session["finalized_at"] else None,
        chunks=chunks,
    )


# ---------------------------------------------------------------- 就绪查询

@app.get(
    "/sessions/{session_id}/ready",
    response_model=ReadyResponse,
    tags=["sessions"],
    summary="查询当前可领取的分片与被依赖阻塞的分片",
)
def get_ready_chunks(session_id: str):
    get_session_or_404(session_id)
    rows = get_conn().execute(
        "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
        (session_id,),
    ).fetchall()
    deps = load_dependencies(session_id)
    now = utcnow()

    completed = {r["chunk_index"] for r in rows if r["status"] == "completed"}
    ready: list[int] = []
    blocked: list[BlockedChunk] = []
    for row in rows:
        chunk_index = row["chunk_index"]
        if row["status"] == "completed":
            continue
        missing_deps = [d for d in deps.get(chunk_index, []) if d not in completed]
        if missing_deps:
            blocked.append(
                BlockedChunk(chunk_index=chunk_index, missing_dependencies=missing_deps)
            )
        elif not lease_valid(row, now):
            # 无租约或租约已过期，且前置已全部完成：立即可领取
            ready.append(chunk_index)

    return ReadyResponse(
        session_id=session_id,
        ready_chunks=ready,
        blocked_chunks=blocked,
    )


# ---------------------------------------------------------------- 租约

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/lease",
    response_model=LeaseResponse,
    tags=["leases"],
    summary="领取分片租约（限时，独占；前置分片未完成时返回 409）",
    responses={409: {"description": "租约冲突；前置未完成时响应体包含 missing_dependencies"}},
)
def claim_lease(
    session_id: str,
    chunk_index: int,
    req: Optional[LeaseRequest] = Body(default=None),
):
    ttl = DEFAULT_LEASE_TTL_SECONDS
    if req is not None and req.lease_ttl_seconds is not None:
        ttl = min(req.lease_ttl_seconds, MAX_LEASE_TTL_SECONDS)

    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        chunk = get_chunk_or_404(session_id, chunk_index)

        if chunk["status"] == "completed":
            raise HTTPException(status_code=409, detail="chunk already completed")
        if lease_valid(chunk, now):
            return conflict(
                "chunk lease already held",
                lease_expires_at=chunk["lease_expires_at"],
            )
        missing_deps = incomplete_dependencies(session_id, chunk_index)
        if missing_deps:
            return conflict(
                "chunk dependencies not completed",
                missing_dependencies=missing_deps,
            )
        # 会话级并发上限：已占满槽位（过期租约不计入）时拒绝，避免单片接口绕过批量上限
        max_active = session["max_active_leases"]
        active = count_active_leases(session_id, now)
        if max_active is not None and active >= max_active:
            return conflict(
                "session max active leases reached",
                max_active_leases=max_active,
                active_leases=active,
            )

        token = secrets.token_urlsafe(32)
        expires_at = now + timedelta(seconds=ttl)
        get_conn().execute(
            "UPDATE chunks SET status = 'leased', lease_token = ?, lease_expires_at = ?"
            " WHERE session_id = ? AND chunk_index = ?",
            (token, expires_at.isoformat(), session_id, chunk_index),
        )
        get_conn().commit()

    return LeaseResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        lease_token=token,
        expires_at=expires_at,
    )


@app.post(
    "/sessions/{session_id}/leases/batch",
    response_model=BatchLeaseResponse,
    tags=["leases"],
    summary="会话级原子批量领取租约（按分片序号，受并发上限约束）",
)
def claim_leases_batch(session_id: str, req: BatchLeaseRequest):
    """一次领取至多 count 个租约：资格计算与写入在同一临界区，保证并发请求不重复分配、
    不突破 max_active_leases；数量不超过请求值与剩余并发槽位；无可领取项时返回空列表。"""
    ttl = DEFAULT_LEASE_TTL_SECONDS
    if req.lease_ttl_seconds is not None:
        ttl = min(req.lease_ttl_seconds, MAX_LEASE_TTL_SECONDS)

    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")

        # 资格计算与写入均在 DB_LOCK 临界区内：其它领取请求无法穿插看到中间状态
        ready, blocked = claimable_chunks(session_id, now)
        max_active = session["max_active_leases"]
        active = count_active_leases(session_id, now)
        remaining = None if max_active is None else max(0, max_active - active)
        quota = req.count if remaining is None else min(req.count, remaining)

        granted: list[BatchLeaseItem] = []
        if quota > 0:
            expires_at = now + timedelta(seconds=ttl)
            conn = get_conn()
            for chunk_index in ready[:quota]:
                token = secrets.token_urlsafe(32)
                conn.execute(
                    "UPDATE chunks SET status = 'leased', lease_token = ?, lease_expires_at = ?"
                    " WHERE session_id = ? AND chunk_index = ?",
                    (token, expires_at.isoformat(), session_id, chunk_index),
                )
                granted.append(
                    BatchLeaseItem(
                        chunk_index=chunk_index,
                        lease_token=token,
                        expires_at=expires_at,
                    )
                )
            conn.commit()

        active_after = active + len(granted)
        next_expiry = earliest_expiry(session_id)

    return BatchLeaseResponse(
        session_id=session_id,
        requested=req.count,
        granted=granted,
        active=active_after,
        blocked=blocked,
        next_expiry=next_expiry,
        max_active_leases=max_active,
        remaining_slots=(
            None if max_active is None else max(0, max_active - active_after)
        ),
    )


# ---------------------------------------------------------------- 完成上报

@app.post(
    "/sessions/{session_id}/chunks/{chunk_index}/complete",
    response_model=CompleteResponse,
    tags=["chunks"],
    summary="上报分片完成（幂等）",
)
def complete_chunk(session_id: str, chunk_index: int, req: CompleteRequest):
    now = utcnow()
    with DB_LOCK:
        session = get_session_or_404(session_id)
        chunk = get_chunk_or_404(session_id, chunk_index)

        if chunk["status"] == "completed":
            # 幂等重放：同一租约令牌 + 同一摘要 => 返回首次完成结果
            if chunk["lease_token"] == req.lease_token and chunk["chunk_digest"] == req.chunk_digest:
                return CompleteResponse(
                    session_id=session_id,
                    chunk_index=chunk_index,
                    status="completed",
                    chunk_digest=chunk["chunk_digest"],
                    completed_at=parse_ts(chunk["completed_at"]),
                    idempotent_replay=True,
                )
            raise HTTPException(
                status_code=409,
                detail="chunk already completed with different lease token or digest",
            )

        if session["status"] == "finalized":
            raise HTTPException(status_code=409, detail="session already finalized")
        if chunk["lease_token"] is None or chunk["lease_token"] != req.lease_token:
            raise HTTPException(status_code=409, detail="lease token mismatch")
        if not lease_valid(chunk, now):
            raise HTTPException(status_code=409, detail="lease expired")

        get_conn().execute(
            "UPDATE chunks SET status = 'completed', chunk_digest = ?, completed_at = ?"
            " WHERE session_id = ? AND chunk_index = ?",
            (req.chunk_digest, now.isoformat(), session_id, chunk_index),
        )
        get_conn().commit()

    return CompleteResponse(
        session_id=session_id,
        chunk_index=chunk_index,
        status="completed",
        chunk_digest=req.chunk_digest,
        completed_at=now,
        idempotent_replay=False,
    )


# ---------------------------------------------------------------- 提交

@app.post(
    "/sessions/{session_id}/finalize",
    response_model=FinalizeResponse,
    tags=["sessions"],
    summary="提交会话（全部分片完成后）",
    responses={409: {"description": "存在缺失分片，响应体包含 missing_chunks"}},
)
def finalize_session(session_id: str):
    with DB_LOCK:
        session = get_session_or_404(session_id)
        rows = get_conn().execute(
            "SELECT * FROM chunks WHERE session_id = ? ORDER BY chunk_index",
            (session_id,),
        ).fetchall()

        missing = [r["chunk_index"] for r in rows if r["status"] != "completed"]
        if missing:
            return conflict("upload incomplete", missing_chunks=missing)

        now = utcnow()
        finalized_at = parse_ts(session["finalized_at"]) if session["finalized_at"] else None
        if session["status"] != "finalized":
            finalized_at = now
            get_conn().execute(
                "UPDATE sessions SET status = 'finalized', finalized_at = ? WHERE id = ?",
                (finalized_at.isoformat(), session_id),
            )
            get_conn().commit()
        # 已 finalized 时重复提交：幂等返回同一份清单

    return FinalizeResponse(
        session_id=session_id,
        status="finalized",
        file_digest=session["file_digest"],
        digest_algorithm=session["digest_algorithm"],
        total_chunks=session["total_chunks"],
        manifest=[
            ManifestEntry(
                chunk_index=r["chunk_index"],
                chunk_digest=r["chunk_digest"],
                completed_at=parse_ts(r["completed_at"]),
            )
            for r in rows
        ],
        finalized_at=finalized_at,
    )


# ---------------------------------------------------------------- 其他

@app.get("/healthz", tags=["meta"], summary="健康检查")
def healthz():
    get_conn().execute("SELECT 1")
    return {"status": "ok"}


@app.get("/", tags=["meta"], summary="服务信息")
def root():
    return {
        "service": "chunked-upload-coordinator",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "default_lease_ttl_seconds": DEFAULT_LEASE_TTL_SECONDS,
        "max_lease_ttl_seconds": MAX_LEASE_TTL_SECONDS,
    }
