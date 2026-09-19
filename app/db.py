"""SQLite 存储层：单连接 + 进程内写锁，启动时自动建库建表。"""
from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

DB_PATH = os.environ.get("UPLOAD_DB_PATH", "./data/coordinator.db")

# 进程内写锁：SQLite 单写者，串行化所有状态变更，保证租约/完成/提交的原子性。
DB_LOCK = threading.RLock()

_conn: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id                TEXT PRIMARY KEY,
    total_chunks      INTEGER NOT NULL,
    file_digest       TEXT NOT NULL,
    digest_algorithm  TEXT NOT NULL DEFAULT 'sha256',
    status            TEXT NOT NULL DEFAULT 'active',   -- active | finalized
    max_active_leases INTEGER,                          -- 会话级并发租约上限；NULL 表示不限
    base_retry_seconds INTEGER NOT NULL DEFAULT 5,      -- 失败退避基数（秒）
    max_retry_seconds  INTEGER NOT NULL DEFAULT 300,    -- 失败退避上限（秒）
    created_at        TEXT NOT NULL,
    finalized_at      TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    session_id        TEXT NOT NULL REFERENCES sessions(id),
    chunk_index       INTEGER NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',  -- pending | leased | completed
    lease_token       TEXT,
    lease_expires_at  TEXT,
    chunk_digest      TEXT,
    completed_at      TEXT,
    failure_count     INTEGER NOT NULL DEFAULT 0,       -- 累计失败上报次数（完成后仍保留）
    retry_after       TEXT,                             -- 退避冷却结束时刻；NULL 表示未冷却
    PRIMARY KEY (session_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_chunks_session ON chunks(session_id);

-- 分片依赖图：chunk_index 领取租约前，depends_on 必须已完成
CREATE TABLE IF NOT EXISTS chunk_dependencies (
    session_id        TEXT NOT NULL,
    chunk_index       INTEGER NOT NULL,
    depends_on        INTEGER NOT NULL,
    PRIMARY KEY (session_id, chunk_index, depends_on),
    FOREIGN KEY (session_id, chunk_index) REFERENCES chunks(session_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_chunk_deps_session ON chunk_dependencies(session_id);
"""

# 增量迁移：针对早于当前 SCHEMA 创建的旧库，幂等执行（CREATE TABLE IF NOT EXISTS
# 不会为已存在的表补列，故需显式 ALTER）。
MIGRATIONS = [
    # 0001: 会话级并发租约上限；旧库已存在的会话取 NULL，即保持“不限并发”的旧行为
    (
        "sessions.max_active_leases",
        "ALTER TABLE sessions ADD COLUMN max_active_leases INTEGER",
    ),
    # 0002: 失败退避参数；旧会话回填默认值 5/300，行为等价于新建库的默认退避策略
    (
        "sessions.base_retry_seconds",
        "ALTER TABLE sessions ADD COLUMN base_retry_seconds INTEGER NOT NULL DEFAULT 5",
    ),
    (
        "sessions.max_retry_seconds",
        "ALTER TABLE sessions ADD COLUMN max_retry_seconds INTEGER NOT NULL DEFAULT 300",
    ),
    # 0003: 分片失败计数与冷却结束时刻；旧分片计数为 0、无冷却
    (
        "chunks.failure_count",
        "ALTER TABLE chunks ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0",
    ),
    (
        "chunks.retry_after",
        "ALTER TABLE chunks ADD COLUMN retry_after TEXT",
    ),
]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)；
    # 使用位置下标以兼容未设置 row_factory 的连接。
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def run_migrations(conn: sqlite3.Connection) -> None:
    with DB_LOCK:
        for column, ddl in MIGRATIONS:
            table, _, column_name = column.partition(".")
            if column_name not in _table_columns(conn, table):
                conn.execute(ddl)
        conn.commit()


def get_conn() -> sqlite3.Connection:
    """返回进程级共享连接（惰性创建，首次调用时建库建表）。"""
    global _conn
    if _conn is None:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
        _conn.execute("PRAGMA busy_timeout=5000")
        with DB_LOCK:
            _conn.executescript(SCHEMA)
            _conn.commit()
        run_migrations(_conn)  # 补齐旧库缺失的列
    return _conn


def init_db() -> None:
    """启动时显式初始化数据库。"""
    get_conn()
