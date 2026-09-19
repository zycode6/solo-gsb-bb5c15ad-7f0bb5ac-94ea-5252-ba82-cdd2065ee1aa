"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, model_validator


def validate_dependencies(
    dependencies: dict[int, list[int]], total_chunks: int
) -> dict[int, list[int]]:
    """校验并规范化依赖图：拒绝越界、自依赖与有环关系；返回去重、升序后的图。"""
    normalized: dict[int, list[int]] = {}
    for chunk_index, deps in dependencies.items():
        if not 0 <= chunk_index < total_chunks:
            raise ValueError(
                f"dependency key {chunk_index} out of range [0, {total_chunks})"
            )
        unique = sorted(set(deps))
        for dep in unique:
            if not 0 <= dep < total_chunks:
                raise ValueError(
                    f"dependency {dep} of chunk {chunk_index} out of range [0, {total_chunks})"
                )
            if dep == chunk_index:
                raise ValueError(f"chunk {chunk_index} cannot depend on itself")
        if unique:
            normalized[chunk_index] = unique

    # 环检测（Kahn 拓扑）：仅键集合内部的子图可能成环，非键节点均为汇点
    indegree = {node: 0 for node in normalized}
    for deps in normalized.values():
        for dep in deps:
            if dep in indegree:
                indegree[dep] += 1
    queue = [node for node, deg in indegree.items() if deg == 0]
    visited = 0
    while queue:
        node = queue.pop()
        visited += 1
        for dep in normalized[node]:
            if dep in indegree:
                indegree[dep] -= 1
                if indegree[dep] == 0:
                    queue.append(dep)
    if visited != len(normalized):
        raise ValueError("dependencies contain a cycle")
    return normalized


class CreateSessionRequest(BaseModel):
    total_chunks: int = Field(ge=1, le=100_000, description="总分片数")
    file_digest: str = Field(min_length=1, max_length=256, description="期望的完整文件摘要")
    digest_algorithm: str = Field(default="sha256", min_length=1, max_length=32,
                                  description="摘要算法标识，仅作记录")
    dependencies: Optional[dict[int, list[int]]] = Field(
        default=None,
        description="分片依赖图：键为分片序号，值为该分片领取租约前必须完成的前置分片序号列表；"
                    "缺省表示无依赖（保持原有调度行为）")
    max_active_leases: Optional[int] = Field(
        default=None, ge=1, le=100_000,
        description="会话级并发租约上限：当前有效（未过期）租约数达到该值后，"
                    "单分片领取返回 409、批量领取只返回空列表；缺省/NULL 表示不限并发")

    @model_validator(mode="after")
    def _validate_dependencies(self):
        if self.dependencies:
            self.dependencies = validate_dependencies(self.dependencies, self.total_chunks)
        return self


class SessionResponse(BaseModel):
    id: str
    total_chunks: int
    file_digest: str
    digest_algorithm: str
    status: str
    max_active_leases: Optional[int] = Field(
        default=None, description="会话级并发租约上限；null 表示不限并发")
    created_at: datetime
    finalized_at: Optional[datetime] = None


class LeaseRequest(BaseModel):
    lease_ttl_seconds: Optional[int] = Field(
        default=None, ge=1,
        description="租约有效期（秒），缺省取服务端默认值，超过服务端上限会被截断")


class LeaseResponse(BaseModel):
    session_id: str
    chunk_index: int
    lease_token: str
    expires_at: datetime


class BatchLeaseRequest(BaseModel):
    count: int = Field(
        ge=1, le=100_000,
        description="期望领取的分片数量上限；实际领取数还受剩余并发槽位与可领取分片数限制")
    lease_ttl_seconds: Optional[int] = Field(
        default=None, ge=1,
        description="本批租约统一有效期（秒），缺省取服务端默认值，超过服务端上限会被截断")


class BatchLeaseItem(BaseModel):
    chunk_index: int
    lease_token: str
    expires_at: datetime


class BatchLeaseResponse(BaseModel):
    session_id: str
    requested: int = Field(description="请求的 count")
    granted: list[BatchLeaseItem] = Field(
        description="本次实际领取的租约，按分片序号升序；无可领取项或无剩余槽位时为空列表")
    active: int = Field(description="本次领取后，会话内当前有效（未过期）租约总数")
    blocked: int = Field(
        description="未完成、且前置依赖尚未全部完成的分片数量（不含已完成、不看租约状态）")
    next_expiry: Optional[datetime] = Field(
        default=None,
        description="本次领取后会话内当前有效租约中最早的过期时间（槽位占满时可据此等待重试）；"
                    "无有效租约时为 null")
    max_active_leases: Optional[int] = Field(
        default=None, description="会话级并发租约上限；null 表示不限并发")
    remaining_slots: Optional[int] = Field(
        default=None, description="当前剩余并发槽位；不限并发时为 null")


class CompleteRequest(BaseModel):
    lease_token: str = Field(min_length=1, max_length=128, description="当前有效租约令牌")
    chunk_digest: str = Field(min_length=1, max_length=256, description="该分片的摘要")


class CompleteResponse(BaseModel):
    session_id: str
    chunk_index: int
    status: str
    chunk_digest: str
    completed_at: datetime
    idempotent_replay: bool = Field(default=False, description="是否为幂等重放")


class ManifestEntry(BaseModel):
    chunk_index: int
    chunk_digest: str
    completed_at: datetime


class FinalizeResponse(BaseModel):
    session_id: str
    status: str
    file_digest: str
    digest_algorithm: str
    total_chunks: int
    manifest: list[ManifestEntry]
    finalized_at: datetime


class ChunkStatus(BaseModel):
    chunk_index: int
    status: str  # pending | leased | completed（过期租约在读时按 pending 展示）
    lease_expires_at: Optional[datetime] = None
    chunk_digest: Optional[str] = None
    completed_at: Optional[datetime] = None
    dependencies: list[int] = Field(default_factory=list,
                                    description="领取租约前必须完成的前置分片（升序）")


class BlockedChunk(BaseModel):
    chunk_index: int
    missing_dependencies: list[int] = Field(description="尚未完成的前置分片（升序）")


class ReadyResponse(BaseModel):
    session_id: str
    ready_chunks: list[int] = Field(
        description="当前可领取的分片：未完成、无有效租约（过期租约视为可领取）且前置已全部完成")
    blocked_chunks: list[BlockedChunk] = Field(
        description="被依赖阻塞的分片及各自未完成的前置项")


class SessionStatusResponse(BaseModel):
    id: str
    status: str
    total_chunks: int
    completed_chunks: int
    missing_chunks: list[int]
    file_digest: str
    digest_algorithm: str
    max_active_leases: Optional[int] = Field(
        default=None, description="会话级并发租约上限；null 表示不限并发")
    active_leases: int = Field(
        default=0, description="当前有效（未过期）租约数，过期租约不计入")
    created_at: datetime
    finalized_at: Optional[datetime] = None
    chunks: list[ChunkStatus]
