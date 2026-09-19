# 断点分片上传协调 API（Chunked Upload Coordinator）

只协调**元数据**的上传调度服务：创建会话、分片租约、完成上报、提交汇总与状态查询。
**不接收、不保存任何文件内容**，文件字节流由客户端自行点对点传输或写入对象存储。

技术栈：Python 3.12 · FastAPI · SQLite（WAL）

## 快速启动（单条命令）

```bash
docker compose up --build
```

启动后访问：

| 地址 | 说明 |
|---|---|
| `http://localhost:8000` | 服务信息 |
| `http://localhost:8000/docs` | Swagger 交互式 API 文档 |
| `http://localhost:8000/openapi.json` | OpenAPI 描述 |
| `http://localhost:8000/healthz` | 健康检查 |

依赖安装已纳入镜像构建，SQLite 建库建表在应用启动时自动完成，无需额外初始化步骤。
数据通过命名卷 `coordinator-data` 持久化，删除卷即清空全部状态。

## 配置（环境变量）

均可在 `docker-compose.yml` 的 `environment` 中调整：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `UPLOAD_DB_PATH` | `/data/coordinator.db` | SQLite 数据库文件路径 |
| `DEFAULT_LEASE_TTL_SECONDS` | `300` | 租约默认有效期（秒） |
| `MAX_LEASE_TTL_SECONDS` | `3600` | 租约有效期上限（秒），超出会被截断 |

端口映射默认为宿主机 `8000` → 容器 `8000`，可在 `docker-compose.yml` 的 `ports` 中修改。

## 核心语义

- **会话**：创建时登记 `total_chunks`（总分片数）与 `file_digest`（期望的完整文件摘要），分片序号从 `0` 开始。
- **依赖（可选）**：创建会话时可提交 `dependencies`（键与值均为分片序号），声明某分片领取租约前必须完成的前置分片。越界、自依赖、有环的依赖图会被拒绝（`422`）；不提供时保持无依赖的原有行为。
- **租约**：同一分片同一时刻仅允许一个有效租约；租约过期后可被重新领取（令牌轮换）。前置分片未全部完成时领取返回 `409` 及按序缺失的 `missing_dependencies`；前置全部完成后无需额外操作即可领取。
- **会话级并发上限（可选）**：创建会话时可设置 `max_active_leases`，限制该会话**当前有效（未过期）租约**的总数。单片领取在槽位占满时返回 `409`；批量领取则原子地领取至多 `min(count, 剩余槽位, 可领取分片数)` 个租约。不设置（`null`）时保持原有不限并发的行为。
- **批量领取**：`POST /sessions/{id}/leases/batch` 在**同一临界区**内完成资格计算与写入：仅从“未完成、无有效租约（过期租约视为空闲）、前置已全部完成”的分片中，按分片序号升序领取，返回各分片独立令牌；并发请求之间不会重复分配、也不会突破并发上限。无可领取项或无剩余槽位时返回空列表及 `active` / `blocked` / `next_expiry` 汇总。
- **完成上报**：必须携带该分片**当前有效租约的令牌**与分片摘要；以相同令牌 + 相同摘要重复上报为幂等成功（返回 `idempotent_replay: true`），摘要不同则返回 `409`。
- **提交**：仅当全部分片完成才允许提交，返回按分片序号升序汇总的清单；否则返回 `409` 及 `missing_chunks` 缺失分片列表。重复提交幂等返回同一清单。
- 已完成的分片不可再领取租约；已提交的会话进入只读状态。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/sessions` | 创建上传会话（可选 `dependencies` 依赖图、`max_active_leases` 并发上限） |
| `GET` | `/sessions/{session_id}` | 查询会话与逐分片状态（含各分片 `dependencies`、`max_active_leases` 与当前 `active_leases`） |
| `GET` | `/sessions/{session_id}/ready` | 当前可领取的分片，及被依赖阻塞的分片与各自未完成前置项 |
| `POST` | `/sessions/{session_id}/chunks/{index}/lease` | 领取分片租约（同样受 `max_active_leases` 约束） |
| `POST` | `/sessions/{session_id}/leases/batch` | 原子批量领取租约（按分片序号，数量不超过 `count` 与剩余并发槽位） |
| `POST` | `/sessions/{session_id}/chunks/{index}/complete` | 上报分片完成（幂等） |
| `POST` | `/sessions/{session_id}/finalize` | 提交会话，返回按序分片清单 |
| `GET` | `/healthz` | 健康检查 |

## 批量领取与并发上限（curl）

```bash
# 1. 创建会话：8 个分片，同一时刻最多 3 个有效租约
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 8, "file_digest": "<文件SHA256>", "max_active_leases": 3}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 一次申请 5 个、TTL 120 秒：实际只发 3 个（受剩余并发槽位限制），按分片序号升序返回
curl -s -X POST http://localhost:8000/sessions/$SID/leases/batch \
  -H 'Content-Type: application/json' -d '{"count": 5, "lease_ttl_seconds": 120}'
# => {
#      "requested": 5,
#      "granted": [
#        {"chunk_index": 0, "lease_token": "...", "expires_at": "..."},
#        {"chunk_index": 1, "lease_token": "...", "expires_at": "..."},
#        {"chunk_index": 2, "lease_token": "...", "expires_at": "..."}
#      ],
#      "active": 3, "blocked": 0, "next_expiry": "2026-...Z",
#      "max_active_leases": 3, "remaining_slots": 0
#    }

# 3. 槽位占满时再申请：返回空列表与汇总，可按 next_expiry 等待重试
curl -s -X POST http://localhost:8000/sessions/$SID/leases/batch \
  -H 'Content-Type: application/json' -d '{"count": 5}'
# => {"requested": 5, "granted": [], "active": 3, "blocked": 0,
#     "next_expiry": "2026-...Z", "max_active_leases": 3, "remaining_slots": 0}

# 4. 用令牌上报完成后，槽位与后续分片同时释放，再批量领取即可拿到新的分片
#    （单片接口 POST .../chunks/{index}/lease 同样占用槽位，占满时返回 409）

# 5. 状态查询暴露并发上限与当前有效租约数（过期租约不计入）
curl -s http://localhost:8000/sessions/$SID
# => {"max_active_leases": 3, "active_leases": 3, ...}
```

依赖图与批量领取可以组合使用：被前置依赖阻塞的分片不会进入 `granted`，
其数量汇总在响应的 `blocked` 中；前置分片陆续完成后会自动具备领取资格。
不设置 `max_active_leases` 的会话中，`max_active_leases` 与 `remaining_slots` 均为 `null`，
批量领取只受 `count` 与可领取分片数约束。

## 调用示例（curl）

```bash
# 1. 创建会话：4 个分片，登记期望的文件 SHA-256
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "digest_algorithm": "sha256"}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 领取分片 0 的租约（有效期 120 秒）
LEASE=$(curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  -H 'Content-Type: application/json' -d '{"lease_ttl_seconds": 120}')
TOKEN=$(echo "$LEASE" | python3 -c 'import sys, json; print(json.load(sys.stdin)["lease_token"])')

# 3. 客户端自行传输该分片后，上报完成（携带租约令牌与分片摘要）
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"<分片0的SHA256>\"}"

# 4. 对其余分片重复 2-3 步，然后提交会话（缺分片时返回 409 + missing_chunks）
curl -s -X POST http://localhost:8000/sessions/$SID/finalize

# 5. 随时查询状态
curl -s http://localhost:8000/sessions/$SID
```

## 依赖调度示例（curl）

```bash
# 1. 创建带依赖的会话：分片 2 依赖 0 和 1，分片 3 依赖 2（键/值均为分片序号）
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "<文件SHA256>", "digest_algorithm": "sha256",
       "dependencies": {"2": [0, 1], "3": [2]}}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 查看当前可领取与被阻塞的分片
#    => ready_chunks: [0, 1]
#    => blocked_chunks: [{chunk_index: 2, missing_dependencies: [0, 1]}, {chunk_index: 3, ...}]
curl -s http://localhost:8000/sessions/$SID/ready

# 3. 前置未完成时领取分片 2：返回 409 及按序缺失的前置分片
#    => {"detail": "chunk dependencies not completed", "missing_dependencies": [0, 1]}
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/2/lease

# 4. 完成分片 0、1 后，分片 2 自动变为可领取，无需额外操作
TOKEN=$(curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["lease_token"])')
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"<分片0的SHA256>\"}"
# ... 同样完成分片 1，之后即可正常领取分片 2

# 5. 状态查询中每个分片都带有 dependencies 字段
curl -s http://localhost:8000/sessions/$SID
```

## 本地开发（不用 Docker）

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

运行测试（需额外安装 `pytest`、`httpx`）：

```bash
python3 -m pytest tests/ -q
```

## 项目结构

```
app/
  main.py      # FastAPI 应用与全部端点
  db.py        # SQLite 连接与建库建表（启动时自动执行）
  schemas.py   # Pydantic 请求/响应模型
tests/
  test_api.py  # 租约独占/过期、幂等、提交等端到端行为测试
Dockerfile
docker-compose.yml
requirements.txt
```
