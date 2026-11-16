# 测序中心归档回收 API（Archive Reclamation Service）

按批次号管理测序 ZIP 归档的 FastAPI + SQLite 服务。两个实例共享同一数据卷，
内容按 SHA-256 去重，批次撤下后只回收**已无任何有效批次引用**的原件，并支持
进程崩溃后重启续清。新增**容量准入**：启动时配置原件字节上限，已占用原件与
在途预留合并记账、按 SHA-256 只计一份，超额只拒绝新增内容，不影响下载、
清退与已有内容复用。

## 功能与接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/batches/{batch_id}/archive` | 上传 ZIP（≤ 2 MiB），multipart 字段 `file`；容量不足返回 **507** |
| GET  | `/batches/{batch_id}/archive` | 下载该批次的原始字节 |
| POST | `/batches/{batch_id}/retire` | 撤下批次（幂等），随后内联执行回收 |
| POST | `/gc/run` | 手动触发标记-清扫式垃圾回收 |
| GET  | `/batches?status=active|retired` | 批次摘要列表 |
| GET  | `/batches/{batch_id}` | 单个批次摘要 |
| GET  | `/storage/summary` | 台账核对 + 容量摘要（上限/已占用/预留/可用） |
| GET  | `/healthz` | 健康检查 |
| GET  | `/docs` | Swagger UI（OpenAPI 文档，可直接演示上传、清退与容量查询） |

### 业务规则

- 上传仅接受合法 ZIP，单批次上限 **2 MiB**（超限返回 413，非 ZIP 返回 400）。
- 相同 SHA-256 内容在共享卷上**只存一份**（`blobs/<sha256>`），多批次共用。
- 同号同内容重传：返回 200 且 `reused_existing_content=true`，**不重复扣额**。
- 同号异内容：拒绝并返回 **409**，原内容不受影响。
- 撤下的批次号**永久不可复用**：再传返回 409，新下载返回 **404**。
- 回收只删除无 active 批次引用的原件；仍被其他批次共用的原件保留。
- 下载一旦开始即持有独立文件描述符流式传输，**不持有数据库事务/锁**；
  即使期间原件被移入隔离区或删除（POSIX 语义），慢速下载仍可完整结束，
  且不会阻塞其他批次的撤下与回收。

## 容量准入

### 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `ARCHIVE_CAPACITY_BYTES` | 未设置（不限额） | 原件字节上限，非负整数；两个实例必须配置相同值 |
| `ARCHIVE_HEARTBEAT_SECONDS` | `2` | 本实例心跳刷新间隔 |
| `ARCHIVE_NODE_TTL_SECONDS` | `30` | 节点心跳过期时间，超过即判定为死亡、其预留成为无主预留 |

### 记账口径

- **已占用 `occupied_bytes`**：`blobs` 表全部行的字节之和，**包含 `deleting`
  状态**。撤下受理不会立即返额；只有原件被物理删除且台账行删除后才释放。
  物理删除失败时行保留在 `deleting`、额度继续占用，下次 GC 成功后才释放。
- **在途预留 `reserved_bytes`**：已通过准入、但 blob 行尚未出现的上传预留。
  预留按 SHA-256 主键去重，相同内容并发上传**只预留一份、只扣一次额**。
- **已占用 + 在途预留按 SHA-256 取并集**：同一哈希即使同时存在 blob 行与
  预留行也只计一份。
- **上传暂存不计额**：`tmp/` 分片在准入判定前不占容量。
- **可用 `available_bytes`**：未配置上限时为 `null`；否则为
  `max(0, 上限 - 已占用 - 预留)`。

### 准入与拒绝

- 新内容在上传的 `BEGIN IMMEDIATE` 事务内判定：`已占用并集 + 本次字节 > 上限`
  即拒绝，返回 **507 Insufficient Storage**，响应体给出上限、已提交、本次
  申请与可用字节；事务回滚，**不创建批次行、不占用批号、不留预留**。
- 双实例并发接收**不同内容**时，SQLite 写锁把准入判定串行化，先到者占额、
  后到者在余额不足时收到 507，两个实例合计**不会超额**。
- 并发接收**相同内容**时，第二个上传命中已有 blob 行/预留，不额外扣额。
- 超额状态下仍可：下载原件、撤下批次并触发回收、复用已有内容；仅拒绝
  *新增*内容。清退真正释放额度后，先前被拒的批号可重新使用。

### 崩溃与重启

- 上传失败或进程退出后，重启 `reconcile()` 释放**无主预留**，判定条件：
  1. 该哈希已被 blob 行覆盖（上传已成功，预留转为正式占用，不重复计费）；
  2. 所属节点心跳缺失或已过 TTL（节点死亡）；
  3. 节点仍存活但其 `tmp/` 分片已消失（上传在提交前失败）。
- **不会释放**另一活上传的预留：所属节点心跳新鲜且分片仍在时，预留与其
  分片都受保护；已成功共享的原件由 blob 行继续计费，也不受影响。
- 同一 `INSTANCE_NAME` 的容器重启时，旧化身留下的预留按无主处理（新进程
  尚无活上传）。

### 非串行保证

- 上传的慢速写盘发生在取数据库写锁之前的 `tmp/` 暂存阶段，不持有任何锁；
  单件写盘停顿不挡其他实例/批次的接收、查询与下载。
- GC 的物理删除在数据库事务之外进行，单件清理停顿不挡无关内容的接收、
  查询和下载；下载全程不持数据库锁。系统不存在全局串行点。

`/storage/summary` 容量字段示例：

```json
{
  "capacity_limit_bytes": 5242880,
  "occupied_bytes": 1048576,
  "reserved_bytes": 131072,
  "available_bytes": 4063232
}
```

## 并发与崩溃安全设计

- SQLite 启用 **WAL** 与 `busy_timeout`；所有写操作在单个
  `BEGIN IMMEDIATE` 事务内完成判定与写入，两个实例靠写锁串行化，
  不会互相读到半成品状态。
- 上传顺序：先写 `tmp/` 临时分片 → 事务内做容量准入、插入/认领 blob 行与
  批次行并提交 → 再原子 `rename` 到 `blobs/`，随后释放本哈希的在途预留。
  行先于文件存在，回收扫描不会误判新上传。
- 回收分两阶段：
  1. **标记**：把无 active 引用的 ready blob 写入 `gc_queue`；
  2. **清扫**：事务内复查引用后置 `deleting`，文件移入 `quarantine/`，
     物理删除成功后才在第二个事务内删行；删除失败则保留 `deleting` 行与
     `gc_queue` 项，额度不释放，后续清扫重试。每一步都幂等。
- 若清扫中内容被新批次重新引用（同哈希上传），事务内复查会把行恢复为
  `ready` 并把隔离文件移回，内容得以保留。
- 进程在任何阶段退出后，重启时 lifespan 调用 `reconcile()`：
  释放无主容量预留、清理临时分片（跳过其他活上传的分片）、续清 `deleting`
  残留、重新被引用的内容恢复、清理无主隔离文件，然后继续常规标记-清扫。
- `gc_queue`、`deleting` 状态、容量预留与节点心跳均持久化在 SQLite 中，
  跨重启可续。

## 数据卷布局

```
/data
├── archives.db          # SQLite（WAL 模式，含 -wal/-shm 旁车文件）
├── blobs/<sha256>       # 内容寻址的原件，每个哈希一份
├── quarantine/<sha256>  # 回收中的原件（崩溃可恢复）
└── tmp/part-<uuid>      # 上传临时分片（不计入容量）
```

`/storage/summary` 同时返回 `blob_rows`（台账行数）与
`blob_files_on_disk`（实际原件数），两者一致即说明回收已收敛；
容量四元组 `capacity_limit_bytes / occupied_bytes / reserved_bytes /
available_bytes` 反映当前准入状态。

## 启动

### Docker Compose（推荐，两个实例 + 共享命名卷）

```bash
docker compose up --build
# 实例 A: http://localhost:8001/docs
# 实例 B: http://localhost:8002/docs
# 共享卷: archive-data -> /data
# 容量上限: 由 ARCHIVE_CAPACITY_BYTES 控制（compose 默认 5 MiB）
```

两个服务挂载同一个命名卷并声明相同的 `ARCHIVE_CAPACITY_BYTES`，
`INSTANCE_NAME` 分别为 `api-a` / `api-b` 用于心跳标识。

### 本地直接运行

```bash
pip install -r requirements.txt
ARCHIVE_DATA_DIR=./data ARCHIVE_CAPACITY_BYTES=5242880 \
  uvicorn app.api:app --port 8000
# 第二个实例换端口、同一 ARCHIVE_DATA_DIR 与相同上限即可
```

### 快速冒烟

```bash
curl -F "file=@batch.zip" http://localhost:8001/batches/B2026-001/archive
curl http://localhost:8002/batches/B2026-001/archive -o roundtrip.zip
curl -X POST http://localhost:8001/batches/B2026-001/retire
curl http://localhost:8001/storage/summary
```

## 测试

```bash
pip install -r requirements.txt pytest httpx
python -m pytest -q
```

测试覆盖：

- 上传/下载往返、2 MiB 限制、ZIP 校验、去重与单原件存储；
- 同号同内容复用、同号异内容 409、撤下号不可复用、撤下后 404；
- 共用原件只在最后一个引用批次撤下后才回收；
- **容量准入**：上限内记账、超额 507 且不占批号、同内容去重只扣一次、
  超额仍可下载/复用/清退、撤下物理删除后才返额、删除失败持续占额、
  失败上传不留预留、重启释放无主预留但保护另一活上传与已共享原件、
  容量摘要四元组、blob 行与预留同哈希只计一份；
- **两个独立 OS 进程**共享同一数据目录的并发：相同/不同内容并发上传的
  扣额正确性与不超额、上传/撤下/回收/下载混合压力、慢速写盘与慢速清理
  不阻塞无关工作、第三实例重启时的无主预留回收与活上传保护；
- 慢速下载不阻塞其他批次撤下，且开始的下载可完整结束；
- 回收两阶段中 `os._exit` 硬崩溃后的重启续清，以及崩溃后内容被
  重新引用时的恢复；临时分片与隔离区孤儿清理。
