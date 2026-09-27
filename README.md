# Webhook 事件接收与重放 API

基于 FastAPI + SQLite 的 Webhook 事件接收服务:按 `event_id` 幂等接收(支持单条与 1~1000 条批量、同一写事务续接哈希链)、记录首次接收顺序,支持时间范围分页查询与批量重放。

## 启动

```bash
docker compose up --build
```

一条命令完成构建、数据库初始化与启动;服务监听 **8000** 端口,接口入口为 **http://localhost:8000**(交互式文档在 http://localhost:8000/docs,健康检查 http://localhost:8000/health)。SQLite 数据持久化在 named volume `webhook-data`,数据库表(含隔离记录表、快照导出表)在启动时自动创建/升级,无需手工初始化。

不用 compose 也可以:

```bash
docker build -t webhook-replay . && docker run -p 8000:8000 webhook-replay
```

本地开发:`pip install -r requirements.txt && uvicorn app.main:app --port 8000`

**配置**:仅一个环境变量 `DB_PATH`——SQLite 数据库文件路径,默认 `events.db`;compose 镜像内置为 `/data/events.db`(挂载在 `webhook-data` 卷)。

## 示例 curl

### 1. 接收事件(幂等)

```bash
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-1","occurred_at":"2026-09-18T09:00:00Z","payload":{"a":1}}'
# 首次: 201 {"status":"created","event_id":"evt-1"}
# 重复: 200 {"status":"duplicate","event_id":"evt-1"}  —— 不产生重复记录
```

### 1b. 批量接收事件(整批一个写事务)

`POST /events/batch` 一次接收 **1~1000 条**与单条接口同格式的事件,请求体为 JSON 数组(不是包一层对象)。整批在**同一个写事务**(`BEGIN IMMEDIATE`)内处理,按请求顺序逐条判定 `event_id`:

- **库中已有**、或**本批前面已出现过**的 ID 一律标记 `duplicate`,不写入、也不消耗 `seq`;
- 仅首次出现的 ID 入库,并从当前链头按请求顺序**逐条续接哈希链**,`seq` 连续无空洞;
- 响应 `results` 严格按**输入顺序**返回每条的 `event_id`/`status`/`seq`(`duplicate` 的 `seq` 为 `null`),并汇总 `created`/`duplicate`;
- 只要新建了至少一条就返回 **201**,全部重复为 **200**;
- 任一条格式非法(字段缺失、`event_id` 为空、时间非法等)在事务开始前即被整体拒绝(**422**);写入过程中失败则事务回滚(**409**)——两种情况都**不会留下部分新事件**;
- 与单条接口共享同一套幂等语义和哈希链:批量里的重复投递与单条重发效果完全一致。

```bash
curl -X POST http://localhost:8000/events/batch \
  -H 'Content-Type: application/json' \
  -d '[
    {"event_id":"evt-1","occurred_at":"2026-09-18T09:00:00Z","payload":{"a":1}},
    {"event_id":"evt-2","occurred_at":"2026-09-18T09:01:00Z","payload":{"a":2}},
    {"event_id":"evt-1","occurred_at":"2026-09-18T09:02:00Z","payload":{"a":3}}
  ]'
```

`evt-1` 已在上面的单条示例中创建,因此响应为(注意输入顺序被完整保留,批内第二次出现的 `evt-1` 同样判重):

```json
{
  "created": 1,
  "duplicate": 2,
  "results": [
    {"event_id": "evt-1", "status": "duplicate", "seq": null},
    {"event_id": "evt-2", "status": "created", "seq": 2},
    {"event_id": "evt-1", "status": "duplicate", "seq": null}
  ]
}
```

空数组(`[]`)或超过 1000 条同样返回 **422**;批量提交与单条提交可以任意交错,哈希链始终连续,随时可用 `/integrity` 校验。

### 2. 按时间范围分页查询

```bash
curl 'http://localhost:8000/events?from=2026-09-18T09:30:00Z&to=2026-09-18T11:30:00Z&page=1&page_size=50'
```

返回 `total / page / page_size / items`,每条含 `seq`(首次接收顺序号)、`event_id`、`occurred_at`、`payload`、`received_at`。`from`/`to` 均可省略。

### 3. 批量重放

```bash
curl -X POST http://localhost:8000/replay \
  -H 'Content-Type: application/json' \
  -d '{"event_ids":["evt-3","evt-1","evt-404"]}'
```

```json
{
  "replayed": 2,
  "skipped": 1,
  "results": [
    {"event_id": "evt-1", "status": "replayed", "event": {"seq": 1, "...": "..."}},
    {"event_id": "evt-3", "status": "replayed", "event": {"seq": 2, "...": "..."}},
    {"event_id": "evt-404", "status": "skipped", "reason": "not_found"}
  ]
}
```

命中的事件**按首次接收顺序(`seq`)输出**,与请求顺序无关;未命中的标记 `skipped`;请求中重复的 `event_id` 只输出一次。

### 4. 完整性校验

```bash
curl http://localhost:8000/integrity
```

```json
{"verified": true, "checked": 3, "first_break": null, "anchor_seq": 0}
```

从**链锚点**开始逐条校验哈希链:**序号连续性**(相邻 `seq` 逐一递增,首条须为 `anchor_seq + 1`)、**前后哈希衔接**(每条 `prev_hash` 等于上一条的 `hash`,首条等于锚点哈希——未压缩过时为创世空串)、**事件字段**(按固定 JSON 规范重算哈希比对)。空库返回 `verified=true`、`checked=0`;全部事件被压缩归档后同样返回 `verified=true`、`checked=0`(锚点本身即完整性证明)。

发现篡改时返回首个断点的 `seq`、`event_id` 和原因(`seq_gap` / `prev_hash_mismatch` / `hash_mismatch`),`checked` 为断点前已通过校验的记录数:

```json
{"verified": false, "checked": 1, "first_break": {"seq": 2, "event_id": "evt-2", "reason": "hash_mismatch"}}
```

### 5. 消费游标:拉取并租用事件

每个 consumer 独立维护确认游标,按首次接收顺序(`seq` 从 1 开始)拉取尚未确认的连续事件。`limit`(默认 100,1~1000)与 `lease_seconds`(默认 30,1~86400)均可省略,甚至可以不带 body:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/pull \
  -H 'Content-Type: application/json' \
  -d '{"limit":100,"lease_seconds":30}'
```

```json
{
  "consumer_id": "worker-1",
  "events": [
    {"seq": 1, "event_id": "evt-1", "occurred_at": "...", "payload": {"a": 1}, "received_at": "..."},
    {"seq": 2, "event_id": "evt-2", "occurred_at": "...", "payload": {"a": 2}, "received_at": "..."}
  ],
  "lease_token": "NcueAr-DjIE5Cjwdn0X5OiSxe9CH_XyCkya2G0vp3sk",
  "lease_expires_at": "2026-09-18T19:38:12.919089+00:00"
}
```

- **租约期内重复拉取**:原样返回**同一批次、同一 token、同一过期时间**(不续租),参数里的 `limit`/`lease_seconds` 不影响结果。
- **租约过期后**:同一批次可被重新投递(沿用原区间,换发**新 token**);旧 token 立即作废。
- **空队列**:返回 `{"events":[],"lease_token":null,"lease_expires_at":null}`,**不创建租约**;之后新事件到达可继续拉取。
- 首次出现的 `consumer_id` 自动建档,游标为 0,即首批从 `seq=1` 开始。

### 6. 消费游标:确认批次

只能确认**当前未过期租约**的整批事件,确认后游标推进到本批次末尾,下次 `pull` 从下一个 `seq` 开始:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/ack \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"NcueAr-DjIE5Cjwdn0X5OiSxe9CH_XyCkya2G0vp3sk"}'
```

```json
{"consumer_id": "worker-1", "acknowledged": true, "cursor_seq": 2}
```

以下情况一律返回 **409**(游标保持不变,等待租约过期后重新投递):

- token 与当前租约不一致(**伪造**或**旧 token**——旧批次过期后已被新租约取代);
- 租约**已过期**(过期批次可重投,旧 token 不得再确认);
- 当前没有租约(从未拉取、已确认完毕、或上次拉取是空批次),包括未知 consumer。

游标只能经由确认前移(整批 `ack`,或前缀 `ack-prefix`),因此**禁止越序确认**:批次内的任何一条都无法被单独跳过或提前确认。需要处理到一半先确认一部分时,见下一节的前缀确认。

一个典型的拉取-确认循环:

```bash
# 1) 租用
BATCH=$(curl -s -X POST http://localhost:8000/consumers/worker-1/pull \
  -H 'Content-Type: application/json' -d '{"limit":100,"lease_seconds":30}')
TOKEN=$(printf '%s' "$BATCH" | python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_token"])')
# 2) 处理 $BATCH ...
# 3) 确认
curl -X POST http://localhost:8000/consumers/worker-1/ack \
  -H 'Content-Type: application/json' -d "{\"lease_token\":\"$TOKEN\"}"
```

### 7. 消费游标:确认连续前缀

处理到批次中途、希望先确认已完成的部分时,可在**当前未过期租约**内确认从批次起点 `lease_from_seq` 到 `through_seq` 的连续前缀:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/ack-prefix \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"NcueAr-DjIE5Cjwdn0X5OiSxe9CH_XyCkya2G0vp3sk","through_seq":2}'
```

```json
{"consumer_id": "worker-1", "acknowledged": true, "through_seq": 2, "cursor_seq": 2}
```

确认在**同一个写事务**内完成:校验通过后游标推进到 `through_seq` 并**清除原租约**,旧 token 立即作废。下一次 `pull` 会从 `through_seq + 1` 起从未确认的首条**重新租用**(换发新 token);前缀确认的 `[lease_from_seq, through_seq]` 不会再次投递。

- `through_seq` 必须落在当前租约区间 `[lease_from_seq, lease_to_seq]` 内;因为前缀必须从批次起点连续,**无法跳过中间事件**单独确认后面的某一条。
- `through_seq` 等于批次末尾 `lease_to_seq` 时**等同整批确认**,效果与 `ack` 完全一致。
- 确认后再用同一 token 调 `ack` 或 `ack-prefix` 都会得到 **409**(租约已清除);需要继续确认剩余事件,请先重新 `pull`。

以下情况一律返回 **409 且状态不变**(游标与租约均不改动):

- **未知 consumer**,或当前没有租约(从未拉取、已确认完毕、或上次拉取是空批次);
- token 与当前租约不一致(**伪造**或**旧 token**);
- 租约**已过期**(过期批次可重投,旧 token 不得再确认);
- `through_seq` **越界**:早于批次起点(响应 `through_seq before batch start`)或晚于批次末尾(`through_seq beyond batch end`),响应中同时给出当前租约的 `lease_from_seq`/`lease_to_seq`。

```json
{"detail": "through_seq beyond batch end", "through_seq": 9,
 "lease_from_seq": 1, "lease_to_seq": 2}
```

典型用法:租一大批、边处理边确认前缀,租约快到期时已确认的部分不会在重投中重复出现:

```bash
# 1) 租用一批, 处理完前 50 条
# 2) 确认连续前缀
curl -X POST http://localhost:8000/consumers/worker-1/ack-prefix \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\":\"$TOKEN\",\"through_seq\":50}"
# 3) 再次 pull: 从 seq=51 开始租用未确认事件
curl -X POST http://localhost:8000/consumers/worker-1/pull \
  -H 'Content-Type: application/json' -d '{"limit":100,"lease_seconds":30}'
```

### 8. 消费游标:回退重投

处理逻辑出错或需要重放历史时,可把**已登记** consumer 的游标回退到 `target_seq`,下一次 `pull` 从 `target_seq + 1` 重新投递:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/rewind \
  -H 'Content-Type: application/json' \
  -d '{"target_seq":42}'
```

```json
{"consumer_id": "worker-1", "previous_cursor_seq": 100, "cursor_seq": 42, "next_seq": 43}
```

- **回退区间**:`target_seq` 必须落在 `[anchor_seq, cursor_seq]`(链锚点 ~ 当前游标)。低于锚点(对应事件已压缩归档、无法重投)或大于当前游标(**向前推进只能经由 `ack` 整批确认或 `ack-prefix` 前缀确认**)一律返回 **409** 且状态不变;`target_seq` 等于当前游标时为幂等空操作。
- **租约**:存在**未过期租约**时返回 **409**(回退会与在途批次冲突);**已过期租约**在同一事务内清除后再回退,旧 token 随之作废,后续 `pull` 不会重投旧批次区间。
- **未知 consumer** 返回 **409**,不会像 `pull` 那样自动建档。
- **并发安全**:回退与 `pull`/`ack`/`ack-prefix`/`compact` 在同一写锁下串行执行,并发下只有单一结果;回退后游标不低于链锚点,`compact` 的安全边界也随之收紧,绝不会产生指向已压缩事件的租约。

```json
{"detail": "target_seq below chain anchor", "target_seq": 3, "anchor_seq": 4}
```

### 9. 消费游标:隔离毒丸事件

批次首条事件反复处理失败(毒丸)时,可只隔离这一条、越过它继续消费剩余事件,而不必回退或卡住整个批次:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/quarantine \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"NcueAr-DjIE5Cjwdn0X5OiSxe9CH_XyCkya2G0vp3sk","reason":"JSON 解析失败: 缺少必填字段 order_id"}'
```

```json
{
  "consumer_id": "worker-1",
  "quarantined": true,
  "quarantine_id": 1,
  "seq": 1,
  "event_id": "evt-1",
  "cursor_seq": 1,
  "quarantined_at": "2026-09-26T12:00:00.000000+00:00"
}
```

全部操作在**单个写事务**(`BEGIN IMMEDIATE`)内完成:

1. **核对令牌**:必须存在与 `lease_token` 一致且**未过期**的租约;无租约、租约过期、伪造或旧令牌一律 **409 且状态不变**(与 `ack` 同一套校验)。
2. **核对租约起点与游标相邻**:`lease_from_seq` 必须等于下一条可投递事件(正常情况下为 `cursor_seq + 1`;压缩归档后才建档的 consumer 游标仍为 0,已归档区间不可投递,起点为 `anchor_seq + 1`)。不相邻返回 409 `lease start not adjacent to cursor`(响应附 `cursor_seq`/`anchor_seq`/`lease_from_seq`/`lease_to_seq`),状态不变。
3. **复制隔离记录**:把首条事件的**完整内容**(`seq`/`event_id`/`occurred_at`/`payload`/`received_at` 及原事件 `hash`)连同非空 `reason`、隔离时间、所用令牌写入**独立隔离表** `quarantined_events`(不修改、不删除原事件)。
4. **游标仅越过这一条**:`cursor_seq` 推进到 `lease_from_seq`,同时**清除整批租约**。下一次 `pull` 从该 seq 之后**重新租用剩余事件**(换发新 token);原批次的旧令牌立即作废,再用它调 `ack`/`ack-prefix`/`quarantine` 都是 409。

- **只能隔离首条**:不允许跳过批次中间的事件(与"禁止越序确认"一致);失败原因为空或纯空白由请求校验拦截(**422**)。
- **单事件批次**:隔离后租约清除,下次 `pull` 返回空批次,直到新事件到达。

查询某 consumer 的隔离记录(按隔离时间分页):

```bash
curl 'http://localhost:8000/consumers/worker-1/quarantine?page=1&page_size=50'
```

```json
{
  "consumer_id": "worker-1",
  "total": 1,
  "page": 1,
  "page_size": 50,
  "items": [
    {
      "id": 1,
      "consumer_id": "worker-1",
      "event": {
        "seq": 1,
        "event_id": "evt-1",
        "occurred_at": "2026-09-26T09:00:01+00:00",
        "payload": {"a": 1},
        "received_at": "2026-09-26T11:59:30.000000+00:00",
        "hash": "2b2f3ae9…(64 位十六进制)"
      },
      "reason": "JSON 解析失败: 缺少必填字段 order_id",
      "quarantined_at": "2026-09-26T12:00:00.000000+00:00"
    }
  ]
}
```

隔离记录携带的是原事件内容的**独立副本**,因此即使原事件之后被安全归档压缩(`/retention/compact` 物理删除),隔离副本仍可随时查看,并可用 `event.hash` 与归档锚点核对;不同 consumer 的隔离记录互相独立,未知 consumer 返回空列表。

### 10. 安全归档压缩

已确认消费的历史事件可压缩归档:物理删除 `(当前锚点, cutoff_seq]` 的连续事件前缀,并把最后一条被删记录的 `seq`/`hash` 持久化为**链锚点**,`/integrity` 之后从锚点继续校验,历史不可篡改性不受影响。

```bash
# 先演练: 只返回预计删除数与边界, 不写库
curl -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' \
  -d '{"cutoff_seq":1000,"dry_run":true}'
```

```json
{"status": "dry_run", "dry_run": true, "would_delete": 1000,
 "from_seq": 1, "through_seq": 1000, "safe_through_seq": 1000}
```

```bash
# 确认无误后真正执行
curl -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' \
  -d '{"cutoff_seq":1000}'
```

```json
{"status": "compacted", "dry_run": false, "deleted": 1000,
 "from_seq": 1, "through_seq": 1000,
 "anchor_seq": 1000, "anchor_hash": "9f2c…(64 位十六进制)",
 "safe_through_seq": 1000}
```

**安全边界**:`cutoff_seq` 不得超过所有已登记 consumer 的**最小 `cursor_seq`**(未确认的事件绝不会被删);一个 consumer 都没有时以**当前最大 seq** 为边界。越界返回 **409** 并给出当前安全上界:

```json
{"detail": "cutoff_seq exceeds safe retention boundary",
 "cutoff_seq": 1200, "safe_through_seq": 1000}
```

**幂等**:`cutoff_seq` 落在既往压缩已覆盖的区间(≤ 当前 `anchor_seq`)时返回 `{"status":"noop","deleted":0,…}`,不重复删除、不改变锚点;`dry_run` 任何时候都不写库。全部事件被压缩后,新写入的事件会从锚点哈希继续接链,`/integrity` 仍返回 `verified=true`。

一个典型的演练-执行循环:

```bash
# 1) 演练, 确认预计删除数与边界
curl -s -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' -d '{"cutoff_seq":1000,"dry_run":true}'
# 2) 执行
curl -s -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' -d '{"cutoff_seq":1000}'
# 3) 校验链(从锚点继续)
curl -s http://localhost:8000/integrity
```

### 11. 持久化快照导出

需要把某一时刻的事件流整体导出(对账、迁移、审计)时,可创建**持久化快照**:在**同一个数据库事务**内把当前全部未压缩事件(含链字段 `prev_hash`/`hash`)连同链锚点复制到独立的快照表,并返回快照 ID、锚点序号与哈希、末尾序号和总数:

```bash
curl -X POST http://localhost:8000/exports
```

```json
{
  "export_id": 1,
  "anchor_seq": 1000,
  "anchor_hash": "9f2c…(64 位十六进制)",
  "end_seq": 1200,
  "total": 200,
  "created_at": "2026-09-27T08:00:00.000000+00:00"
}
```

快照与事件接收(`/events`)、压缩归档(`/retention/compact`)在同一写锁下**串行**执行,因此副本和元数据严格来自同一时刻;事件表为空时创建的是**空快照**(`end_seq == anchor_seq`、`total = 0`)。

按原 `seq` 升序分页读取快照副本:

```bash
# 第一页: after_seq 缺省为该快照的锚点序号
curl 'http://localhost:8000/exports/1/events?limit=100'
# 后续页: 用上一页返回的 next_after_seq
curl 'http://localhost:8000/exports/1/events?after_seq=1100&limit=100'
```

```json
{
  "export_id": 1,
  "after_seq": 1000,
  "limit": 100,
  "count": 100,
  "events": [
    {"seq": 1001, "event_id": "evt-1", "occurred_at": "...", "payload": {"a": 1},
     "received_at": "...", "prev_hash": "9f2c…", "hash": "ab01…"}
  ],
  "next_after_seq": 1100
}
```

- `after_seq`(不含,默认锚点序号)必须落在该快照的 **`[anchor_seq, end_seq]`** 内,否则返回 **422**(响应附 `anchor_seq`/`end_seq`);`limit` 范围 1~1000(默认 100),越界同样 422。
- `next_after_seq` 是下一页的起始序号,**末页为 `null`**;`after_seq == end_seq` 或空快照时始终返回空页(`events: []`、`next_after_seq: null`)。
- 分页只读**副本**:快照之后的继续写入、原事件被压缩归档、服务重启都**不改变**已建快照的分页结果。

删除快照(元数据与全部副本在**同一事务**内原子删除):

```bash
curl -X DELETE http://localhost:8000/exports/1
# => {"export_id": 1, "deleted": true}
```

未知或已删除的快照,`GET /exports/{id}/events` 与 `DELETE /exports/{id}` 统一返回 **404**;删除不影响其他快照。

## 哈希链规范

每条记录写入时在同一事务内计算并存储两个字段:

- `prev_hash`:上一条记录的 `hash`(首条为空串 `""`)
- `hash`:对以下文档做**固定 JSON 规范化**(键排序、紧凑分隔符 `,`/`:`、非 ASCII 不转义)后的 SHA-256 十六进制:

```json
{"seq":1,"event_id":"evt-1","occurred_at":"...","payload":{...},"received_at":"...","prev_hash":""}
```

重复 `event_id` 的投递走幂等返回,不会写入记录,链保持不变;`POST /events/batch` 中批量重复(库中已有或批内前面已出现)的条目同样跳过,只有新建条目参与续链。任何对历史记录的字段篡改、删除或重排都会被 `/integrity` 在首个断点处检出。

**链锚点**:压缩归档删除的历史前缀由单行表 `chain_anchor` 记录——`anchor_seq`(已压缩到的最大 seq)与 `anchor_hash`(最后被删记录的哈希)。锚点之后的第一条现存记录必须 `seq = anchor_seq + 1` 且 `prev_hash = anchor_hash`;事件表为空时新写入的记录也从锚点哈希续链,因此压缩不会削弱后续记录的防篡改能力。

**旧库迁移**:启动时自动为已有 SQLite 数据补加 `prev_hash`/`hash` 列并按首次接收顺序回填整条链,无需手工操作;迁移可重复执行(幂等)。

## 设计要点

- **幂等**:`event_id` 上有 `UNIQUE` 约束,重复插入触发 `IntegrityError` 后返回 200 `duplicate`,并发下也不会产生重复记录。
- **批量接收**:`POST /events/batch` 接受 1~1000 条同格式事件的 JSON 数组,在单个 `BEGIN IMMEDIATE` 事务内"一次查齐库中已存在的 `event_id` + 用批内 `seen` 集合追踪 → 按输入顺序逐条判重、续链接入"。库中已有或批内重复的条目标记 `duplicate`(`seq` 为 null)且不消耗 seq,新建条目 `seq` 连续;响应 `results` 按输入顺序返回并汇总 `created`/`duplicate`,有新建即 201、全重复 200。格式非法在事务前整体 422,写入冲突回滚整批(409),任何失败路径都不留部分新事件;与单条接口共用同一判重逻辑与哈希链,单条/批量可任意交错提交。
- **接收顺序**:自增主键 `id` 即首次接收顺序,重放按它排序。
- **完整性哈希链**:每条记录引用上一条哈希,写入与链字段更新在同一事务完成(`BEGIN IMMEDIATE` 保证并发下链头一致);`GET /integrity` 全链校验。
- **时间比较**:`occurred_at` 归一化为 UTC ISO8601 字符串存储,范围查询走字典序比较,并建有索引。
- **消费游标与租约**:每个 consumer 一行(`consumers` 表),保存已确认游标与当前租约(token、批次起止 seq、过期时间)。`pull` 在 `BEGIN IMMEDIATE` 事务内完成"判租约 → 取批次 → 写租约",与事件写入互斥,并发重复拉取只会得到同一个租约;`ack` 仅推进整批游标并清空租约,token 不符、过期或无租约一律 409。
- **前缀确认**:`POST /consumers/{consumer_id}/ack-prefix` 与 `ack` 共用同一套租约校验,额外要求 `through_seq` 落在 `[lease_from_seq, lease_to_seq]`;在单个 `BEGIN IMMEDIATE` 事务内把游标推进到 `through_seq` 并清除租约,旧 token 立即作废,下次 `pull` 从 `through_seq+1` 重新租用。`through_seq` 等于批次末尾即整批确认;未知 consumer、过期/伪造/旧 token、序号越界均 409 且状态不变。
- **游标回退**:`POST /consumers/{consumer_id}/rewind` 在单个 `BEGIN IMMEDIATE` 事务内完成"校验 consumer/租约/回退区间 `[anchor_seq, cursor_seq]` → 清除已过期租约 → 回退游标",与 `pull`/`ack`/`ack-prefix`/`compact` 串行,并发下只有单一结果。未过期租约、低于链锚点、向前推进、未知 consumer 均 409 且状态不变;回退后游标不低于链锚点,不会产生指向已压缩事件的租约。
- **毒丸隔离**:`POST /consumers/{consumer_id}/quarantine` 在单个 `BEGIN IMMEDIATE` 事务内完成"核对令牌(与 `ack` 同套:无租约/过期/旧令牌 409)→ 核对租约起点与游标相邻(考虑链锚点)→ 复制首条事件完整内容、原哈希与非空原因到独立 `quarantined_events` 表 → 游标仅越过这一条并清除整批租约"。下次 `pull` 重新租用剩余事件并换发新 token,旧令牌立即作废;失败原因空白返回 422。`GET /consumers/{consumer_id}/quarantine` 分页查看隔离记录;副本独立于原事件存储,原事件被压缩归档(物理删除)后仍可查看。隔离会把游标推进过该事件,因此被隔离事件视为"已处理",可正常纳入安全归档,内容由隔离副本留存。
- **安全归档压缩**:`POST /retention/compact` 在单个 `BEGIN IMMEDIATE` 事务内完成"读锚点 → 算安全边界(所有 consumer 的最小 `cursor_seq`,无 consumer 时为当前最大 seq)→ 校验 `cutoff_seq` → 删连续前缀 → 写链锚点"。越界 409 并返回 `safe_through_seq`;`dry_run` 只算不写;已覆盖区间幂等返回 `noop`。
- **持久化快照导出**:`POST /exports` 在单个 `BEGIN IMMEDIATE` 事务内完成"读链锚点 → 写快照元数据(锚点 seq/hash、末尾 seq、总数)→ `INSERT ... SELECT` 整体复制当前全部未压缩事件到 `export_events`",与事件写入、压缩归档串行,副本与元数据来自同一时刻。副本落库后不可变:`GET /exports/{id}/events` 只查快照表,按 `(snapshot_id, seq)` 主键升序分页(多取一条判断 `next_after_seq`,末页为 `null`),后续写入/压缩/重启均不影响;`after_seq` 越出 `[anchor_seq, end_seq]` 返回 422。`DELETE /exports/{id}` 在同一事务内删除元数据与全部副本,未知或已删除统一 404。
- **配置**:数据库路径由环境变量 `DB_PATH` 控制(容器内默认 `/data/events.db`)。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/events` | 接收单个事件(幂等) |
| POST | `/events/batch` | 批量接收 1~1000 条事件(JSON 数组,单写事务):库中已有/批内重复标 `duplicate` 不写,新建按序续链;响应按输入顺序给出状态与 `seq`,整批失败无部分写入 |
| GET | `/events` | 时间范围分页查询(`from`/`to`/`page`/`page_size`) |
| POST | `/replay` | 批量重放,按首次接收顺序输出 |
| GET | `/integrity` | 从链锚点校验完整性哈希链,返回 `verified`/`checked`/`first_break`/`anchor_seq` |
| POST | `/consumers/{consumer_id}/pull` | 按首次接收顺序拉取并原子租用未确认事件(`limit`/`lease_seconds`) |
| POST | `/consumers/{consumer_id}/ack` | 确认当前租约批次并推进游标;过期/伪造/旧 token 返回 409 |
| POST | `/consumers/{consumer_id}/ack-prefix` | 确认租约内 `[批次起点, through_seq]` 连续前缀并清除租约,下次 `pull` 从 `through_seq+1` 重租;`through_seq` 到批次末尾即整批确认;未知 consumer/过期/旧 token/越界序号返回 409 |
| POST | `/consumers/{consumer_id}/rewind` | 游标回退到 `target_seq`(限 `[anchor_seq, cursor_seq]`),下次 `pull` 从 `target_seq+1` 重投;未过期租约/越界/未知 consumer 返回 409 |
| POST | `/consumers/{consumer_id}/quarantine` | 隔离当前未过期租约的首条事件(带非空 `reason`):单事务核对令牌与租约相邻性、复制完整事件到独立隔离记录、游标越过一条并清除整批租约;无租约/过期/旧令牌/不相邻返回 409,原因为空返回 422 |
| GET | `/consumers/{consumer_id}/quarantine` | 分页查询该 consumer 的隔离记录;携带原事件完整内容副本,原事件归档后仍可查看 |
| POST | `/retention/compact` | 安全归档压缩连续事件前缀(`cutoff_seq`/`dry_run`);越界 409 并返回 `safe_through_seq` |
| POST | `/exports` | 创建持久化快照:单事务复制全部未压缩事件与链锚点,返回快照 ID、锚点 seq/hash、末尾 seq 与总数 |
| GET | `/exports/{id}/events` | 按原 `seq` 升序分页读取快照副本(`after_seq` 默认锚点序号,限 `[anchor_seq, end_seq]` 否则 422;`limit` 1~1000;`next_after_seq` 末页为 null) |
| DELETE | `/exports/{id}` | 单事务原子删除快照元数据与全部副本;未知或已删除统一 404 |
| GET | `/health` | 健康检查 |

交互式文档:启动后访问 `http://localhost:8000/docs`。
