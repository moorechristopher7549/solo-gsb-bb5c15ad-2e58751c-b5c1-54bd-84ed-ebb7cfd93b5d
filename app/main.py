import json
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import Body, FastAPI, Path, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from .chain import record_hash
from .db import get_chain_anchor, get_conn, init_db

app = FastAPI(title="Webhook Event Receiver & Replay")


@app.on_event("startup")
def startup() -> None:
    init_db()


# ---------- models ----------

class EventIn(BaseModel):
    event_id: str = Field(..., min_length=1, max_length=255)
    occurred_at: datetime
    payload: dict[str, Any]


class ReplayIn(BaseModel):
    event_ids: list[str] = Field(..., min_length=1, max_length=1000)


class PullIn(BaseModel):
    limit: int = Field(100, ge=1, le=1000)
    lease_seconds: int = Field(30, ge=1, le=86400)


class AckIn(BaseModel):
    lease_token: str = Field(..., min_length=1, max_length=255)


class AckPrefixIn(BaseModel):
    lease_token: str = Field(..., min_length=1, max_length=255)
    # 不加下界约束: 超出租约区间(含 <=0)统一走 409, 而不是 422
    through_seq: int


class RewindIn(BaseModel):
    # 不加下界约束: 低于链锚点(含负数)统一走 409, 而不是 422
    target_seq: int = Field(...)


class CompactIn(BaseModel):
    cutoff_seq: int = Field(..., ge=0)
    dry_run: bool = False


class QuarantineIn(BaseModel):
    lease_token: str = Field(..., min_length=1, max_length=255)
    # 非空失败原因; 只允许空白也视为非法(422), 409 只保留给租约状态冲突
    reason: str = Field(..., min_length=1, max_length=2000)

    @field_validator("reason")
    @classmethod
    def reason_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("reason must not be blank")
        return v


# ---------- helpers ----------

def normalize_dt(dt: datetime) -> str:
    """归一化为 UTC ISO8601 字符串, 保证可按字典序做时间范围比较。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def lease_conflict(detail: str) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": detail})


def row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "seq": row["id"],  # 首次接收顺序号
        "event_id": row["event_id"],
        "occurred_at": row["occurred_at"],
        "payload": json.loads(row["payload"]),
        "received_at": row["received_at"],
    }


def row_to_quarantine(row: sqlite3.Row) -> dict[str, Any]:
    """隔离记录视图: 自带原事件完整内容副本, 原记录被压缩归档后仍可查看。"""
    return {
        "id": row["id"],
        "consumer_id": row["consumer_id"],
        "event": {
            "seq": row["event_seq"],
            "event_id": row["event_id"],
            "occurred_at": row["occurred_at"],
            "payload": json.loads(row["payload"]),
            "received_at": row["received_at"],
            "hash": row["event_hash"],
        },
        "reason": row["reason"],
        "quarantined_at": row["quarantined_at"],
    }


def row_to_export_event(row: sqlite3.Row) -> dict[str, Any]:
    """快照副本中的事件视图: 含链字段 prev_hash/hash 的完整记录。"""
    return {
        "seq": row["seq"],
        "event_id": row["event_id"],
        "occurred_at": row["occurred_at"],
        "payload": json.loads(row["payload"]),
        "received_at": row["received_at"],
        "prev_hash": row["prev_hash"],
        "hash": row["hash"],
    }


# ---------- endpoints ----------

def current_chain_head(conn: sqlite3.Connection) -> str:
    """读取当前链头哈希: 取最后一条事件的 hash; 事件表为空时从链锚点续链(创世为空串)。"""
    row = conn.execute("SELECT hash FROM events ORDER BY id DESC LIMIT 1").fetchone()
    if row:
        return row["hash"]
    # 事件表为空: 若曾压缩归档则从链锚点续链, 否则为创世空串
    _, prev_hash = get_chain_anchor(conn)
    return prev_hash


def insert_chained_event(
    conn: sqlite3.Connection,
    *,
    event_id: str,
    occurred_at: str,
    payload: Any,
    received_at: str,
    prev_hash: str,
) -> tuple[int, str]:
    """在当前写事务内插入一条事件并续接哈希链, 返回 (新 seq, 本记录哈希)。调用方须持有写事务。"""
    cur = conn.execute(
        "INSERT INTO events (event_id, occurred_at, payload, received_at)"
        " VALUES (?, ?, ?, ?)",
        (event_id, occurred_at, json.dumps(payload, ensure_ascii=False), received_at),
    )
    seq = cur.lastrowid
    h = record_hash(
        seq=seq,
        event_id=event_id,
        occurred_at=occurred_at,
        payload=payload,
        received_at=received_at,
        prev_hash=prev_hash,
    )
    conn.execute(
        "UPDATE events SET prev_hash = ?, hash = ? WHERE id = ?",
        (prev_hash, h, seq),
    )
    return seq, h


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events", status_code=201)
def create_event(event: EventIn):
    """接收单个事件, 按 event_id 幂等: 重复投递不产生重复记录, 也不改变哈希链。"""
    received_at = utc_now()
    occurred_at = normalize_dt(event.occurred_at)
    try:
        with get_conn() as conn:
            # BEGIN IMMEDIATE 提前获取写锁, 保证"读链头 -> 写入"在同一事务内原子完成
            conn.execute("BEGIN IMMEDIATE")
            prev_hash = current_chain_head(conn)
            insert_chained_event(
                conn,
                event_id=event.event_id,
                occurred_at=occurred_at,
                payload=event.payload,
                received_at=received_at,
                prev_hash=prev_hash,
            )
    except sqlite3.IntegrityError:
        # event_id 已存在 -> 幂等返回, 不写入重复记录, 链保持不变
        return JSONResponse(
            status_code=200,
            content={"status": "duplicate", "event_id": event.event_id},
        )
    return {"status": "created", "event_id": event.event_id}


@app.post("/events/batch")
def create_events_batch(
    events: list[EventIn] = Body(..., min_length=1, max_length=1000),
):
    """批量接收 1~1000 条事件(与单条接口同格式的 JSON 数组), 整批在同一写事务中处理。

    - 按请求顺序逐条判定 event_id: 库中已有、或本批前面已出现过的 ID 标记
      duplicate, 不写入、不消耗 seq; 仅首次出现的 ID 入库。
    - 新事件按请求顺序从当前链头逐条续接哈希链, seq 连续无空洞。
    - 任一条格式非法(422, 校验在事务前完成)或写入失败(事务回滚)时,
      整批不留下部分新事件。
    - 响应 results 严格按输入顺序给出每条状态(created 附 seq; duplicate 的 seq 为 null),
      并汇总 created/duplicate 数量。全部重复返回 200, 否则 201。
    """
    # 整批共用同一接收时间(与单条接口一样进入哈希), 保证批内链一致
    received_at = utc_now()
    normalized = [(e.event_id, normalize_dt(e.occurred_at), e.payload) for e in events]

    results: list[dict[str, Any]] = []
    created = 0
    with get_conn() as conn:
        try:
            # BEGIN IMMEDIATE 提前获取写锁: 判定重复 -> 批量写入在同一事务内原子完成
            conn.execute("BEGIN IMMEDIATE")
            # 库中已存在的 event_id 一次取齐; 本批内首次出现的 ID 实时加入 seen
            placeholders = ",".join("?" * len(normalized))
            existing = {
                row["event_id"]
                for row in conn.execute(
                    f"SELECT event_id FROM events WHERE event_id IN ({placeholders})",
                    [eid for eid, _, _ in normalized],
                ).fetchall()
            }
            seen: set[str] = set(existing)
            prev_hash = current_chain_head(conn)

            for event_id, occurred_at, payload in normalized:
                if event_id in seen:
                    # 库中已有或本批前面出现过: 幂等跳过, 不写入也不推进链头
                    results.append(
                        {"event_id": event_id, "status": "duplicate", "seq": None}
                    )
                    continue
                # 唯一约束兜底(写锁下理论上不会冲突): 冲突即放弃整批
                seq, h = insert_chained_event(
                    conn,
                    event_id=event_id,
                    occurred_at=occurred_at,
                    payload=payload,
                    received_at=received_at,
                    prev_hash=prev_hash,
                )
                prev_hash = h
                seen.add(event_id)
                created += 1
                results.append({"event_id": event_id, "status": "created", "seq": seq})
        except sqlite3.IntegrityError:
            # 并发兜底: 批量不接受"部分写入", 回滚整批, 不留下任何新事件
            conn.rollback()
            return JSONResponse(
                status_code=409,
                content={"detail": "concurrent event_id conflict, whole batch rejected"},
            )

    body = {
        "created": created,
        "duplicate": len(events) - created,
        "results": results,
    }
    return JSONResponse(status_code=201 if created else 200, content=body)


@app.get("/events")
def list_events(
    from_: Optional[datetime] = Query(None, alias="from", description="occurred_at 下界(含)"),
    to: Optional[datetime] = Query(None, description="occurred_at 上界(含)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """按 occurred_at 时间范围分页查询。"""
    clauses, params = [], []
    if from_ is not None:
        clauses.append("occurred_at >= ?")
        params.append(normalize_dt(from_))
    if to is not None:
        clauses.append("occurred_at <= ?")
        params.append(normalize_dt(to))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM events {where}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM events {where} ORDER BY occurred_at, id LIMIT ? OFFSET ?",
            (*params, page_size, (page - 1) * page_size),
        ).fetchall()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [row_to_event(r) for r in rows],
    }


@app.post("/replay")
def replay(req: ReplayIn):
    """批量重放: 命中的事件按首次接收顺序输出, 未命中的标记为 skipped。"""
    # 请求内去重, 保证重复 id 不产生重复输出
    requested = list(dict.fromkeys(req.event_ids))

    found: dict[str, dict[str, Any]] = {}
    with get_conn() as conn:
        placeholders = ",".join("?" * len(requested))
        rows = conn.execute(
            f"SELECT * FROM events WHERE event_id IN ({placeholders}) ORDER BY id",
            requested,
        ).fetchall()
    for r in rows:
        found[r["event_id"]] = row_to_event(r)

    results: list[dict[str, Any]] = []
    # 命中部分: 严格按首次接收顺序(seq)输出
    for event in sorted(found.values(), key=lambda e: e["seq"]):
        results.append({"event_id": event["event_id"], "status": "replayed", "event": event})
    # 未命中部分: 按请求顺序追加, 标记 skipped
    for eid in requested:
        if eid not in found:
            results.append({"event_id": eid, "status": "skipped", "reason": "not_found"})

    replayed = sum(1 for r in results if r["status"] == "replayed")
    return {
        "replayed": replayed,
        "skipped": len(results) - replayed,
        "results": results,
    }


# ---------- consumer cursor: pull / ack / rewind ----------

@app.post("/consumers/{consumer_id}/pull")
def pull(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: PullIn = Body(default_factory=PullIn),
):
    """按首次接收顺序拉取尚未确认的连续事件并原子租用。

    - 首次 consumer 的游标为 0, 即从 seq=1 开始投递。
    - 存在未过期租约时, 重复拉取原样返回同一批次(不续租、不换 token)。
    - 租约过期后同一批次可被重新投递(换发新 token)。
    - 队列为空(没有游标之后的事件)时返回空批次, 不创建租约。
    """
    now = utc_now()
    # BEGIN IMMEDIATE 与事件写入互斥, 保证"读批次 -> 建租约"原子完成
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        # 首次出现的 consumer 自动建档, 游标从 0 开始
        conn.execute(
            "INSERT OR IGNORE INTO consumers (consumer_id, cursor_seq) VALUES (?, 0)",
            (consumer_id,),
        )
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()

        active = (
            consumer["lease_token"] is not None
            and datetime.fromisoformat(consumer["lease_expires_at"]) > datetime.fromisoformat(now)
        )
        if active:
            # 租约未过期: 重复拉取必须返回同一批次
            rows = conn.execute(
                "SELECT * FROM events WHERE id BETWEEN ? AND ? ORDER BY id",
                (consumer["lease_from_seq"], consumer["lease_to_seq"]),
            ).fetchall()
            return {
                "consumer_id": consumer_id,
                "events": [row_to_event(r) for r in rows],
                "lease_token": consumer["lease_token"],
                "lease_expires_at": consumer["lease_expires_at"],
            }

        if consumer["lease_token"] is not None:
            # 上一租约已过期: 同一批次可重新投递(沿用原区间, 换发新 token)
            rows = conn.execute(
                "SELECT * FROM events WHERE id BETWEEN ? AND ? ORDER BY id",
                (consumer["lease_from_seq"], consumer["lease_to_seq"]),
            ).fetchall()
            token = secrets.token_urlsafe(32)
            expires_at = (
                datetime.fromisoformat(now) + timedelta(seconds=req.lease_seconds)
            ).isoformat()
            conn.execute(
                "UPDATE consumers SET lease_token = ?, lease_expires_at = ?"
                " WHERE consumer_id = ?",
                (token, expires_at, consumer_id),
            )
            return {
                "consumer_id": consumer_id,
                "events": [row_to_event(r) for r in rows],
                "lease_token": token,
                "lease_expires_at": expires_at,
            }

        # 从未租用: 从游标之后取一段连续事件
        rows = conn.execute(
            "SELECT * FROM events WHERE id > ? ORDER BY id LIMIT ?",
            (consumer["cursor_seq"], req.limit),
        ).fetchall()
        if not rows:
            # 空队列: 返回空批次且不创建租约
            return {
                "consumer_id": consumer_id,
                "events": [],
                "lease_token": None,
                "lease_expires_at": None,
            }

        from_seq, to_seq = rows[0]["id"], rows[-1]["id"]
        token = secrets.token_urlsafe(32)
        expires_at = (
            datetime.fromisoformat(now) + timedelta(seconds=req.lease_seconds)
        ).isoformat()
        conn.execute(
            "UPDATE consumers SET lease_token = ?, lease_from_seq = ?,"
            " lease_to_seq = ?, lease_expires_at = ? WHERE consumer_id = ?",
            (token, from_seq, to_seq, expires_at, consumer_id),
        )
        return {
            "consumer_id": consumer_id,
            "events": [row_to_event(r) for r in rows],
            "lease_token": token,
            "lease_expires_at": expires_at,
        }


@app.post("/consumers/{consumer_id}/ack")
def ack(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: AckIn = Body(...),
):
    """确认当前租约批次: 仅接受未过期且与当前租约一致的 token, 游标推进到批次末尾。

    过期、伪造或旧 token 一律 409; 游标只随整批确认前移, 天然禁止越序确认。
    """
    now = datetime.now(timezone.utc)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        # 未知 consumer / 从未拉取过: 没有可确认的租约
        if consumer is None or consumer["lease_token"] is None:
            return lease_conflict("no active lease")
        if consumer["lease_token"] != req.lease_token:
            # 伪造或旧 token(旧批次过期后已被新租约取代)
            return lease_conflict("invalid lease token")
        if datetime.fromisoformat(consumer["lease_expires_at"]) <= now:
            # 租约已过期, 该批次可被重新投递, 旧 token 不得再确认
            return lease_conflict("lease expired")

        to_seq = consumer["lease_to_seq"]
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL,"
            " lease_from_seq = NULL, lease_to_seq = NULL, lease_expires_at = NULL"
            " WHERE consumer_id = ?",
            (to_seq, consumer_id),
        )
    return {
        "consumer_id": consumer_id,
        "acknowledged": True,
        "cursor_seq": to_seq,
    }


@app.post("/consumers/{consumer_id}/ack-prefix")
def ack_prefix(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: AckPrefixIn = Body(...),
):
    """确认当前租约批次 [from_seq, through_seq] 的连续前缀。

    仅接受未过期且与当前租约一致的 token, through_seq 必须落在
    [lease_from_seq, lease_to_seq] 内; 校验通过后在同一写事务内把游标
    推进到 through_seq 并清除租约, 下次 pull 从 through_seq+1 重新租用,
    旧 token 立即作废(不能再用于 ack 或前缀确认)。

    through_seq == lease_to_seq 时等同整批确认。未知 consumer、无租约、
    token 不符(伪造/旧 token)、租约过期或序号越界一律 409 且状态不变。
    """
    now = datetime.now(timezone.utc)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        # 未知 consumer / 从未拉取过: 没有可确认的租约
        if consumer is None or consumer["lease_token"] is None:
            return lease_conflict("no active lease")
        if consumer["lease_token"] != req.lease_token:
            # 伪造或旧 token(旧批次过期后已被新租约取代, 或已被确认清除)
            return lease_conflict("invalid lease token")
        if datetime.fromisoformat(consumer["lease_expires_at"]) <= now:
            # 租约已过期, 该批次可被重新投递, 旧 token 不得再确认
            return lease_conflict("lease expired")
        if req.through_seq < consumer["lease_from_seq"]:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "through_seq before batch start",
                    "through_seq": req.through_seq,
                    "lease_from_seq": consumer["lease_from_seq"],
                    "lease_to_seq": consumer["lease_to_seq"],
                },
            )
        if req.through_seq > consumer["lease_to_seq"]:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "through_seq beyond batch end",
                    "through_seq": req.through_seq,
                    "lease_from_seq": consumer["lease_from_seq"],
                    "lease_to_seq": consumer["lease_to_seq"],
                },
            )

        # 同一事务内: 推进游标到 through_seq 并清除原租约
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL,"
            " lease_from_seq = NULL, lease_to_seq = NULL, lease_expires_at = NULL"
            " WHERE consumer_id = ?",
            (req.through_seq, consumer_id),
        )
    return {
        "consumer_id": consumer_id,
        "acknowledged": True,
        "through_seq": req.through_seq,
        "cursor_seq": req.through_seq,
    }


@app.post("/consumers/{consumer_id}/rewind")
def rewind(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: RewindIn = Body(...),
):
    """将已登记 consumer 的游标回退到 target_seq, 下一次 pull 从 target_seq+1 重新投递。

    - 仅允许回退到 [anchor_seq, cursor_seq]: 低于链锚点(事件已压缩归档)或
      向前推进(target_seq > 当前游标)一律 409, 状态不变; 未知 consumer 同样 409。
    - 存在未过期租约时 409; 已过期租约在同一事务内清除后再回退,
      避免后续 pull 重投旧批次区间。
    - 与 pull/ack/compact 共用 BEGIN IMMEDIATE 写锁, 并发下只有单一结果;
      回退后游标不低于链锚点, 不会产出指向已压缩事件的租约。
    """
    now = datetime.now(timezone.utc)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_expires_at"
            " FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        # 未知 consumer: 不像 pull 那样自动建档, 直接 409
        if consumer is None:
            return lease_conflict("unknown consumer")

        if (
            consumer["lease_token"] is not None
            and datetime.fromisoformat(consumer["lease_expires_at"]) > now
        ):
            # 租约未过期: 回退会与在途批次冲突, 拒绝且状态不变
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "consumer has an active lease",
                    "lease_expires_at": consumer["lease_expires_at"],
                },
            )

        anchor_seq, _ = get_chain_anchor(conn)
        cursor_seq = consumer["cursor_seq"]
        target = req.target_seq
        if target < anchor_seq:
            # 目标早于链锚点: 对应事件已压缩归档, 无法重新投递
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "target_seq below chain anchor",
                    "target_seq": target,
                    "anchor_seq": anchor_seq,
                },
            )
        if target > cursor_seq:
            # 只允许回退; 向前推进只能经由 ack 整批确认
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "target_seq would advance cursor",
                    "target_seq": target,
                    "cursor_seq": cursor_seq,
                },
            )

        # 同一事务内: 清除(已过期的)租约并回退游标
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL,"
            " lease_from_seq = NULL, lease_to_seq = NULL, lease_expires_at = NULL"
            " WHERE consumer_id = ?",
            (target, consumer_id),
        )
    return {
        "consumer_id": consumer_id,
        "previous_cursor_seq": cursor_seq,
        "cursor_seq": target,
        "next_seq": target + 1,
    }


# ---------- quarantine: 隔离毒丸事件 ----------

@app.post("/consumers/{consumer_id}/quarantine", status_code=201)
def quarantine(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: QuarantineIn = Body(...),
):
    """隔离当前未过期租约批次的首条事件(消费失败的"毒丸"), 只越过这一条继续消费。

    单个写事务(BEGIN IMMEDIATE)内完成全部核对与状态变更:
    - 必须存在与 lease_token 一致且未过期的租约, 且批次起点必须是下一条
      可投递事件(lease_from_seq = max(cursor_seq, anchor_seq) + 1),
      防止旧批次/旧令牌绕过;
    - 复制首条事件的完整内容(event_id/occurred_at/payload/received_at/hash)及
      失败原因到独立隔离记录;
    - 游标仅越过这一条(cursor_seq = lease_from_seq)并清除整批租约;
    - 下一次 pull 重新租用剩余事件(换发新 token), 旧令牌立即作废, 不能再确认。

    未知 consumer、无租约、伪造/旧 token、租约过期或起点不相邻一律 409 且
    状态不变; 失败原因为空或纯空白由请求校验拦截(422)。
    """
    now = datetime.now(timezone.utc)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        # 未知 consumer / 从未拉取过: 没有可隔离的租约
        if consumer is None or consumer["lease_token"] is None:
            return lease_conflict("no active lease")
        if consumer["lease_token"] != req.lease_token:
            # 伪造或旧 token(旧批次过期后已被新租约取代, 或已被 ack/隔离清除)
            return lease_conflict("invalid lease token")
        if datetime.fromisoformat(consumer["lease_expires_at"]) <= now:
            # 租约已过期, 该批次可被重新投递, 旧 token 不得再隔离
            return lease_conflict("lease expired")

        from_seq = consumer["lease_from_seq"]
        # 租约起点必须与"下一条可投递事件"相邻: 通常即 cursor_seq + 1;
        # 压缩归档后才建档的 consumer 游标仍为 0, 已归档区间不可投递, 起点为 anchor_seq + 1。
        anchor_seq, _ = get_chain_anchor(conn)
        expected_from = max(consumer["cursor_seq"], anchor_seq) + 1
        if from_seq != expected_from:
            # 租约起点不与可投递队首相邻: 状态不自洽的旧批次, 拒绝且状态不变
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "lease start not adjacent to cursor",
                    "cursor_seq": consumer["cursor_seq"],
                    "anchor_seq": anchor_seq,
                    "lease_from_seq": from_seq,
                    "lease_to_seq": consumer["lease_to_seq"],
                },
            )

        # 只允许隔离首条事件: 按批次起点精确取这一条
        row = conn.execute(
            "SELECT id, event_id, occurred_at, payload, received_at, hash"
            " FROM events WHERE id = ?",
            (from_seq,),
        ).fetchone()
        if row is None:
            # 首条事件缺失(如已被压缩归档): 不能安全隔离, 状态不变
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "lease head event unavailable",
                    "lease_from_seq": from_seq,
                },
            )

        quarantined_at = now.isoformat()
        cur = conn.execute(
            "INSERT INTO quarantined_events (consumer_id, event_seq, event_id,"
            " occurred_at, payload, received_at, event_hash, reason,"
            " quarantined_at, lease_token)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                consumer_id,
                row["id"],
                row["event_id"],
                row["occurred_at"],
                row["payload"],
                row["received_at"],
                row["hash"],
                req.reason,
                quarantined_at,
                req.lease_token,
            ),
        )
        quarantine_id = cur.lastrowid

        # 游标仅越过这一条, 整批租约清除: 剩余事件下次 pull 重新租用
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL,"
            " lease_from_seq = NULL, lease_to_seq = NULL, lease_expires_at = NULL"
            " WHERE consumer_id = ?",
            (from_seq, consumer_id),
        )

    return {
        "consumer_id": consumer_id,
        "quarantined": True,
        "quarantine_id": quarantine_id,
        "seq": row["id"],
        "event_id": row["event_id"],
        "cursor_seq": from_seq,
        "quarantined_at": quarantined_at,
    }


@app.get("/consumers/{consumer_id}/quarantine")
def list_quarantine(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """按 consumer_id 查询该 consumer 的隔离记录(隔离时间顺序分页)。

    每条记录携带原事件的完整内容副本, 因此原事件被 /retention/compact
    安全归档(物理删除)后, 隔离副本仍可查看。未知 consumer 或无记录返回空列表。
    """
    with get_conn() as conn:
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM quarantined_events WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM quarantined_events WHERE consumer_id = ?"
            " ORDER BY id LIMIT ? OFFSET ?",
            (consumer_id, page_size, (page - 1) * page_size),
        ).fetchall()

    return {
        "consumer_id": consumer_id,
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [row_to_quarantine(r) for r in rows],
    }


# ---------- retention: 安全归档压缩 ----------

@app.post("/retention/compact")
def compact(req: CompactIn):
    """压缩归档 (当前锚点, cutoff_seq] 的连续事件前缀。

    - 安全边界: cutoff_seq 不得超过所有已登记 consumer 的最小 cursor_seq;
      无 consumer 时以当前最大 seq 为边界。越界返回 409 并给出 safe_through_seq。
    - 执行时持久化最后一条被删记录的 seq/hash 作为链锚点, /integrity 从锚点继续校验。
    - dry_run=true 只返回预计删除数与边界, 不写库。
    - cutoff_seq 已被既往压缩覆盖时为幂等空操作, 不再校验安全边界。
    """
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        anchor_seq, anchor_hash = get_chain_anchor(conn)

        # 安全边界: 已登记 consumer 的最小游标; 一个都没有时为当前最大 seq
        row = conn.execute("SELECT MIN(cursor_seq) AS m FROM consumers").fetchone()
        if row["m"] is not None:
            safe_through_seq = row["m"]
        else:
            row = conn.execute("SELECT MAX(id) AS m FROM events").fetchone()
            safe_through_seq = row["m"] or 0

        # 幂等: 请求区间已被既往压缩覆盖, 无记录可删, 直接返回
        if req.cutoff_seq <= anchor_seq:
            result: dict[str, Any] = {
                "dry_run": req.dry_run,
                "from_seq": anchor_seq + 1,
                "through_seq": anchor_seq,  # from > through 表示空区间
                "safe_through_seq": safe_through_seq,
            }
            if req.dry_run:
                return {"status": "dry_run", **result, "would_delete": 0}
            return {
                "status": "noop",
                **result,
                "deleted": 0,
                "anchor_seq": anchor_seq,
                "anchor_hash": anchor_hash,
            }

        if req.cutoff_seq > safe_through_seq:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "cutoff_seq exceeds safe retention boundary",
                    "cutoff_seq": req.cutoff_seq,
                    "safe_through_seq": safe_through_seq,
                },
            )

        # 连续前缀: 从锚点后一条起逐条连续, 遇断档即止
        rows = conn.execute(
            "SELECT id, hash FROM events WHERE id > ? AND id <= ? ORDER BY id",
            (anchor_seq, req.cutoff_seq),
        ).fetchall()
        prefix: list[sqlite3.Row] = []
        expected = anchor_seq + 1
        for r in rows:
            if r["id"] != expected:
                break
            prefix.append(r)
            expected += 1

        from_seq = anchor_seq + 1
        through_seq = prefix[-1]["id"] if prefix else anchor_seq

        if req.dry_run:
            # 只返回预计删除数与边界, 不写库
            return {
                "status": "dry_run",
                "dry_run": True,
                "would_delete": len(prefix),
                "from_seq": from_seq,
                "through_seq": through_seq,
                "safe_through_seq": safe_through_seq,
            }

        if prefix:
            conn.execute(
                "DELETE FROM events WHERE id BETWEEN ? AND ?",
                (from_seq, through_seq),
            )
            # 持久化链锚点: 最后一条被删记录的 seq/hash
            anchor_seq, anchor_hash = through_seq, prefix[-1]["hash"]
            conn.execute(
                "INSERT INTO chain_anchor (id, anchor_seq, anchor_hash) VALUES (1, ?, ?)"
                " ON CONFLICT(id) DO UPDATE SET anchor_seq = excluded.anchor_seq,"
                " anchor_hash = excluded.anchor_hash",
                (anchor_seq, anchor_hash),
            )

        return {
            "status": "compacted" if prefix else "noop",
            "dry_run": False,
            "deleted": len(prefix),
            "from_seq": from_seq,
            "through_seq": through_seq,
            "anchor_seq": anchor_seq,
            "anchor_hash": anchor_hash,
            "safe_through_seq": safe_through_seq,
        }


def _chain_break(checked: int, seq: int, event_id: str, reason: str) -> dict[str, Any]:
    return {
        "verified": False,
        "checked": checked,
        "first_break": {"seq": seq, "event_id": event_id, "reason": reason},
    }


@app.get("/integrity")
def integrity():
    """从链锚点开始校验哈希链: 序号连续性、前后哈希衔接、记录内容哈希。

    压缩归档后, 锚点(最后被删记录的 seq/hash)即校验起点; 全部事件已压缩时
    没有待校验记录, 仍返回 verified=true。
    """
    with get_conn() as conn:
        anchor_seq, anchor_hash = get_chain_anchor(conn)
        rows = conn.execute(
            "SELECT id, event_id, occurred_at, payload, received_at, prev_hash, hash"
            " FROM events WHERE id > ? ORDER BY id",
            (anchor_seq,),
        ).fetchall()

    checked = 0
    prev_seq = anchor_seq
    prev_hash = anchor_hash
    for row in rows:
        seq = row["id"]
        # 序号连续性: 相邻记录 seq 必须逐一递增(首条须为锚点 seq + 1)
        if seq != prev_seq + 1:
            return _chain_break(checked, seq, row["event_id"], "seq_gap")
        # 前后哈希衔接: prev_hash 必须等于上一条记录的 hash(首条为锚点哈希)
        if row["prev_hash"] is None or row["prev_hash"] != prev_hash:
            return _chain_break(checked, seq, row["event_id"], "prev_hash_mismatch")
        # 事件字段: 按固定 JSON 规范重算哈希并与存储值比对
        if row["hash"] is None or row["hash"] != record_hash(
            seq=seq,
            event_id=row["event_id"],
            occurred_at=row["occurred_at"],
            payload=json.loads(row["payload"]),
            received_at=row["received_at"],
            prev_hash=row["prev_hash"],
        ):
            return _chain_break(checked, seq, row["event_id"], "hash_mismatch")
        checked += 1
        prev_seq = seq
        prev_hash = row["hash"]

    return {
        "verified": True,
        "checked": checked,
        "first_break": None,
        "anchor_seq": anchor_seq,
    }


# ---------- exports: 持久化快照导出 ----------

@app.post("/exports", status_code=201)
def create_export():
    """创建持久化快照: 同一事务内复制当前全部未压缩事件及链锚点。

    BEGIN IMMEDIATE 与事件写入(/events)、压缩归档(/retention/compact)互斥,
    保证副本与元数据(锚点/末尾序号/总数)来自同一时刻。副本落库后不再变化:
    后续写入、原事件被压缩归档、服务重启都不影响已建快照的分页结果。
    事件表为空时为空快照: end_seq == anchor_seq, total = 0。
    """
    created_at = utc_now()
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        anchor_seq, anchor_hash = get_chain_anchor(conn)
        row = conn.execute("SELECT COUNT(*) AS c, MAX(id) AS m FROM events").fetchone()
        total = row["c"]
        # 空事件表: 末尾序号等于锚点序号
        end_seq = row["m"] if row["m"] is not None else anchor_seq
        cur = conn.execute(
            "INSERT INTO export_snapshots (anchor_seq, anchor_hash, end_seq, total,"
            " created_at) VALUES (?, ?, ?, ?, ?)",
            (anchor_seq, anchor_hash, end_seq, total, created_at),
        )
        export_id = cur.lastrowid
        # 同一事务内整体复制当前全部未压缩事件(含链字段 prev_hash/hash)
        conn.execute(
            "INSERT INTO export_events (snapshot_id, seq, event_id, occurred_at,"
            " payload, received_at, prev_hash, hash)"
            " SELECT ?, id, event_id, occurred_at, payload, received_at, prev_hash,"
            " hash FROM events ORDER BY id",
            (export_id,),
        )
    return {
        "export_id": export_id,
        "anchor_seq": anchor_seq,
        "anchor_hash": anchor_hash,
        "end_seq": end_seq,
        "total": total,
        "created_at": created_at,
    }


@app.get("/exports/{export_id}/events")
def list_export_events(
    export_id: int = Path(...),
    after_seq: Optional[int] = Query(
        None, description="起始序号(不含), 默认为该快照的锚点序号"
    ),
    limit: int = Query(100, ge=1, le=1000),
):
    """按原 seq 升序分页读取快照副本, 返回完整事件(含 prev_hash/hash)。

    - after_seq 缺省为快照锚点序号, 即从副本第一条开始; 必须落在
      [anchor_seq, end_seq] 内, 否则 422(响应附锚点与末尾序号)。
    - next_after_seq 为下一页的起始序号, 末页为 null; 空快照始终返回空页。
    - 只读副本: 与 events 表无关, 后续写入/压缩/重启均不改变结果。
    """
    with get_conn() as conn:
        snap = conn.execute(
            "SELECT id, anchor_seq, end_seq FROM export_snapshots WHERE id = ?",
            (export_id,),
        ).fetchone()
        # 未知或已删除的快照统一 404
        if snap is None:
            return JSONResponse(status_code=404, content={"detail": "export not found"})

        start = snap["anchor_seq"] if after_seq is None else after_seq
        if start < snap["anchor_seq"] or start > snap["end_seq"]:
            return JSONResponse(
                status_code=422,
                content={
                    "detail": "after_seq out of snapshot range",
                    "after_seq": start,
                    "anchor_seq": snap["anchor_seq"],
                    "end_seq": snap["end_seq"],
                },
            )

        # 多取一条判断是否还有下一页
        rows = conn.execute(
            "SELECT seq, event_id, occurred_at, payload, received_at, prev_hash, hash"
            " FROM export_events WHERE snapshot_id = ? AND seq > ?"
            " ORDER BY seq LIMIT ?",
            (export_id, start, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    rows = rows[:limit]
    return {
        "export_id": export_id,
        "after_seq": start,
        "limit": limit,
        "count": len(rows),
        "events": [row_to_export_event(r) for r in rows],
        # 末页(含空页)为 null
        "next_after_seq": rows[-1]["seq"] if has_more else None,
    }


@app.delete("/exports/{export_id}")
def delete_export(export_id: int = Path(...)):
    """删除快照: 同一事务内原子删除元数据与全部事件副本。

    未知或已删除的快照统一返回 404, 状态不变。
    """
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        snap = conn.execute(
            "SELECT id FROM export_snapshots WHERE id = ?", (export_id,)
        ).fetchone()
        # 未知或已删除统一 404
        if snap is None:
            return JSONResponse(status_code=404, content={"detail": "export not found"})
        conn.execute("DELETE FROM export_events WHERE snapshot_id = ?", (export_id,))
        conn.execute("DELETE FROM export_snapshots WHERE id = ?", (export_id,))
    return {"export_id": export_id, "deleted": True}
