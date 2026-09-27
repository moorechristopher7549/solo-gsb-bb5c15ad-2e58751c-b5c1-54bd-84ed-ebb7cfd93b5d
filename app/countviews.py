"""事件日计数视图: 可选的 payload 顶层字符串字段等值规则 + 按 occurred_at 的 UTC 日期分桶。

计数只反映当前 events 表内的现存事件:
- 视图创建时在调用方的写事务内回填现存事件;
- 单条/批量接收仅对真正新建的事件 +1(重复 event_id 由调用方判重, 不进入这里);
- 压缩归档物理删除事件时在同一事务内 -1, 归零的空桶立即移除;
- 桶按 occurred_at(已归一化为 UTC ISO8601)的 UTC 日期划分, 迟到事件自然归入其发生日。
"""
import json
import sqlite3
from typing import Any, Optional

# 规则规模上限, 防止单条请求/单事务无界膨胀
MAX_MATCH_FIELDS = 20
MAX_FIELD_NAME_LEN = 255
MAX_FIELD_VALUE_LEN = 1000


def event_matches(payload: Any, rules: dict[str, str]) -> bool:
    """事件是否命中视图规则。

    空规则匹配全部事件; 否则要求 payload 为 JSON 对象, 且每个规则字段都是
    顶层"字符串字段 = 字符串值"的全等匹配——字段缺失或值不是字符串都不命中。
    """
    if not rules:
        return True
    if not isinstance(payload, dict):
        return False
    for field, expected in rules.items():
        value = payload.get(field)
        if not isinstance(value, str) or value != expected:
            return False
    return True


def utc_day(occurred_at: str) -> str:
    """occurred_at 已归一化为 UTC ISO8601, 前 10 字符即 UTC 日期(YYYY-MM-DD)。"""
    return occurred_at[:10]


def load_views(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """读取全部视图规则(事件写入/压缩事务内调用, 视图数量通常很少)。"""
    return conn.execute("SELECT id, match_rules FROM count_views").fetchall()


def apply_events(
    conn: sqlite3.Connection,
    views: list[sqlite3.Row],
    events: list[tuple[str, Any]],
    delta: int,
) -> None:
    """在调用方持有的写事务内, 对一批事件更新全部视图的日计数。

    events 为 (occurred_at, payload[已解析的 Python 对象]) 列表;
    delta=+1 用于新建事件/创建回填, delta=-1 用于压缩归档删除。
    计数先按 (视图, UTC 日期) 聚合, 再每桶一次 upsert; 扣减后归零(含异常转负)
    的桶立即删除, 保证读视图时看不到空桶。
    """
    if not views or not events:
        return
    parsed = [(v["id"], json.loads(v["match_rules"])) for v in views]
    # view_id -> {day: 本批命中数}
    per_view: dict[int, dict[str, int]] = {}
    for occurred_at, payload in events:
        day = utc_day(occurred_at)
        for view_id, rules in parsed:
            if event_matches(payload, rules):
                buckets = per_view.setdefault(view_id, {})
                buckets[day] = buckets.get(day, 0) + 1

    for view_id, buckets in per_view.items():
        for day, n in buckets.items():
            signed = delta * n
            conn.execute(
                "INSERT INTO count_view_days (view_id, day, count) VALUES (?, ?, ?)"
                " ON CONFLICT(view_id, day) DO UPDATE SET"
                " count = count_view_days.count + excluded.count",
                (view_id, day, signed),
            )
        if delta < 0:
            # 删除事件后计数归零的桶移除(<=0 同时兜底任何不自洽状态)
            conn.execute(
                "DELETE FROM count_view_days WHERE view_id = ? AND count <= 0",
                (view_id,),
            )


def backfill_view(
    conn: sqlite3.Connection, view_id: int, rules: dict[str, str]
) -> None:
    """创建视图时在同一写事务内回填当前 events 表内全部匹配事件的日计数。"""
    rows = conn.execute("SELECT occurred_at, payload FROM events").fetchall()
    events = [(r["occurred_at"], json.loads(r["payload"])) for r in rows]
    # 用普通 dict 模拟 sqlite3.Row 的字段访问, 复用同一条 upsert 路径
    apply_events(conn, [{"id": view_id, "match_rules": json.dumps(rules)}], events, 1)


def get_view(conn: sqlite3.Connection, name: str) -> Optional[dict[str, Any]]:
    """按名称读取视图规则与各 UTC 日计数(按日期升序)及总数; 不存在返回 None。"""
    view = conn.execute(
        "SELECT id, name, match_rules, created_at FROM count_views WHERE name = ?",
        (name,),
    ).fetchone()
    if view is None:
        return None
    rows = conn.execute(
        "SELECT day, count FROM count_view_days WHERE view_id = ? ORDER BY day",
        (view["id"],),
    ).fetchall()
    days = [{"day": r["day"], "count": r["count"]} for r in rows]
    return {
        "name": view["name"],
        "matches": json.loads(view["match_rules"]),
        "created_at": view["created_at"],
        "days": days,
        "total": sum(d["count"] for d in days),
    }
