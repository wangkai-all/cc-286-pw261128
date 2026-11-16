"""迁移引擎：把历史区间按段重算到某规则版本。

设计要点：
- 段(segment)是调度与失败恢复的单位；桶(bucket)是重算的原子单位。
  段边界可以任意切（默认 6h），重算时先找出与段相交的所有桶，
  再对【整个桶】按 events 全量重算 —— 跨段边界的桶（如跨午夜班次
  被段切成两半）也不会算错，且重复执行结果不变（幂等）。
- 失败（如事件时间戳损坏）只影响当前段：状态置 failed、记录错误，
  修复数据后 retry 即可，其余段不受影响。
- 重算只写本规则版本的 buckets/aggregates，绝不触碰其他版本 ——
  旧口径查询在迁移期间读到的是始终完整的旧聚合。
"""
from __future__ import annotations

import sqlite3
from datetime import timedelta

from . import rules, storage


class SegmentError(Exception):
    pass


def create_job(conn: sqlite3.Connection, rule_version: str, site_id: str,
               start: str, end: str, segment_minutes: int = 360) -> int:
    s, e = rules.parse_ts(start), rules.parse_ts(end)
    if e <= s:
        raise rules.RuleError("job range must be positive")
    if segment_minutes <= 0:
        raise rules.RuleError("segment_minutes must be positive")
    storage.load_rule(conn, rule_version)  # 版本必须存在
    storage.site_tz(conn, site_id)         # 站点必须存在
    with storage.write_txn(conn):
        cur = conn.execute(
            "INSERT INTO jobs(rule_version, site_id, range_start_utc, range_end_utc,"
            " segment_minutes, created_utc) VALUES (?,?,?,?,?,?)",
            (rule_version, site_id, rules.iso(s), rules.iso(e), segment_minutes,
             storage.utcnow_iso()))
        job_id = cur.lastrowid
        t = s
        while t < e:
            t2 = min(t + timedelta(minutes=segment_minutes), e)
            conn.execute(
                "INSERT INTO segments(job_id, rule_version, site_id, range_start_utc,"
                " range_end_utc) VALUES (?,?,?,?,?)",
                (job_id, rule_version, site_id, rules.iso(t), rules.iso(t2)))
            t = t2
    return job_id


def recompute_segment(conn: sqlite3.Connection, seg: sqlite3.Row) -> dict:
    """重算一个段。抛异常即段失败（由调用方记录）。返回统计。"""
    rule = storage.load_rule(conn, seg["rule_version"])
    tz = storage.site_tz(conn, seg["site_id"])
    site_id = seg["site_id"]
    s_iso, e_iso = seg["range_start_utc"], seg["range_end_utc"]

    rows = conn.execute(
        "SELECT event_id, ts_utc FROM events"
        " WHERE site_id=? AND quarantined=0 AND ts_utc>=? AND ts_utc<?",
        (site_id, s_iso, e_iso)).fetchall()
    bad = [r["event_id"] for r in rows if _unparseable(r["ts_utc"])]
    if bad:
        raise SegmentError(f"events with unparseable ts_utc: {bad}; "
                           "fix via /api/events/fix then retry the segment")

    # 1) 段内事件 -> 受影响的桶（可能超出段界，如跨午夜班次）
    affected: dict[str, rules.BucketRef] = {}
    for r in rows:
        b = rules.assign(rule, tz, rules.parse_ts(r["ts_utc"]))
        affected[b.label] = b

    # 2) 写事务内以整桶为单位 delete + 从 events 全量重算
    with storage.write_txn(conn):
        for label in affected:
            conn.execute("DELETE FROM aggregates WHERE rule_version=? AND site_id=?"
                         " AND bucket_label=?", (rule.rule_version, site_id, label))
            conn.execute("DELETE FROM buckets WHERE rule_version=? AND site_id=?"
                         " AND bucket_label=?", (rule.rule_version, site_id, label))
        n_events = 0
        for label, b in affected.items():
            evs = conn.execute(
                "SELECT metric, value, ts_utc FROM events"
                " WHERE site_id=? AND quarantined=0 AND ts_utc>=? AND ts_utc<?",
                (site_id, rules.iso(b.start_utc), rules.iso(b.end_utc))).fetchall()
            bad2 = [i for i, r in enumerate(evs) if _unparseable(r["ts_utc"])]
            if bad2:
                raise SegmentError(f"unparseable ts_utc inside bucket {label!r}")
            stats: dict[str, dict] = {}
            for r in evs:
                st = stats.setdefault(r["metric"], {"count": 0, "sum": 0.0,
                                                    "min": None, "max": None})
                v = float(r["value"])
                st["count"] += 1
                st["sum"] += v
                st["min"] = v if st["min"] is None else min(st["min"], v)
                st["max"] = v if st["max"] is None else max(st["max"], v)
                n_events += 1
            if not stats:
                continue  # 桶内已无有效事件（例如事件被隔离），保持删除状态
            conn.execute(
                "INSERT OR REPLACE INTO buckets(rule_version, site_id, bucket_label,"
                " start_utc, end_utc) VALUES (?,?,?,?,?)",
                (rule.rule_version, site_id, label,
                 rules.iso(b.start_utc), rules.iso(b.end_utc)))
            for metric, st in stats.items():
                conn.execute(
                    "INSERT INTO aggregates(rule_version, site_id, bucket_label, metric,"
                    " count, sum, min, max) VALUES (?,?,?,?,?,?,?,?)",
                    (rule.rule_version, site_id, label, metric,
                     st["count"], st["sum"], st["min"], st["max"]))
    return {"buckets": len(affected), "events": n_events}


def _unparseable(ts) -> bool:
    if ts is None:
        return True
    try:
        rules.parse_ts(ts)
        return False
    except rules.RuleError:
        return True


def run_pending_segments(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    """处理待办段（worker 循环与手动触发共用）。返回每段结果。"""
    due = conn.execute(
        "SELECT * FROM segments WHERE status='pending' ORDER BY segment_id LIMIT ?",
        (limit,)).fetchall()
    outcomes = []
    for seg in due:
        with storage.write_txn(conn):
            conn.execute("UPDATE segments SET status='running', attempts=attempts+1,"
                         " updated_utc=? WHERE segment_id=?",
                         (storage.utcnow_iso(), seg["segment_id"]))
        try:
            stats = recompute_segment(conn, conn.execute(
                "SELECT * FROM segments WHERE segment_id=?",
                (seg["segment_id"],)).fetchone())
        except Exception as exc:  # 段失败不影响其他段
            with storage.write_txn(conn):
                conn.execute("UPDATE segments SET status='failed', last_error=?,"
                             " updated_utc=? WHERE segment_id=?",
                             (str(exc), storage.utcnow_iso(), seg["segment_id"]))
            outcomes.append({"segment_id": seg["segment_id"], "status": "failed",
                             "error": str(exc)})
        else:
            with storage.write_txn(conn):
                conn.execute("UPDATE segments SET status='done', last_error=NULL,"
                             " updated_utc=? WHERE segment_id=?",
                             (storage.utcnow_iso(), seg["segment_id"]))
            outcomes.append({"segment_id": seg["segment_id"], "status": "done", **stats})
    return outcomes


def retry_segment(conn: sqlite3.Connection, segment_id: int) -> None:
    with storage.write_txn(conn):
        cur = conn.execute(
            "UPDATE segments SET status='pending', last_error=NULL, updated_utc=?"
            " WHERE segment_id=? AND status IN ('failed','running')",
            (storage.utcnow_iso(), segment_id))
        if cur.rowcount == 0:
            raise rules.RuleError(f"segment {segment_id} is not in failed/running state")


def _invalidate_segments(conn: sqlite3.Connection, site_id: str,
                         ts_strings: list[str]) -> list[int]:
    """数据修复后，把包含相关时间点的已完成段重置为 pending。

    重算以段为单位推进；事件被改正/隔离后，凡区间覆盖其新旧位置的
    done 段都必须重算，否则这些段里的桶会静默地保持旧值。
    """
    affected: set[int] = set()
    for ts in ts_strings:
        if not ts:
            continue
        for r in conn.execute(
                "SELECT segment_id FROM segments WHERE site_id=? AND status='done'"
                " AND source='recompute' AND range_start_utc<=? AND range_end_utc>?",
                (site_id, ts, ts)).fetchall():
            affected.add(r["segment_id"])
    for sid in affected:
        conn.execute("UPDATE segments SET status='pending', updated_utc=?"
                     " WHERE segment_id=?", (storage.utcnow_iso(), sid))
    return sorted(affected)


def fix_event(conn: sqlite3.Connection, event_id: str, ts_utc: str | None) -> dict:
    """修复坏事件：给出合法 ts_utc 则改正，传 None 则隔离（不再参与重算）。

    无论哪种方式，都会使覆盖相关时间点的已完成段失效（转 pending），
    由调用方/worker 重新执行。
    """
    with storage.write_txn(conn):
        row = conn.execute("SELECT site_id, ts_utc FROM events WHERE event_id=?",
                           (event_id,)).fetchone()
        if row is None:
            raise rules.RuleError(f"unknown event: {event_id!r}")
        old_ts = row["ts_utc"]
        if ts_utc is None:
            conn.execute("UPDATE events SET quarantined=1 WHERE event_id=?", (event_id,))
            inv = _invalidate_segments(conn, row["site_id"], [old_ts])
            return {"event_id": event_id, "action": "quarantined",
                    "invalidated_segments": inv}
        ts = rules.parse_ts(ts_utc)  # 非法则抛错，不改库
        new_iso = rules.iso(ts)
        conn.execute("UPDATE events SET ts_utc=? WHERE event_id=?", (new_iso, event_id))
        inv = _invalidate_segments(conn, row["site_id"], [old_ts, new_iso])
        return {"event_id": event_id, "action": "repaired", "ts_utc": new_iso,
                "invalidated_segments": inv}


def list_jobs(conn: sqlite3.Connection) -> list[dict]:
    jobs = [dict(r) for r in conn.execute("SELECT * FROM jobs ORDER BY job_id DESC")]
    for j in jobs:
        segs = conn.execute(
            "SELECT status, COUNT(*) c FROM segments WHERE job_id=? GROUP BY status",
            (j["job_id"],)).fetchall()
        counts = {r["status"]: r["c"] for r in segs}
        j["segments"] = counts
        if counts.get("failed"):
            j["status"] = "failed"
        elif counts.get("pending") or counts.get("running"):
            j["status"] = "running"
        else:
            j["status"] = "done"
    return jobs


def list_segments(conn: sqlite3.Connection, job_id: int | None = None) -> list[dict]:
    if job_id is None:
        rows = conn.execute("SELECT * FROM segments ORDER BY segment_id DESC LIMIT 200")
    else:
        rows = conn.execute("SELECT * FROM segments WHERE job_id=? ORDER BY segment_id",
                            (job_id,))
    return [dict(r) for r in rows]
