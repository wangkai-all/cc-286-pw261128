"""持续入仓：对每个事件按所有 active 规则版本原子双写聚合。

幂等：event_id 为主键，重复投递只计一次（事件行未插入则不加聚合）。
"""
from __future__ import annotations

import sqlite3

from . import rules, storage

_UPSERT_AGG = """
INSERT INTO aggregates(rule_version, site_id, bucket_label, metric, count, sum, min, max)
VALUES (?,?,?,?,1,?,?,?)
ON CONFLICT(rule_version, site_id, bucket_label, metric) DO UPDATE SET
  count = count + 1,
  sum   = sum + excluded.sum,
  min   = MIN(min, excluded.min),
  max   = MAX(max, excluded.max)
"""


def apply_one(conn: sqlite3.Connection, rule_version: str, site_id: str,
              bucket: rules.BucketRef, metric: str, value: float) -> None:
    """把一条事件计入某规则版本下的一个桶（调用方须持有写事务）。"""
    conn.execute(
        "INSERT OR IGNORE INTO buckets(rule_version, site_id, bucket_label, start_utc, end_utc)"
        " VALUES (?,?,?,?,?)",
        (rule_version, site_id, bucket.label,
         rules.iso(bucket.start_utc), rules.iso(bucket.end_utc)))
    conn.execute(_UPSERT_AGG,
                 (rule_version, site_id, bucket.label, metric, value, value, value))


def ingest_events(conn: sqlite3.Connection, events: list[dict]) -> dict:
    """批量入仓。返回 {inserted, duplicated, rejected}。

    时间戳非法的事件被拒绝（不入库）；调用方若想模拟坏上游直写，
    应使用 synth.inject_bad_event 而不是本函数。
    """
    active = [storage.load_rule(conn, r["rule_version"])
              for r in storage.list_rule_versions(conn, active_only=True)]
    inserted = duplicated = 0
    rejected: list[dict] = []
    with storage.write_txn(conn):
        for e in events:
            eid = e.get("event_id")
            try:
                ts = rules.parse_ts(e["ts_utc"])
                tz = storage.site_tz(conn, e["site_id"])
            except rules.RuleError as exc:
                rejected.append({"event_id": eid, "error": str(exc)})
                continue
            cur = conn.execute(
                "INSERT OR IGNORE INTO events(event_id, site_id, device_id, ts_utc, metric, value)"
                " VALUES (?,?,?,?,?,?)",
                (eid, e["site_id"], e.get("device_id", "unknown"),
                 rules.iso(ts), e.get("metric", "count"), float(e.get("value", 1.0))))
            if cur.rowcount == 0:
                duplicated += 1
                continue
            inserted += 1
            for rule in active:
                bucket = rules.assign(rule, tz, ts)
                apply_one(conn, rule.rule_version, e["site_id"], bucket,
                          e.get("metric", "count"), float(e.get("value", 1.0)))
    return {"inserted": inserted, "duplicated": duplicated, "rejected": rejected}
