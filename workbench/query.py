"""查询层：按口径读聚合 + 覆盖率报告 + 旧链接兼容。

完整性语义（两层分开，互不掩盖）：
- 计算覆盖：某区间是否已被 done 段或入仓双写覆盖（决定 pending）。
- 数据可恢复性：某区间是否存在原始事件（决定 unrecoverable）。
  即使段已「成功」重算，只要该区间没有原始事件而其他口径有聚合，
  就必须标 unrecoverable —— 段跑过不等于数据能恢复，绝不把缺失静默当 0。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from . import rules, storage

FAR_FUTURE = datetime(9999, 1, 1, tzinfo=rules.UTC)


def _merge(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    out: list[list[datetime]] = []
    for s, e in sorted(intervals):
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return [(s, e) for s, e in out]


def _subtract(start: datetime, end: datetime,
              covered: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    gaps, cur = [], start
    for s, e in _merge(covered):
        if e <= cur:
            continue
        if s > cur:
            gaps.append((cur, min(s, end)))
        cur = max(cur, e)
        if cur >= end:
            break
    if cur < end:
        gaps.append((cur, end))
    return gaps


def _has_events(conn, site_id, s: datetime, e: datetime) -> bool:
    return conn.execute(
        "SELECT 1 FROM events WHERE site_id=? AND quarantined=0"
        " AND ts_utc>=? AND ts_utc<? LIMIT 1",
        (site_id, rules.iso(s), rules.iso(e))).fetchone() is not None


def _other_agg_intervals(conn, site_id, rule_version,
                         s: datetime, e: datetime) -> list[tuple[datetime, datetime]]:
    return [(rules.parse_ts(r["start_utc"]), rules.parse_ts(r["end_utc"]))
            for r in conn.execute(
                "SELECT DISTINCT b.start_utc, b.end_utc FROM buckets b"
                " WHERE b.site_id=? AND b.rule_version<>? AND b.start_utc<? AND b.end_utc>?",
                (site_id, rule_version, rules.iso(e), rules.iso(s)))]


def coverage(conn: sqlite3.Connection, site_id: str, rule_version: str,
             start: datetime, end: datetime) -> dict:
    """覆盖率报告：pending（可重算未重算）/ unrecoverable（无原始事件）/ empty。"""
    covered = [(rules.parse_ts(r["range_start_utc"]), rules.parse_ts(r["range_end_utc"]))
               for r in conn.execute(
                   "SELECT range_start_utc, range_end_utc FROM segments"
                   " WHERE rule_version=? AND site_id=? AND status='done'",
                   (rule_version, site_id))]
    row = conn.execute("SELECT activated_utc FROM rule_versions WHERE rule_version=?",
                       (rule_version,)).fetchone()
    if row is None:
        raise rules.RuleError(f"unknown rule version: {rule_version!r}")
    covered.append((rules.parse_ts(row["activated_utc"]), FAR_FUTURE))

    pending, unrecoverable, n_empty = [], [], 0
    for gs, ge in _subtract(start, end, covered):
        if _has_events(conn, site_id, gs, ge):
            pending.append((gs, ge))
        elif any(s < ge and e > gs for s, e in
                 _other_agg_intervals(conn, site_id, rule_version, gs, ge)):
            unrecoverable.append((gs, ge))
        else:
            n_empty += 1

    # 数据层：其他口径的桶，其【完整区间】内没有任何原始事件 → 该桶的数据
    # 无法从 events 重算（典型：旧仓导入）。桶与查询区间的交集标 unrecoverable。
    # 注意判据是桶全区间而非裁剪后的窗口 —— 窗口内无事件只是正常的空档。
    for bs, be in _other_agg_intervals(conn, site_id, rule_version, start, end):
        if not _has_events(conn, site_id, bs, be):
            cs, ce = max(bs, start), min(be, end)
            if cs < ce:
                unrecoverable.append((cs, ce))

    fmt = lambda iv: [{"start_utc": rules.iso(s), "end_utc": rules.iso(e)}
                      for s, e in _merge(iv)]
    pending_l, unrec_l = fmt(pending), fmt(unrecoverable)
    return {"status": "complete" if not pending_l and not unrec_l else "partial",
            "pending": pending_l, "unrecoverable": unrec_l, "empty_ranges": n_empty}


def query(conn: sqlite3.Connection, site_id: str, rule_version: str,
          start: str, end: str, metric: str | None = None) -> dict:
    s, e = rules.parse_ts(start), rules.parse_ts(end)
    if e <= s:
        raise rules.RuleError("query range must be positive")
    params: list = [rule_version, site_id, rules.iso(e), rules.iso(s)]
    sql = ("SELECT a.bucket_label, a.metric, a.count, a.sum, a.min, a.max,"
           " b.start_utc, b.end_utc FROM aggregates a"
           " JOIN buckets b ON a.rule_version=b.rule_version AND a.site_id=b.site_id"
           " AND a.bucket_label=b.bucket_label"
           " WHERE a.rule_version=? AND a.site_id=? AND b.start_utc<? AND b.end_utc>?")
    if metric:
        sql += " AND a.metric=?"
        params.append(metric)
    sql += " ORDER BY b.start_utc, a.bucket_label, a.metric"
    buckets: dict[str, dict] = {}
    for r in conn.execute(sql, params):
        b = buckets.setdefault(r["bucket_label"], {
            "bucket_label": r["bucket_label"], "start_utc": r["start_utc"],
            "end_utc": r["end_utc"], "metrics": {}})
        b["metrics"][r["metric"]] = {"count": r["count"], "sum": r["sum"],
                                     "min": r["min"], "max": r["max"]}
    cov = coverage(conn, site_id, rule_version, s, e)
    return {"site_id": site_id, "rule_version": rule_version,
            "range": {"start_utc": rules.iso(s), "end_utc": rules.iso(e)},
            "buckets": list(buckets.values()), "coverage": cov}


def legacy_hourly(conn: sqlite3.Connection, site_id: str, date_str: str,
                  hour: int) -> dict:
    """旧查询链接兼容层：/api/legacy/hourly?site=&date=&hour=

    永远读 v1 聚合，语义与迁移前完全一致。fall-back 当天同一本地小时
    可能返回两个桶（偏移不同），如实返回而不是合并。
    """
    if not 0 <= hour <= 23:
        raise rules.RuleError("hour must be 0..23")
    prefix = f"hour:{date_str}T{hour:02d}:"
    rows = conn.execute(
        "SELECT a.bucket_label, a.metric, a.count, a.sum, a.min, a.max,"
        " b.start_utc, b.end_utc FROM aggregates a"
        " JOIN buckets b ON a.rule_version=b.rule_version AND a.site_id=b.site_id"
        " AND a.bucket_label=b.bucket_label"
        " WHERE a.rule_version='v1' AND a.site_id=? AND a.bucket_label LIKE ? || '%'"
        " ORDER BY b.start_utc", (site_id, prefix)).fetchall()
    buckets: dict[str, dict] = {}
    for r in rows:
        b = buckets.setdefault(r["bucket_label"], {
            "bucket_label": r["bucket_label"], "start_utc": r["start_utc"],
            "end_utc": r["end_utc"], "metrics": {}})
        b["metrics"][r["metric"]] = {"count": r["count"], "sum": r["sum"],
                                     "min": r["min"], "max": r["max"]}
    return {"site_id": site_id, "rule_version": "v1", "date": date_str, "hour": hour,
            "buckets": list(buckets.values())}


def attribute(conn: sqlite3.Connection, site_id: str, ts: str) -> dict:
    """事件归属查看器：同一瞬间在各规则版本下分别落入哪个桶。"""
    t = rules.parse_ts(ts)
    tz = storage.site_tz(conn, site_id)
    out = []
    for r in storage.list_rule_versions(conn):
        rule = storage.load_rule(conn, r["rule_version"])
        b = rules.assign(rule, tz, t)
        out.append({"rule_version": rule.rule_version, "kind": rule.kind,
                    "bucket_label": b.label,
                    "start_utc": rules.iso(b.start_utc), "end_utc": rules.iso(b.end_utc),
                    "duration_hours": round((b.end_utc - b.start_utc)
                                            / timedelta(hours=1), 4)})
    return {"site_id": site_id, "tz": tz, "ts_utc": rules.iso(t), "attributions": out}
