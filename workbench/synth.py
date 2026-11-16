"""可核对的合成事件：确定性生成 + 独立暴力对账。

- 手工放置的边界事件覆盖：spring-forward（不存在的小时）、fall-back
  （重复的小时）、班次边界 ±1 分钟、跨午夜班次归属。
- 随机填充用固定种子，可重复。
- verify() 用一条独立路径（从 events 表逐条重算）与 aggregates 表对账，
  任何不一致都会逐桶列出。
"""
from __future__ import annotations

import random
import sqlite3
from datetime import datetime, timedelta

from . import rules, storage

METRIC = "power_kw"

# ---- 手工边界事件（UTC 时刻 + 说明）----
_EDGE_NY = [
    # 2026-03-08 spring forward：02:00 本地不存在，01:59 之后直接 03:00
    ("2026-03-08T06:59:30Z", "spring: 跳变前最后一小时(01:00 EST)内"),
    ("2026-03-08T07:00:30Z", "spring: 跳变后(03:00 EDT)，不存在 02:xx"),
    ("2026-03-08T07:30:00Z", "spring: 03:00 桶"),
    # 2026-11-01 fall back：01:30 本地发生两次（EDT 与 EST）
    ("2026-11-01T05:30:00Z", "fall: 第一个 01:30 (EDT, -04:00)"),
    ("2026-11-01T06:30:00Z", "fall: 第二个 01:30 (EST, -05:00)"),
    # fall-back 当夜的跨午夜班次 22:00->06:00 真实时长 9 小时
    ("2026-10-31T22:30:00Z", "fall前夜: 归 2026-10-31 夜班"),
    ("2026-11-01T09:30:00Z", "fall夜: 04:30 EST，仍归 2026-10-31 夜班"),
    # 班次边界 ±1 分钟（班次 06-14 / 14-22 / 22-06，EST = UTC-5）
    ("2026-11-15T10:59:00Z", "边界: 05:59 夜班最后一分钟"),
    ("2026-11-15T11:00:00Z", "边界: 06:00 早班开始"),
    ("2026-11-15T18:59:00Z", "边界: 13:59 早班最后一分钟"),
    ("2026-11-15T19:00:00Z", "边界: 14:00 中班开始"),
    ("2026-11-16T02:59:00Z", "边界: 21:59 中班最后一分钟"),
    ("2026-11-16T03:00:00Z", "边界: 22:00 夜班开始(归 11-15 营业日)"),
    ("2026-11-16T06:30:00Z", "跨午夜: 01:30 仍归 11-15 夜班"),
    ("2026-11-16T10:30:00Z", "跨午夜: 05:30 仍归 11-15 夜班"),
]

_EDGE_SH = [
    ("2026-11-15T21:59:00Z", "上海: 05:59 夜班末尾"),
    ("2026-11-15T22:00:00Z", "上海: 06:00 早班开始"),
    ("2026-11-16T13:59:00Z", "上海: 21:59 中班末尾"),
    ("2026-11-16T14:00:00Z", "上海: 22:00 夜班开始(归 11-16)"),
    ("2026-11-16T17:30:00Z", "上海: 次日 01:30 仍归 11-16 夜班"),
]


def synth_events() -> list[dict]:
    """确定性合成事件集（固定种子），供演示与核对。"""
    events: list[dict] = []
    n = 0

    def add(site, device, ts, value, note=""):
        nonlocal n
        n += 1
        events.append({"event_id": f"evt-{n:04d}", "site_id": site,
                       "device_id": device, "ts_utc": ts,
                       "metric": METRIC, "value": value, "note": note})

    for i, (ts, note) in enumerate(_EDGE_NY):
        add("site-ny", f"dev-ny-{i % 3 + 1}", ts, 10.0 + i, note)
    for i, (ts, note) in enumerate(_EDGE_SH):
        add("site-sh", f"dev-sh-{i % 2 + 1}", ts, 20.0 + i, note)

    rng = random.Random(42)
    base = datetime(2026, 11, 10, tzinfo=rules.UTC)
    for site, devs in (("site-ny", 3), ("site-sh", 2)):
        for _ in range(120):
            ts = base + timedelta(minutes=rng.randrange(0, 7 * 24 * 60))
            add(site, f"dev-{site[-2:]}-{rng.randrange(1, devs + 1)}",
                rules.iso(ts), round(rng.uniform(1, 100), 2))
    return events


def inject_bad_event(conn: sqlite3.Connection, site_id: str = "site-ny") -> str:
    """模拟坏上游直写：时间戳非法的事件绕过入仓校验进入 events 表，
    使覆盖该区间的迁移段失败，用于演示失败段的修复与恢复。"""
    event_id = "evt-bad-0001"
    with storage.write_txn(conn):
        conn.execute(
            "INSERT OR REPLACE INTO events(event_id, site_id, device_id, ts_utc,"
            " metric, value) VALUES (?,?,?,?,?,?)",
            (event_id, site_id, "dev-ny-9", "2026-11-15T25:61:00Z", METRIC, 999.0))
    return event_id


def legacy_import(conn: sqlite3.Connection, site_id: str,
                  start: str, end: str) -> dict:
    """模拟旧数仓：只有 v1 小时聚合、没有原始事件的历史区间。

    直接写 v1 聚合（count/sum 有值，min/max 为 NULL —— 旧汇总本来就没存），
    并登记一段 source='legacy-import' 的 done 段，表示 v1 口径在该区间完整。
    这个区间对新口径就是 unrecoverable：原始事件不存在，无法精确重算。
    """
    s, e = rules.parse_ts(start), rules.parse_ts(end)
    tz = storage.site_tz(conn, site_id)
    rng = random.Random(7)
    n = 0
    with storage.write_txn(conn):
        t = s
        while t < e:
            b = rules.assign(rules.RuleVersion.local_hour(), tz, t)
            label = b.label
            conn.execute(
                "INSERT OR IGNORE INTO buckets(rule_version, site_id, bucket_label,"
                " start_utc, end_utc) VALUES ('v1',?,?,?,?)",
                (site_id, label, rules.iso(b.start_utc), rules.iso(b.end_utc)))
            count = rng.randrange(3, 30)
            total = round(rng.uniform(50, 500), 2)
            conn.execute(
                "INSERT OR REPLACE INTO aggregates(rule_version, site_id, bucket_label,"
                " metric, count, sum, min, max) VALUES ('v1',?,?,?,?,?,NULL,NULL)",
                (site_id, label, METRIC, count, total))
            n += 1
            t = b.end_utc
        conn.execute(
            "INSERT INTO segments(job_id, rule_version, site_id, range_start_utc,"
            " range_end_utc, status, source, updated_utc)"
            " VALUES (NULL,'v1',?,?,?,'done','legacy-import',?)",
            (site_id, rules.iso(s), rules.iso(e), storage.utcnow_iso()))
    return {"legacy_hours": n}


def brute_force(events: list[dict], rule: rules.RuleVersion, tz: str) -> dict:
    """独立重算：纯 Python 逐事件分桶聚合。作为对账的参照系。"""
    out: dict[str, dict] = {}
    for e in events:
        if e.get("quarantined"):
            continue
        b = rules.assign(rule, tz, rules.parse_ts(e["ts_utc"]))
        m = out.setdefault(b.label, {}).setdefault(
            e["metric"], {"count": 0, "sum": 0.0, "min": None, "max": None})
        v = float(e["value"])
        m["count"] += 1
        m["sum"] += v
        m["min"] = v if m["min"] is None else min(m["min"], v)
        m["max"] = v if m["max"] is None else max(m["max"], v)
    return out


def verify(conn: sqlite3.Connection, site_id: str, rule_version: str,
           start: str | None = None, end: str | None = None) -> dict:
    """对账：events 表独立重算 vs aggregates 表。返回差异列表（空=一致）。

    可选区间过滤：只比较【完全落在区间内】的桶（跨区间边界的桶两边都排除，
    避免半桶误判），事件按区间过滤后重算。
    """
    rule = storage.load_rule(conn, rule_version)
    tz = storage.site_tz(conn, site_id)
    events = [dict(r) for r in conn.execute(
        "SELECT event_id, ts_utc, metric, value, quarantined FROM events"
        " WHERE site_id=?", (site_id,))]
    events = [e for e in events if not _bad_ts(e["ts_utc"])]
    lo = rules.parse_ts(start) if start else None
    hi = rules.parse_ts(end) if end else None
    if lo or hi:
        events = [e for e in events
                  if (lo is None or rules.parse_ts(e["ts_utc"]) >= lo)
                  and (hi is None or rules.parse_ts(e["ts_utc"]) < hi)]
    expected = brute_force(events, rule, tz)

    actual: dict[str, dict] = {}
    bounds: dict[str, tuple] = {}
    for r in conn.execute(
            "SELECT a.bucket_label, a.metric, a.count, a.sum, a.min, a.max,"
            " b.start_utc, b.end_utc FROM aggregates a JOIN buckets b"
            " ON a.rule_version=b.rule_version AND a.site_id=b.site_id"
            " AND a.bucket_label=b.bucket_label"
            " WHERE a.rule_version=? AND a.site_id=?", (rule_version, site_id)):
        actual.setdefault(r["bucket_label"], {})[r["metric"]] = {
            "count": r["count"], "sum": r["sum"], "min": r["min"], "max": r["max"]}
        bounds[r["bucket_label"]] = (rules.parse_ts(r["start_utc"]),
                                     rules.parse_ts(r["end_utc"]))

    def inside(label) -> bool:
        bs, be = bounds.get(label, (None, None))
        if bs is None:  # expected 里的桶：用规则重算边界
            return True
        return (lo is None or bs >= lo) and (hi is None or be <= hi)

    expected = {k: v for k, v in expected.items() if inside(k)}
    actual = {k: v for k, v in actual.items() if inside(k)}

    # legacy-import 区间内的桶是旧仓数据（本来就没有原始事件），
    # 不属于「events 可核对」范围：单独列出，不参与一致性判定。
    legacy_ranges = [(rules.parse_ts(r["range_start_utc"]),
                      rules.parse_ts(r["range_end_utc"]))
                     for r in conn.execute(
                         "SELECT range_start_utc, range_end_utc FROM segments"
                         " WHERE site_id=? AND rule_version=? AND source='legacy-import'"
                         " AND status='done'", (site_id, rule_version))]

    def in_legacy(label) -> bool:
        bs, be = bounds.get(label, (None, None))
        if bs is None:
            return False
        return any(ls <= bs and be <= le for ls, le in legacy_ranges)

    legacy_only = sorted(k for k in actual if in_legacy(k))
    actual = {k: v for k, v in actual.items() if not in_legacy(k)}

    diffs = []
    for label in sorted(set(expected) | set(actual)):
        e, a = expected.get(label, {}), actual.get(label, {})
        for metric in sorted(set(e) | set(a)):
            em, am = e.get(metric), a.get(metric)
            if em is None:
                diffs.append({"bucket": label, "metric": metric,
                              "issue": "db 有而 events 重算没有", "db": am})
            elif am is None:
                diffs.append({"bucket": label, "metric": metric,
                              "issue": "events 重算有而 db 缺失", "expected": em})
            elif (em["count"] != am["count"]
                  or abs(em["sum"] - am["sum"]) > 1e-6
                  or not _close(em["min"], am["min"]) or not _close(em["max"], am["max"])):
                diffs.append({"bucket": label, "metric": metric,
                              "issue": "数值不一致", "expected": em, "db": am})
    return {"site_id": site_id, "rule_version": rule_version,
            "buckets_expected": len(expected), "buckets_in_db": len(actual),
            "legacy_only_buckets": len(legacy_only),
            "consistent": not diffs, "diffs": diffs[:50]}


def _bad_ts(ts) -> bool:
    if ts is None:
        return True
    try:
        rules.parse_ts(ts)
        return False
    except rules.RuleError:
        return True


def _close(a, b) -> bool:
    if a is None or b is None:
        return a is b
    return abs(a - b) <= 1e-6
