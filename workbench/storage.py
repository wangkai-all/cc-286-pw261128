"""存储层：SQLite schema、连接与写事务。

一致性模型的物理基础：
- events 是唯一的原始事实来源；aggregates/buckets 永远可以从 events 重算。
- 入仓对所有 active 规则版本原子双写（同一事务）。
- 迁移按段重算，段内以「整个桶」为原子单位 delete+recompute，幂等。
- 所有写事务经过全局写锁串行化（单写者），读者（查询）不加锁。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

from . import rules

SCHEMA = """
CREATE TABLE IF NOT EXISTS sites (
  site_id TEXT PRIMARY KEY,
  name    TEXT NOT NULL,
  tz      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rule_versions (
  rule_version  TEXT PRIMARY KEY,
  kind          TEXT NOT NULL,
  params_json   TEXT NOT NULL,
  activated_utc TEXT NOT NULL,   -- 该版本从此刻起由入仓双写覆盖
  status        TEXT NOT NULL DEFAULT 'active'  -- active | retired
);
CREATE TABLE IF NOT EXISTS events (
  event_id   TEXT PRIMARY KEY,
  site_id    TEXT NOT NULL REFERENCES sites(site_id),
  device_id  TEXT NOT NULL,
  ts_utc     TEXT,               -- 允许非法值入库（模拟坏上游），重算时暴露
  metric     TEXT NOT NULL,
  value      REAL NOT NULL,
  quarantined INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_site_ts ON events(site_id, ts_utc);
CREATE TABLE IF NOT EXISTS buckets (
  rule_version TEXT NOT NULL,
  site_id      TEXT NOT NULL,
  bucket_label TEXT NOT NULL,
  start_utc    TEXT NOT NULL,
  end_utc      TEXT NOT NULL,
  PRIMARY KEY (rule_version, site_id, bucket_label)
);
CREATE TABLE IF NOT EXISTS aggregates (
  rule_version TEXT NOT NULL,
  site_id      TEXT NOT NULL,
  bucket_label TEXT NOT NULL,
  metric       TEXT NOT NULL,
  count        INTEGER NOT NULL,
  sum          REAL NOT NULL,
  min          REAL,
  max          REAL,
  PRIMARY KEY (rule_version, site_id, bucket_label, metric)
);
CREATE TABLE IF NOT EXISTS jobs (
  job_id          INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_version    TEXT NOT NULL,
  site_id         TEXT NOT NULL,
  range_start_utc TEXT NOT NULL,
  range_end_utc   TEXT NOT NULL,
  segment_minutes INTEGER NOT NULL,
  created_utc     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS segments (
  segment_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id          INTEGER REFERENCES jobs(job_id),
  rule_version    TEXT NOT NULL,
  site_id         TEXT NOT NULL,
  range_start_utc TEXT NOT NULL,
  range_end_utc   TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'pending',  -- pending|running|done|failed
  source          TEXT NOT NULL DEFAULT 'recompute', -- recompute|legacy-import
  attempts        INTEGER NOT NULL DEFAULT 0,
  last_error      TEXT,
  updated_utc     TEXT
);
"""

DEFAULT_SITES = [
    ("site-ny", "纽约站（有夏令时）", "America/New_York"),
    ("site-sh", "上海站（无夏令时）", "Asia/Shanghai"),
]

# 单写者锁：入仓与段重算的写事务都经过它，避免 WAL 下的写-写冲突与丢更新。
WRITE_LOCK = threading.RLock()


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def write_txn(conn: sqlite3.Connection):
    """串行化的写事务：持锁 + BEGIN IMMEDIATE，异常自动回滚。"""
    with WRITE_LOCK:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    for site_id, name, tz in DEFAULT_SITES:
        conn.execute("INSERT OR IGNORE INTO sites(site_id, name, tz) VALUES (?,?,?)",
                     (site_id, name, tz))


def utcnow_iso() -> str:
    return rules.iso(datetime.now(timezone.utc))


# ---- 规则版本 ----

def create_rule_version(conn: sqlite3.Connection, rule: rules.RuleVersion,
                        activated_utc: str | None = None) -> None:
    """规则版本不可变：重复 id 直接拒绝，没有更新路径。"""
    with write_txn(conn):
        cur = conn.execute(
            "INSERT OR IGNORE INTO rule_versions(rule_version, kind, params_json, activated_utc)"
            " VALUES (?,?,?,?)",
            (rule.rule_version, rule.kind, json.dumps(rule.params, sort_keys=True),
             activated_utc or utcnow_iso()))
        if cur.rowcount == 0:
            raise rules.RuleError(f"rule version {rule.rule_version!r} already exists; "
                                  "rule versions are immutable, create a new one")


def load_rule(conn: sqlite3.Connection, rule_version: str) -> rules.RuleVersion:
    row = conn.execute("SELECT * FROM rule_versions WHERE rule_version=?",
                       (rule_version,)).fetchone()
    if row is None:
        raise rules.RuleError(f"unknown rule version: {rule_version!r}")
    return rules.RuleVersion(row["rule_version"], row["kind"], json.loads(row["params_json"]))


def list_rule_versions(conn: sqlite3.Connection, active_only: bool = False) -> list[dict]:
    sql = "SELECT * FROM rule_versions" + (" WHERE status='active'" if active_only else "")
    return [dict(r) for r in conn.execute(sql + " ORDER BY rule_version")]


def site_tz(conn: sqlite3.Connection, site_id: str) -> str:
    row = conn.execute("SELECT tz FROM sites WHERE site_id=?", (site_id,)).fetchone()
    if row is None:
        raise rules.RuleError(f"unknown site: {site_id!r}")
    return row["tz"]


def list_sites(conn: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in conn.execute("SELECT * FROM sites ORDER BY site_id")]
