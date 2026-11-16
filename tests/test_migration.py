"""迁移引擎测试：重算正确性、幂等、段边界、失败恢复、不可恢复区间、并发一致性。"""
import os
import tempfile
import threading
import unittest

from workbench import ingest, migrate, query, rules, storage, synth

SHIFTS = [
    {"name": "morning", "start": "06:00", "end": "13:30"},
    {"name": "evening", "start": "13:30", "end": "22:00"},
    {"name": "night", "start": "22:00", "end": "06:00"},
]
RANGE = ("2026-03-01T00:00:00Z", "2026-11-18T00:00:00Z")  # 覆盖全部合成事件
CUT_RANGE = ("2026-10-31T00:00:00Z", "2026-11-17T00:00:00Z")  # 段切夜班专项


def make_db(path=":memory:"):
    conn = storage.connect(path)
    storage.init_db(conn)
    return conn


def add_v1(conn, activated="2026-01-01T00:00:00Z"):
    storage.create_rule_version(conn, rules.RuleVersion.local_hour(), activated)


def add_v2(conn, activated="2026-11-16T00:00:00Z"):
    storage.create_rule_version(
        conn, rules.RuleVersion.business_shift("v2", SHIFTS), activated)


def run_all(conn):
    total = []
    while True:
        out = migrate.run_pending_segments(conn)
        total += out
        if not out:
            return total


class TestRecompute(unittest.TestCase):
    def setUp(self):
        self.conn = make_db()
        add_v1(self.conn)
        self.events = synth.synth_events()
        ingest.ingest_events(self.conn, self.events)

    def test_recompute_matches_brute_force(self):
        add_v2(self.conn)  # v2 在入仓之后才创建：历史全靠迁移回补
        job = migrate.create_job(self.conn, "v2", "site-ny", *RANGE, segment_minutes=360)
        run_all(self.conn)
        for seg in migrate.list_segments(self.conn, job):
            self.assertEqual(seg["status"], "done", seg)
        v = synth.verify(self.conn, "site-ny", "v2")
        self.assertTrue(v["consistent"], v["diffs"][:3])
        v1 = synth.verify(self.conn, "site-ny", "v1")
        self.assertTrue(v1["consistent"], v1["diffs"][:3])

    def test_idempotent_double_run(self):
        add_v2(self.conn)
        migrate.create_job(self.conn, "v2", "site-ny", *RANGE, segment_minutes=360)
        run_all(self.conn)
        before = self.conn.execute(
            "SELECT * FROM aggregates WHERE rule_version='v2' ORDER BY 1,2,3,4").fetchall()
        migrate.create_job(self.conn, "v2", "site-ny", *RANGE, segment_minutes=360)
        run_all(self.conn)
        after = self.conn.execute(
            "SELECT * FROM aggregates WHERE rule_version='v2' ORDER BY 1,2,3,4").fetchall()
        self.assertEqual([tuple(r) for r in before], [tuple(r) for r in after])

    def test_segment_cut_through_night_shift(self):
        # 段边界（47 分钟，刻意不对齐）任意切穿跨午夜班次，整桶重算保证结果不变
        add_v2(self.conn)
        migrate.create_job(self.conn, "v2", "site-ny", *CUT_RANGE, segment_minutes=47)
        run_all(self.conn)
        v = synth.verify(self.conn, "site-ny", "v2", *CUT_RANGE)
        self.assertTrue(v["consistent"], v["diffs"][:3])

    def test_v1_untouched_by_v2_migration(self):
        before = self.conn.execute(
            "SELECT * FROM aggregates WHERE rule_version='v1' ORDER BY 1,2,3,4").fetchall()
        add_v2(self.conn)
        migrate.create_job(self.conn, "v2", "site-ny", *RANGE, segment_minutes=360)
        run_all(self.conn)
        after = self.conn.execute(
            "SELECT * FROM aggregates WHERE rule_version='v1' ORDER BY 1,2,3,4").fetchall()
        self.assertEqual([tuple(r) for r in before], [tuple(r) for r in after])


class TestFailureRecovery(unittest.TestCase):
    def setUp(self):
        self.conn = make_db()
        add_v1(self.conn)
        ingest.ingest_events(self.conn, synth.synth_events())
        add_v2(self.conn)

    def test_bad_event_fails_segment_then_repair_and_retry(self):
        bad_id = synth.inject_bad_event(self.conn)  # 2026-11-15T25:61:00Z
        job = migrate.create_job(self.conn, "v2", "site-ny", *RANGE, segment_minutes=360)
        run_all(self.conn)
        segs = migrate.list_segments(self.conn, job)
        failed = [s for s in segs if s["status"] == "failed"]
        # 坏事件会污染所有触及它的段（所在段 + 重算到它的桶的段）
        self.assertGreaterEqual(len(failed), 1)
        self.assertTrue(any(bad_id in (s["last_error"] or "") for s in failed))
        self.assertTrue(any(s["status"] == "done" for s in segs))  # 其余段不受影响
        # 修复（改正时间戳）→ 重试全部失败段 → 全部完成 → 对账一致
        migrate.fix_event(self.conn, bad_id, "2026-11-15T12:00:00Z")
        for s in migrate.list_segments(self.conn, job):
            if s["status"] == "failed":
                migrate.retry_segment(self.conn, s["segment_id"])
        run_all(self.conn)
        self.assertTrue(all(s["status"] == "done"
                            for s in migrate.list_segments(self.conn, job)))
        v = synth.verify(self.conn, "site-ny", "v2")
        self.assertTrue(v["consistent"], v["diffs"][:3])

    def test_quarantine_path(self):
        bad_id = synth.inject_bad_event(self.conn)
        job = migrate.create_job(self.conn, "v2", "site-ny", *RANGE, segment_minutes=360)
        run_all(self.conn)
        failed = [s for s in migrate.list_segments(self.conn, job)
                  if s["status"] == "failed"]
        self.assertGreaterEqual(len(failed), 1)
        migrate.fix_event(self.conn, bad_id, None)  # 隔离而不是改正
        for s in migrate.list_segments(self.conn, job):
            if s["status"] == "failed":
                migrate.retry_segment(self.conn, s["segment_id"])
        run_all(self.conn)
        self.assertTrue(all(s["status"] == "done"
                            for s in migrate.list_segments(self.conn, job)))
        v = synth.verify(self.conn, "site-ny", "v2")
        self.assertTrue(v["consistent"], v["diffs"][:3])


class TestUnrecoverable(unittest.TestCase):
    def test_missing_raw_events_not_fabricated(self):
        conn = make_db()
        add_v1(conn)
        add_v2(conn)
        synth.legacy_import(conn, "site-ny",
                            "2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")
        # v1：旧区间有聚合、覆盖率完整（legacy-import 段）
        q1 = query.query(conn, "site-ny", "v1",
                         "2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")
        self.assertEqual(q1["coverage"]["status"], "complete")
        self.assertTrue(len(q1["buckets"]) > 0)
        # v2：没有原始事件 → 标 unrecoverable，且没有任何编造出来的桶
        q2 = query.query(conn, "site-ny", "v2",
                         "2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")
        self.assertEqual(q2["coverage"]["status"], "partial")
        self.assertTrue(q2["coverage"]["unrecoverable"])
        self.assertEqual(q2["coverage"]["pending"], [])
        self.assertEqual(q2["buckets"], [])

    def test_unrecoverable_not_masked_by_completed_segments(self):
        # 关键语义：段「成功」重算过（零事件）也不能把不可恢复区间掩盖成 complete
        conn = make_db()
        add_v1(conn)
        add_v2(conn)
        synth.legacy_import(conn, "site-ny",
                            "2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")
        migrate.create_job(conn, "v2", "site-ny",
                           "2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z",
                           segment_minutes=720)
        run_all(conn)
        q = query.query(conn, "site-ny", "v2",
                        "2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")
        self.assertEqual(q["coverage"]["status"], "partial")
        self.assertTrue(q["coverage"]["unrecoverable"])
        self.assertEqual(q["buckets"], [])


class TestCoverageStates(unittest.TestCase):
    def test_pending_then_complete(self):
        conn = make_db()
        add_v1(conn)
        ingest.ingest_events(conn, synth.synth_events())
        add_v2(conn, activated="2026-11-16T00:00:00Z")
        q = query.query(conn, "site-ny", "v2", *RANGE)
        self.assertEqual(q["coverage"]["status"], "partial")
        self.assertTrue(q["coverage"]["pending"])
        migrate.create_job(conn, "v2", "site-ny", *RANGE, segment_minutes=360)
        run_all(conn)
        q2 = query.query(conn, "site-ny", "v2", *RANGE)
        self.assertEqual(q2["coverage"]["status"], "complete")


class TestConcurrency(unittest.TestCase):
    def test_ingest_during_migration_stays_consistent(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "t.db")
            conn = make_db(path)
            add_v1(conn)
            add_v2(conn, activated="2026-11-10T00:00:00Z")  # 双写全程开启
            events = synth.synth_events()
            ingest.ingest_events(conn, events[:60])
            migrate.create_job(conn, "v2", "site-ny", *RANGE, segment_minutes=720)
            migrate.create_job(conn, "v2", "site-sh", *RANGE, segment_minutes=720)

            errors = []

            def feed():  # 迁移进行的同时持续入仓
                try:
                    c = storage.connect(path)
                    for i in range(60, len(events), 10):
                        ingest.ingest_events(c, events[i:i + 10])
                except Exception as exc:  # pragma: no cover
                    errors.append(exc)

            t = threading.Thread(target=feed)
            t.start()
            run_all(conn)
            t.join()
            self.assertEqual(errors, [])
            for site in ("site-ny", "site-sh"):
                for rv in ("v1", "v2"):
                    v = synth.verify(conn, site, rv)
                    self.assertTrue(v["consistent"], (site, rv, v["diffs"][:3]))
            conn.close()


if __name__ == "__main__":
    unittest.main()
