"""兼容性测试：旧查询链接在迁移前后结果一致；规则版本不可变。"""
import unittest

from workbench import ingest, migrate, query, rules, storage, synth
from workbench.rules import RuleError

SHIFTS = [
    {"name": "morning", "start": "06:00", "end": "13:30"},
    {"name": "evening", "start": "13:30", "end": "22:00"},
    {"name": "night", "start": "22:00", "end": "06:00"},
]
RANGE = ("2026-11-10T00:00:00Z", "2026-11-17T00:00:00Z")


def run_all(conn):
    while migrate.run_pending_segments(conn):
        pass


class TestLegacyLinks(unittest.TestCase):
    def setUp(self):
        self.conn = storage.connect(":memory:")
        storage.init_db(self.conn)
        storage.create_rule_version(self.conn, rules.RuleVersion.local_hour(),
                                    "2026-01-01T00:00:00Z")
        ingest.ingest_events(self.conn, synth.synth_events())

    def test_legacy_hourly_identical_across_migration(self):
        probes = [("2026-11-15", 5), ("2026-11-15", 13), ("2026-11-15", 22),
                  ("2026-11-01", 1), ("2026-03-08", 2)]
        before = [query.legacy_hourly(self.conn, "site-ny", d, h) for d, h in probes]
        # 迁移 v2 全量历史
        storage.create_rule_version(
            self.conn, rules.RuleVersion.business_shift("v2", SHIFTS),
            "2026-11-16T00:00:00Z")
        migrate.create_job(self.conn, "v2", "site-ny", *RANGE, segment_minutes=360)
        run_all(self.conn)
        after = [query.legacy_hourly(self.conn, "site-ny", d, h) for d, h in probes]
        self.assertEqual(before, after)

    def test_fall_back_hour_returns_two_buckets(self):
        # 旧链接查 2026-11-01 01 点：两个 01:00（EDT/EST）如实各返一桶
        r = query.legacy_hourly(self.conn, "site-ny", "2026-11-01", 1)
        labels = {b["bucket_label"] for b in r["buckets"]}
        self.assertEqual(labels, {"hour:2026-11-01T01:00:00-04:00",
                                  "hour:2026-11-01T01:00:00-05:00"})

    def test_spring_forward_missing_hour_returns_empty(self):
        r = query.legacy_hourly(self.conn, "site-ny", "2026-03-08", 2)
        self.assertEqual(r["buckets"], [])

    def test_v1_query_unchanged_by_v2_migration(self):
        before = query.query(self.conn, "site-ny", "v1", *RANGE)
        storage.create_rule_version(
            self.conn, rules.RuleVersion.business_shift("v2", SHIFTS),
            "2026-11-16T00:00:00Z")
        migrate.create_job(self.conn, "v2", "site-ny", *RANGE, segment_minutes=360)
        run_all(self.conn)
        after = query.query(self.conn, "site-ny", "v1", *RANGE)
        self.assertEqual(before, after)


class TestRuleImmutability(unittest.TestCase):
    def test_duplicate_version_rejected(self):
        conn = storage.connect(":memory:")
        storage.init_db(conn)
        storage.create_rule_version(conn, rules.RuleVersion.local_hour())
        with self.assertRaises(RuleError):
            storage.create_rule_version(conn, rules.RuleVersion.local_hour())
        with self.assertRaises(RuleError):
            storage.create_rule_version(
                conn, rules.RuleVersion.business_shift("v1", SHIFTS))

    def test_new_version_coexists(self):
        conn = storage.connect(":memory:")
        storage.init_db(conn)
        storage.create_rule_version(conn, rules.RuleVersion.local_hour())
        storage.create_rule_version(
            conn, rules.RuleVersion.business_shift("v2", SHIFTS))
        storage.create_rule_version(
            conn, rules.RuleVersion.business_shift(
                "v3", [{"name": "a", "start": "08:00", "end": "20:00"},
                       {"name": "b", "start": "20:00", "end": "08:00"}]))
        versions = {r["rule_version"] for r in storage.list_rule_versions(conn)}
        self.assertEqual(versions, {"v1", "v2", "v3"})


if __name__ == "__main__":
    unittest.main()
