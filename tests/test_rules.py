"""规则引擎测试：DST、跨午夜、不等长班次、归属规则、校验。

关键断言原则：任何时长/边界都来自墙钟→UTC 转换，而不是固定小时加减。
"""
import unittest
from datetime import date, timedelta

from workbench import rules
from workbench.rules import RuleError

NY = "America/New_York"
SH = "Asia/Shanghai"


def ts(s):
    return rules.parse_ts(s)


# 不等长班次：7.5h / 8.5h / 8h，夜班跨午夜
SHIFTS = [
    {"name": "morning", "start": "06:00", "end": "13:30"},
    {"name": "evening", "start": "13:30", "end": "22:00"},
    {"name": "night", "start": "22:00", "end": "06:00"},
]


def v2(**kw):
    return rules.RuleVersion.business_shift("v2", SHIFTS, **kw)


class TestLocalHour(unittest.TestCase):
    rule = rules.RuleVersion.local_hour()

    def test_spring_forward_skipped_hour(self):
        # 2026-03-08 纽约 02:00-02:59 不存在
        before = rules.assign(self.rule, NY, ts("2026-03-08T06:59:30Z"))
        self.assertEqual(before.label, "hour:2026-03-08T01:00:00-05:00")
        self.assertEqual((rules.iso(before.start_utc), rules.iso(before.end_utc)),
                         ("2026-03-08T06:00:00Z", "2026-03-08T07:00:00Z"))
        after = rules.assign(self.rule, NY, ts("2026-03-08T07:00:30Z"))
        self.assertEqual(after.label, "hour:2026-03-08T03:00:00-04:00")
        self.assertEqual(rules.iso(after.start_utc), "2026-03-08T07:00:00Z")
        # 该营业日只有 23 个整点桶，且没有 02:00 桶
        labels = {rules.assign(self.rule, NY, ts("2026-03-08T05:00:00Z")
                               + timedelta(hours=h)).label for h in range(23)}
        self.assertEqual(len(labels), 23)
        self.assertFalse(any("T02:00" in l for l in labels))

    def test_fall_back_repeated_hour(self):
        # 2026-11-01 纽约 01:00 发生两次：两个不同的桶，各 1 真实小时，首尾相接
        first = rules.assign(self.rule, NY, ts("2026-11-01T05:30:00Z"))
        second = rules.assign(self.rule, NY, ts("2026-11-01T06:30:00Z"))
        self.assertEqual(first.label, "hour:2026-11-01T01:00:00-04:00")
        self.assertEqual(second.label, "hour:2026-11-01T01:00:00-05:00")
        self.assertEqual(first.end_utc - first.start_utc, timedelta(hours=1))
        self.assertEqual(second.end_utc - second.start_utc, timedelta(hours=1))
        self.assertEqual(first.end_utc, second.start_utc)
        # 该营业日有 25 个整点桶
        labels = {rules.assign(self.rule, NY, ts("2026-11-01T04:00:00Z")
                               + timedelta(hours=h)).label for h in range(25)}
        self.assertEqual(len(labels), 25)

    def test_label_carries_offset(self):
        b = rules.assign(self.rule, SH, ts("2026-11-15T16:20:00Z"))
        self.assertEqual(b.label, "hour:2026-11-16T00:00:00+08:00")


class TestShiftValidation(unittest.TestCase):
    def test_unequal_shifts_accepted(self):
        rule = v2()
        durs = [s.duration_minutes for s in rule.shifts()]
        self.assertEqual(sorted(durs), [450, 480, 510])

    def test_gap_rejected(self):
        with self.assertRaises(RuleError):
            rules.RuleVersion.business_shift("vx", [
                {"name": "a", "start": "06:00", "end": "14:00"},
                {"name": "b", "start": "15:00", "end": "06:00"},  # 1h 缺口
            ])

    def test_overlap_rejected(self):
        with self.assertRaises(RuleError):
            rules.RuleVersion.business_shift("vx", [
                {"name": "a", "start": "06:00", "end": "15:00"},
                {"name": "b", "start": "14:00", "end": "06:00"},  # 重叠 1h
            ])

    def test_zero_length_rejected(self):
        with self.assertRaises(RuleError):
            rules.RuleVersion.business_shift("vx", [
                {"name": "a", "start": "06:00", "end": "06:00"},
            ])

    def test_not_24h_rejected(self):
        with self.assertRaises(RuleError):
            rules.RuleVersion.business_shift("vx", [
                {"name": "a", "start": "06:00", "end": "14:00"},
                {"name": "b", "start": "14:00", "end": "22:00"},
                {"name": "c", "start": "22:00", "end": "05:00"},  # 总共 23h
            ])


class TestBusinessShift(unittest.TestCase):
    def test_unequal_bucket_durations(self):
        rule = v2()
        morning = rules.assign(rule, NY, ts("2026-11-15T12:00:00Z"))  # 07:00 EST
        self.assertEqual(morning.label, "day:2026-11-15:shift:morning")
        self.assertEqual(morning.end_utc - morning.start_utc, timedelta(hours=7.5))
        evening = rules.assign(rule, NY, ts("2026-11-15T19:00:00Z"))  # 14:00 EST
        self.assertEqual(evening.label, "day:2026-11-15:shift:evening")
        self.assertEqual(evening.end_utc - evening.start_utc, timedelta(hours=8.5))

    def test_cross_midnight_attribution_start_day(self):
        rule = v2()
        # 2026-11-16 01:30 EST：物理上是 16 号凌晨，归属 15 号开工的夜班
        b = rules.assign(rule, NY, ts("2026-11-16T06:30:00Z"))
        self.assertEqual(b.label, "day:2026-11-15:shift:night")
        self.assertEqual((rules.iso(b.start_utc), rules.iso(b.end_utc)),
                         ("2026-11-16T03:00:00Z", "2026-11-16T11:00:00Z"))

    def test_end_day_attribution(self):
        rule = v2(attribution="end_day")
        b = rules.assign(rule, NY, ts("2026-11-16T06:30:00Z"))
        self.assertEqual(b.label, "day:2026-11-16:shift:night")

    def test_day_start_boundary(self):
        # 自定义日界 04:00 + 凌晨 02:00 开工的班次：02:00 < 04:00，归前一营业日
        shifts = [
            {"name": "grave", "start": "02:00", "end": "10:00"},
            {"name": "day", "start": "10:00", "end": "18:00"},
            {"name": "swing", "start": "18:00", "end": "02:00"},
        ]
        rule = rules.RuleVersion.business_shift("v3", shifts, day_start="04:00")
        b = rules.assign(rule, NY, ts("2026-11-16T07:30:00Z"))  # 02:30 EST
        self.assertEqual(b.label, "day:2026-11-15:shift:grave")
        rule0 = rules.RuleVersion.business_shift("v4", shifts)  # 默认 00:00 日界
        b0 = rules.assign(rule0, NY, ts("2026-11-16T07:30:00Z"))
        self.assertEqual(b0.label, "day:2026-11-16:shift:grave")

    def test_dst_night_shift_real_duration(self):
        # 不用固定 8h：spring-forward 夜班 7h，fall-back 夜班 9h
        night = [s for s in v2().shifts() if s.name == "night"][0]
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(NY)
        s1, e1 = rules.shift_instance(tz, date(2026, 3, 7), night)   # 春季当夜
        self.assertEqual(e1 - s1, timedelta(hours=7))
        s2, e2 = rules.shift_instance(tz, date(2026, 10, 31), night)  # 秋季当夜
        self.assertEqual(e2 - s2, timedelta(hours=9))
        s3, e3 = rules.shift_instance(tz, date(2026, 11, 15), night)  # 普通夜
        self.assertEqual(e3 - s3, timedelta(hours=8))

    def test_business_day_interval_dst(self):
        s, e = rules.business_day_interval(NY, "00:00", date(2026, 3, 8))
        self.assertEqual(e - s, timedelta(hours=23))
        s, e = rules.business_day_interval(NY, "00:00", date(2026, 11, 1))
        self.assertEqual(e - s, timedelta(hours=25))

    def test_shift_boundaries_minute_precision(self):
        rule = v2()
        # 13:29 EST = 18:29Z 归 morning；13:30 EST = 18:30Z 归 evening
        self.assertEqual(rules.assign(rule, NY, ts("2026-11-15T18:29:59Z")).label,
                         "day:2026-11-15:shift:morning")
        self.assertEqual(rules.assign(rule, NY, ts("2026-11-15T18:30:00Z")).label,
                         "day:2026-11-15:shift:evening")

    def test_no_dst_site(self):
        rule = v2()
        b = rules.assign(rule, SH, ts("2026-11-16T17:30:00Z"))  # 次日 01:30 CST
        self.assertEqual(b.label, "day:2026-11-16:shift:night")
        self.assertEqual(b.end_utc - b.start_utc, timedelta(hours=8))


if __name__ == "__main__":
    unittest.main()
