"""规则版本与归属规则。

三个概念显式分离：
- 时间区间(range)：查询/迁移任务的 UTC 半开区间 [start, end)。
- 桶标签(label)：由规则版本 + 站点时区 + UTC 瞬间纯函数算出的稳定字符串。
- 归属规则(attribution)：instant -> bucket 的映射，按规则版本参数化。

铁律：任何边界都由「本地墙钟时间 + IANA 时区」正向转换为 UTC 瞬间，
绝不通过对本地时间加减固定小时来推算下一天/下一班。
DST 的 23/25 小时营业日、跨午夜班次的 7/9 小时真实时长，都是转换的
结果而不是假设。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc
DAY = timedelta(days=1)
HOUR = timedelta(hours=1)


class RuleError(ValueError):
    pass


def parse_ts(s: str) -> datetime:
    """解析带偏移的 ISO 时间戳并归一到 UTC。naive 或非法时间戳直接拒绝。"""
    try:
        dt = datetime.fromisoformat(s)
    except (ValueError, TypeError) as exc:
        raise RuleError(f"unparseable timestamp: {s!r}") from exc
    if dt.tzinfo is None:
        raise RuleError(f"timestamp lacks UTC offset: {s!r}")
    return dt.astimezone(UTC)


def iso(dt: datetime) -> str:
    """统一存储格式：UTC、秒精度、Z 后缀。字典序即时间序。"""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hhmm(s: str) -> time:
    try:
        hh, mm = s.split(":")
        return time(int(hh), int(mm))
    except Exception as exc:
        raise RuleError(f"bad HH:MM value: {s!r}") from exc


@dataclass(frozen=True)
class BucketRef:
    """一次归属的结果：稳定标签 + 该桶的 UTC 半开区间。"""
    label: str
    start_utc: datetime
    end_utc: datetime


@dataclass(frozen=True)
class Shift:
    name: str
    start: time
    end: time

    @property
    def duration_minutes(self) -> int:
        return (self.end.hour * 60 + self.end.minute
                - self.start.hour * 60 - self.start.minute) % 1440


def validate_shifts(shifts: list[dict]) -> list[Shift]:
    """校验班次表恰好铺满 24h（允许不等长、允许跨午夜），返回按开始时间排序的 Shift。"""
    if not shifts:
        raise RuleError("shifts must not be empty")
    seen = set()
    norm: list[Shift] = []
    for s in shifts:
        name = s.get("name")
        if not name or name in seen:
            raise RuleError(f"shift name missing or duplicated: {name!r}")
        seen.add(name)
        sh = Shift(name, _hhmm(s["start"]), _hhmm(s["end"]))
        if sh.duration_minutes == 0:
            raise RuleError(f"shift {name!r} has zero length")
        norm.append(sh)
    total = sum(sh.duration_minutes for sh in norm)
    if total != 1440:
        raise RuleError(f"shifts must tile exactly 24h, got {total} minutes")
    norm.sort(key=lambda sh: sh.start.hour * 60 + sh.start.minute)
    for i, sh in enumerate(norm):
        end_m = (sh.start.hour * 60 + sh.start.minute + sh.duration_minutes) % 1440
        nxt = norm[(i + 1) % len(norm)]
        nxt_m = nxt.start.hour * 60 + nxt.start.minute
        if end_m != nxt_m:
            raise RuleError(f"gap or overlap after shift {sh.name!r}")
    return norm


@dataclass(frozen=True)
class RuleVersion:
    """规则版本：不可变。改参数 = 新版本。"""
    rule_version: str
    kind: str  # "local_hour" | "business_shift"
    params: dict

    @staticmethod
    def local_hour(rule_version: str = "v1") -> "RuleVersion":
        return RuleVersion(rule_version, "local_hour", {})

    @staticmethod
    def business_shift(rule_version: str, shifts: list[dict],
                       day_start: str = "00:00",
                       attribution: str = "start_day") -> "RuleVersion":
        norm = validate_shifts(shifts)
        _hhmm(day_start)
        if attribution not in ("start_day", "end_day"):
            raise RuleError(f"unknown attribution: {attribution!r}")
        params = {
            "shifts": [{"name": s.name,
                        "start": s.start.strftime("%H:%M"),
                        "end": s.end.strftime("%H:%M")} for s in norm],
            "day_start": day_start,
            "attribution": attribution,
        }
        return RuleVersion(rule_version, "business_shift", params)

    def shifts(self) -> list[Shift]:
        return [Shift(s["name"], _hhmm(s["start"]), _hhmm(s["end"]))
                for s in self.params["shifts"]]


def business_date(local_dt: datetime, day_start: time) -> date:
    """本地墙钟时刻所属的营业日：早于日界时刻的归前一天。"""
    d = local_dt.date()
    return d if local_dt.time() >= day_start else d - DAY


def shift_instance(tz: ZoneInfo, d: date, sh: Shift) -> tuple[datetime, datetime]:
    """班次实例的 UTC 区间。开始/结束各自独立做墙钟→UTC 转换，
    跨午夜班次结束日 +1；DST 夜班的真实时长（7h/9h）由此自然得出。"""
    start_local = datetime.combine(d, sh.start, tzinfo=tz)
    end_d = d if sh.end > sh.start else d + DAY
    end_local = datetime.combine(end_d, sh.end, tzinfo=tz)
    s, e = start_local.astimezone(UTC), end_local.astimezone(UTC)
    if e <= s:
        raise RuleError(f"shift {sh.name!r} on {d} has non-positive UTC duration")
    return s, e


def business_day_interval(tz_name: str, day_start: str, biz: date) -> tuple[datetime, datetime]:
    """营业日的 UTC 区间（用于展示与按日汇总）。DST 日可能是 23/25 小时。"""
    tz = ZoneInfo(tz_name)
    ds = _hhmm(day_start)
    s = datetime.combine(biz, ds, tzinfo=tz).astimezone(UTC)
    e = datetime.combine(biz + DAY, ds, tzinfo=tz).astimezone(UTC)
    return s, e


def assign(rule: RuleVersion, tz_name: str, ts_utc: datetime) -> BucketRef:
    """归属规则主入口：UTC 瞬间 -> 桶。每个瞬间恰好属于一个桶。"""
    tz = ZoneInfo(tz_name)
    if rule.kind == "local_hour":
        return _assign_local_hour(tz, ts_utc)
    if rule.kind == "business_shift":
        return _assign_business_shift(rule, tz, ts_utc)
    raise RuleError(f"unknown rule kind: {rule.kind!r}")


def _assign_local_hour(tz: ZoneInfo, ts: datetime) -> BucketRef:
    """v1 旧口径：本地整点桶。

    标签带本地偏移，fall-back 重复的小时（如 01:00 出现两次）自然分成
    两个桶；spring-forward 不存在的小时永远不会有事件落入，不产生桶。
    每个真实发生的本地小时恰好持续 1 个真实小时：若墙钟 +1h 换算后
    不等于 1 个 UTC 小时（过渡发生在该小时内），回退为 start+1h(UTC)。
    """
    local = ts.astimezone(tz)
    wall = local.replace(minute=0, second=0, microsecond=0)
    start = wall.astimezone(UTC)
    cand = (wall + HOUR).astimezone(UTC)
    end = cand if cand - start == HOUR else start + HOUR
    return BucketRef(label=f"hour:{wall.isoformat()}", start_utc=start, end_utc=end)


def _assign_business_shift(rule: RuleVersion, tz: ZoneInfo, ts: datetime) -> BucketRef:
    """v2 新口径：营业日 + 不等长班次。

    候选开始日只需看本地当天与前一天（跨午夜班次从前一天延伸过来）。
    班次表已校验铺满 24h，故恰好命中一个实例。
    营业日标签由归属规则决定：start_day=归班次开始日（默认，夜班归开工日），
    end_day=归班次结束日；再叠加日界时刻 day_start。
    """
    p = rule.params
    day_start = _hhmm(p.get("day_start", "00:00"))
    attribution = p.get("attribution", "start_day")
    local = ts.astimezone(tz)
    for d in (local.date(), local.date() - DAY):
        for sh in rule.shifts():
            start_utc, end_utc = shift_instance(tz, d, sh)
            if start_utc <= ts < end_utc:
                start_local = datetime.combine(d, sh.start, tzinfo=tz)
                end_d = d if sh.end > sh.start else d + DAY
                end_local = datetime.combine(end_d, sh.end, tzinfo=tz)
                anchor = start_local if attribution == "start_day" else end_local
                biz = business_date(anchor, day_start)
                return BucketRef(label=f"day:{biz.isoformat()}:shift:{sh.name}",
                                 start_utc=start_utc, end_utc=end_utc)
    raise AssertionError("no shift contains instant — shifts must tile 24h")
