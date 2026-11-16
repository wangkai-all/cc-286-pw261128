#!/usr/bin/env python3
"""端到端演示：空库 → 规则版本 → 合成事件入仓 → 迁移 → 失败恢复 → 对账 → （可选）起服务。

用法：
  python3 run_demo.py            # 跑完整流程并打印核对结果
  python3 run_demo.py --serve    # 流程结束后启动工作台 http://127.0.0.1:8765
"""
import argparse
import os
import sys

from workbench import ingest, migrate, query, rules, storage, synth

SHIFTS = [
    {"name": "morning", "start": "06:00", "end": "13:30"},
    {"name": "evening", "start": "13:30", "end": "22:00"},
    {"name": "night", "start": "22:00", "end": "06:00"},
]
RANGE = ("2026-03-01T00:00:00Z", "2026-11-18T00:00:00Z")


def run_all(conn):
    while migrate.run_pending_segments(conn, limit=100):
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="workbench.db")
    ap.add_argument("--serve", action="store_true")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()

    if os.path.exists(args.db):
        os.remove(args.db)
    conn = storage.connect(args.db)
    storage.init_db(conn)

    print("== 1. 规则版本 ==")
    storage.create_rule_version(conn, rules.RuleVersion.local_hour(),
                                activated_utc="2026-01-01T00:00:00Z")
    print("  v1 local_hour（旧口径，本地整点），双写起始 2026-01-01")
    storage.create_rule_version(
        conn, rules.RuleVersion.business_shift("v2", SHIFTS),
        activated_utc="2026-11-16T00:00:00Z")
    print("  v2 business_shift（新口径：营业日 + 不等长班次 7.5h/8.5h/8h，夜班跨午夜归开工日），"
          "双写起始 2026-11-16")

    print("== 2. 合成事件入仓（持续双写） ==")
    events = synth.synth_events()
    res = ingest.ingest_events(conn, events)
    print(f"  入仓 {res['inserted']} 条（含 DST 与班次边界案例，固定种子可复现）")

    print("== 3. 模拟旧数仓：9 月只有 v1 小时汇总、无原始事件 ==")
    legacy = synth.legacy_import(conn, "site-ny",
                                 "2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")
    print(f"  导入 {legacy['legacy_hours']} 个小时桶（count/sum，无 min/max）")

    print("== 4. 历史回补：v2 迁移 2026-03-01 → 2026-11-18（段长 12h） ==")
    bad = synth.inject_bad_event(conn)
    print(f"  注入坏事件 {bad}（ts=2026-11-15T25:61:00Z，模拟坏上游）")
    job = migrate.create_job(conn, "v2", "site-ny", *RANGE, segment_minutes=720)
    migrate.create_job(conn, "v2", "site-sh", *RANGE, segment_minutes=720)
    run_all(conn)
    segs = migrate.list_segments(conn, job)
    failed = [s for s in segs if s["status"] == "failed"]
    print(f"  site-ny 任务 {job}：{sum(1 for s in segs if s['status']=='done')} 段完成，"
          f"{len(failed)} 段失败")
    for s in failed:
        print(f"    段 {s['segment_id']} 失败: {s['last_error']}")

    print("== 5. 修复坏事件并重试失败段 ==")
    fix = migrate.fix_event(conn, bad, "2026-11-15T12:00:00Z")
    print(f"  修复结果: {fix['action']}，失效段: {fix['invalidated_segments']}")
    for s in failed:
        migrate.retry_segment(conn, s["segment_id"])
    # 修复改变了原始事件：该事件新位置处于 v1 的「live 覆盖」区间（无段可失效），
    # 需对该区间所有口径显式补重算任务 —— 修复原始数据 = 相关区间全口径重算。
    migrate.create_job(conn, "v1", "site-ny",
                       "2026-11-15T00:00:00Z", "2026-11-16T00:00:00Z",
                       segment_minutes=360)
    run_all(conn)
    assert all(s["status"] == "done" for s in migrate.list_segments(conn, job))
    print("  全部段 done（含 v1 补丁区间重算）")

    print("== 6. 对账（events 独立重算 vs 聚合表） ==")
    ok = True
    for site in ("site-ny", "site-sh"):
        for rv in ("v1", "v2"):
            v = synth.verify(conn, site, rv)
            ok &= v["consistent"]
            print(f"  {site} / {rv}: {'一致' if v['consistent'] else '不一致'} "
                  f"（{v['buckets_in_db']} 桶）")
    if not ok:
        sys.exit("对账失败")

    print("== 7. 两口径与覆盖率抽查 ==")
    q1 = query.query(conn, "site-ny", "v1",
                     "2026-11-15T00:00:00Z", "2026-11-16T12:00:00Z")
    q2 = query.query(conn, "site-ny", "v2",
                     "2026-11-15T00:00:00Z", "2026-11-16T12:00:00Z")
    print(f"  v1: {len(q1['buckets'])} 个小时桶, 覆盖率 {q1['coverage']['status']}")
    print(f"  v2: {len(q2['buckets'])} 个班次桶, 覆盖率 {q2['coverage']['status']}")
    for b in q2["buckets"]:
        if "night" in b["bucket_label"]:
            print(f"    {b['bucket_label']}  {b['start_utc']} → {b['end_utc']}  "
                  f"count={b['metrics']['power_kw']['count']}")
    q9 = query.query(conn, "site-ny", "v2",
                     "2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z")
    print(f"  v2 查 9 月: 覆盖率 {q9['coverage']['status']}，"
          f"unrecoverable {len(q9['coverage']['unrecoverable'])} 段，"
          f"桶数 {len(q9['buckets'])}（不编造精确值）")

    print("== 8. 旧链接兼容 ==")
    leg = query.legacy_hourly(conn, "site-ny", "2026-11-01", 1)
    print(f"  /api/legacy/hourly?site=site-ny&date=2026-11-01&hour=1 → "
          f"{[b['bucket_label'] for b in leg['buckets']]}（fall-back 两个 01:00）")

    if args.serve:
        from workbench import server
        server.serve(args.db, args.port)
    else:
        print("\n加 --serve 可启动工作台页面（默认 http://127.0.0.1:8765）")


if __name__ == "__main__":
    main()
