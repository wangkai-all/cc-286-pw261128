"""HTTP 冒烟测试：真实起服务，验证页面与关键 API 闭环。"""
import http.client
import json
import os
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer

from workbench import server


class TestServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.app = server.App(os.path.join(cls.tmp.name, "t.db"))
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.make_handler(cls.app))
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.app.stop()
        cls.tmp.cleanup()

    def req(self, method, path, body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port)
        payload = json.dumps(body) if body is not None else None
        conn.request(method, path, payload,
                     {"Content-Type": "application/json"} if body else {})
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        ctype = resp.getheader("Content-Type") or ""
        return resp.status, (json.loads(data) if "json" in ctype else data)

    def test_01_page_served(self):
        status, body = self.req("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn("迁移工作台".encode(), body)

    def test_02_full_flow(self):
        # 建规则版本
        status, r = self.req("POST", "/api/rules", {
            "rule_version": "v1", "kind": "local_hour",
            "activated_utc": "2026-01-01T00:00:00Z"})
        self.assertEqual(status, 200, r)
        status, r = self.req("POST", "/api/rules", {
            "rule_version": "v2", "kind": "business_shift",
            "shifts": [{"name": "morning", "start": "06:00", "end": "13:30"},
                       {"name": "evening", "start": "13:30", "end": "22:00"},
                       {"name": "night", "start": "22:00", "end": "06:00"}],
            "activated_utc": "2026-11-16T00:00:00Z"})
        self.assertEqual(status, 200, r)
        # 重复版本被拒（不可变）
        status, r = self.req("POST", "/api/rules",
                             {"rule_version": "v1", "kind": "local_hour"})
        self.assertEqual(status, 400)
        # 合成数据（含旧仓导入 + 坏事件）
        status, r = self.req("POST", "/api/synth/generate", {"with_bad_event": True})
        self.assertEqual(status, 200, r)
        self.assertGreater(r["ingest"]["inserted"], 0)
        # 迁移 v2 历史
        status, r = self.req("POST", "/api/migrations", {
            "rule_version": "v2", "site": "site-ny",
            "from": "2026-03-01T00:00:00Z", "to": "2026-11-18T00:00:00Z",
            "segment_minutes": 720})
        job_id = r["job_id"]
        status, r = self.req("POST", "/api/migrations/run", {})
        self.assertTrue(any(o["status"] == "failed" for o in r["processed"]))
        # 修复坏事件 → 重试失败段 → 再跑
        status, segs = self.req("GET", f"/api/migrations?job_id={job_id}")
        failed = [s for s in segs["segments"] if s["status"] == "failed"]
        self.assertTrue(failed)
        status, r = self.req("POST", "/api/events/fix",
                             {"event_id": "evt-bad-0001",
                              "ts_utc": "2026-11-15T12:00:00Z"})
        self.assertEqual(r["action"], "repaired")
        for s in failed:
            self.req("POST", f"/api/segments/{s['segment_id']}/retry", {})
        self.req("POST", "/api/migrations/run", {})
        status, segs = self.req("GET", f"/api/migrations?job_id={job_id}")
        self.assertTrue(all(s["status"] == "done" for s in segs["segments"]))
        # 对账
        status, v = self.req("GET", "/api/synth/verify?site=site-ny&rule=v2")
        self.assertTrue(v["consistent"], v["diffs"][:2])
        # 两口径查询 + 覆盖率
        status, q2 = self.req("GET", "/api/query?site=site-ny&rule=v2"
                              "&from=2026-11-15T00:00:00Z&to=2026-11-17T00:00:00Z")
        self.assertEqual(q2["coverage"]["status"], "complete")
        labels = {b["bucket_label"] for b in q2["buckets"]}
        self.assertIn("day:2026-11-15:shift:night", labels)
        # 缺规则参数默认 v1（历史链接语义）
        status, q1 = self.req("GET", "/api/query?site=site-ny"
                              "&from=2026-11-15T00:00:00Z&to=2026-11-16T00:00:00Z")
        self.assertEqual(q1["rule_version"], "v1")
        # 旧链接端点
        status, leg = self.req("GET", "/api/legacy/hourly?site=site-ny"
                               "&date=2026-11-01&hour=1")
        self.assertEqual(len(leg["buckets"]), 2)  # fall-back 两个 01:00
        # 不可恢复区间：v2 查 9 月（只有旧汇总；即使段已重算过也不掩盖）
        status, q9 = self.req("GET", "/api/query?site=site-ny&rule=v2"
                              "&from=2026-09-01T00:00:00Z&to=2026-09-03T00:00:00Z")
        self.assertEqual(q9["coverage"]["status"], "partial")
        self.assertTrue(q9["coverage"]["unrecoverable"])
        self.assertEqual(q9["buckets"], [])
        # 事件归属查看器
        status, a = self.req("GET", "/api/attribute?site=site-ny"
                             "&ts=2026-11-16T06:30:00Z")
        by_rule = {x["rule_version"]: x for x in a["attributions"]}
        self.assertEqual(by_rule["v2"]["bucket_label"],
                         "day:2026-11-15:shift:night")
        self.assertEqual(by_rule["v1"]["bucket_label"],
                         "hour:2026-11-16T01:00:00-05:00")


if __name__ == "__main__":
    unittest.main()
