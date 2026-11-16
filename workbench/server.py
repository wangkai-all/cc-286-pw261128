"""HTTP 服务：JSON API + 工作台页面 + 后台迁移 worker。

运行：python3 -m workbench.server --db workbench.db --port 8765
"""
from __future__ import annotations

import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import ingest, migrate, query, rules, storage, synth

WEB_DIR = Path(__file__).resolve().parent.parent / "web"


class App:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = storage.connect(db_path)
        storage.init_db(self.conn)
        self._stop = threading.Event()
        self._worker: threading.Thread | None = None

    def start_worker(self, interval: float = 0.3) -> None:
        def loop():
            conn = storage.connect(self.db_path)  # worker 用自己的连接
            while not self._stop.is_set():
                try:
                    migrate.run_pending_segments(conn)
                except Exception:
                    pass  # 单段失败已记录在 segments 表，worker 继续
                self._stop.wait(interval)
        self._worker = threading.Thread(target=loop, daemon=True, name="migrate-worker")
        self._worker.start()

    def stop(self) -> None:
        self._stop.set()
        if self._worker:
            self._worker.join(timeout=2)


def make_handler(app: App):
    class Handler(BaseHTTPRequestHandler):
        server_version = "BucketWorkbench/1.0"

        # ---- 基础 ----
        def _send_json(self, obj, status=200):
            body = json.dumps(obj, ensure_ascii=False, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, path: Path):
            body = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _error(self, exc, status=400):
            self._send_json({"error": str(exc)}, status)

        def _body(self) -> dict:
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n) or b"{}")

        def log_message(self, fmt, *args):  # 静默
            pass

        # ---- GET ----
        def do_GET(self):
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            try:
                if u.path == "/":
                    return self._send_html(WEB_DIR / "index.html")
                if u.path == "/api/state":
                    return self._send_json({
                        "sites": storage.list_sites(app.conn),
                        "rule_versions": storage.list_rule_versions(app.conn),
                        "jobs": migrate.list_jobs(app.conn),
                    })
                if u.path == "/api/query":
                    return self._send_json(query.query(
                        app.conn, q["site"], q.get("rule", "v1"),
                        q["from"], q["to"], q.get("metric")))
                if u.path == "/api/legacy/hourly":
                    return self._send_json(query.legacy_hourly(
                        app.conn, q["site"], q["date"], int(q["hour"])))
                if u.path == "/api/attribute":
                    return self._send_json(query.attribute(app.conn, q["site"], q["ts"]))
                if u.path == "/api/migrations":
                    return self._send_json({
                        "jobs": migrate.list_jobs(app.conn),
                        "segments": migrate.list_segments(
                            app.conn, int(q["job_id"]) if "job_id" in q else None),
                    })
                if u.path == "/api/synth/verify":
                    return self._send_json(synth.verify(app.conn, q["site"], q["rule"]))
                return self._send_json({"error": "not found"}, 404)
            except (rules.RuleError, KeyError, ValueError) as exc:
                return self._error(exc)

        # ---- POST ----
        def do_POST(self):
            u = urlparse(self.path)
            try:
                body = self._body()
                if u.path == "/api/rules":
                    kind = body.get("kind")
                    if kind == "local_hour":
                        rule = rules.RuleVersion.local_hour(body["rule_version"])
                    elif kind == "business_shift":
                        rule = rules.RuleVersion.business_shift(
                            body["rule_version"], body["shifts"],
                            body.get("day_start", "00:00"),
                            body.get("attribution", "start_day"))
                    else:
                        raise rules.RuleError(f"unknown kind: {kind!r}")
                    storage.create_rule_version(app.conn, rule, body.get("activated_utc"))
                    return self._send_json({"created": rule.rule_version})
                if u.path == "/api/ingest":
                    return self._send_json(ingest.ingest_events(app.conn, body["events"]))
                if u.path == "/api/synth/generate":
                    events = synth.synth_events()
                    res = ingest.ingest_events(app.conn, events)
                    legacy = None
                    if body.get("with_legacy", True):
                        legacy = synth.legacy_import(
                            app.conn, "site-ny", "2026-09-01T00:00:00Z",
                            "2026-09-03T00:00:00Z")
                    bad = synth.inject_bad_event(app.conn) if body.get("with_bad_event") else None
                    return self._send_json({"ingest": res, "legacy": legacy,
                                            "bad_event": bad})
                if u.path == "/api/migrations":
                    job_id = migrate.create_job(
                        app.conn, body["rule_version"], body["site"],
                        body["from"], body["to"], int(body.get("segment_minutes", 360)))
                    return self._send_json({"job_id": job_id})
                if u.path == "/api/migrations/run":
                    processed = []
                    while True:
                        batch = migrate.run_pending_segments(app.conn, limit=50)
                        processed += batch
                        if not batch:
                            break
                    return self._send_json({"processed": processed})
                if u.path.startswith("/api/segments/") and u.path.endswith("/retry"):
                    seg_id = int(u.path.split("/")[3])
                    migrate.retry_segment(app.conn, seg_id)
                    return self._send_json({"segment_id": seg_id, "status": "pending"})
                if u.path == "/api/events/fix":
                    return self._send_json(migrate.fix_event(
                        app.conn, body["event_id"], body.get("ts_utc")))
                return self._send_json({"error": "not found"}, 404)
            except (rules.RuleError, KeyError, ValueError) as exc:
                return self._error(exc)

    return Handler


def serve(db_path: str, port: int) -> None:
    app = App(db_path)
    app.start_worker()
    httpd = ThreadingHTTPServer(("127.0.0.1", port), make_handler(app))
    print(f"迁移工作台: http://127.0.0.1:{port}  (db={db_path})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.stop()
        httpd.server_close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="workbench.db")
    p.add_argument("--port", type=int, default=8765)
    args = p.parse_args()
    serve(args.db, args.port)


if __name__ == "__main__":
    main()
