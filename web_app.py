#!/usr/bin/env python3
"""
Email Finder — Web App (Flask + SSE)
4 Airtable tabs: Anna, Vlad, Mykola, Khrystia
"""

import os
import sys
import json
import time
import uuid
import asyncio
import signal
import subprocess
import threading
import urllib.parse
from collections import deque
from pathlib import Path
import requests as http_requests
from flask import Flask, render_template, request, Response, jsonify, stream_with_context

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)

# ── CONFIG ──────────────────────────────────────────────────

AIRTABLE_BASE = os.environ.get("AIRTABLE_BASE", "")
AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN", "")
AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}

TABLES = ["Anna", "Vlad", "Mykola", "Khrystia"]

PARALLEL = 3
DEADLINE = 20

# ── TASK STATE ──────────────────────────────────────────────

tasks = {}  # task_id -> {...}

DOMAIN_CHECKER_DIR = Path(__file__).resolve().parent / "domain_availability_checker"
DOMAIN_STATUS_MODES = {
    "instant": "--instant-only",
    "netim": "--netim-only",
    "rdap": None,
}
domain_status_processes = {}
domain_status_logs = {mode: deque(maxlen=300) for mode in DOMAIN_STATUS_MODES}
domain_status_lock = threading.RLock()


def _domain_status_python():
    candidates = (
        DOMAIN_CHECKER_DIR / ".venv" / "Scripts" / "python.exe",
        DOMAIN_CHECKER_DIR / ".venv" / "bin" / "python",
    )
    return str(next((path for path in candidates if path.exists()), Path(sys.executable)))


def _domain_status_environment():
    env = os.environ.copy()
    root_env = Path(__file__).resolve().parent / ".env"
    if root_env.exists():
        for raw_line in root_env.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() == "AIRTABLE_TOKEN":
                value = value.strip().strip('"').strip("'")
                if value:
                    env["AIRTABLE_TOKEN"] = value
                break
    return env


def _read_domain_status_output(mode, process):
    if process.stdout is not None:
        for line in process.stdout:
            with domain_status_lock:
                domain_status_logs[mode].append(line.rstrip())
    process.wait()


def _cleanup_old_tasks():
    """Remove completed tasks older than 30 minutes to free memory."""
    import gc
    cutoff = time.time() - 1800
    dead = [tid for tid, t in list(tasks.items())
            if t.get("status") in ("done", "stopped")
            and t.get("created", 0) < cutoff]
    for tid in dead:
        tasks.pop(tid, None)
    if dead:
        gc.collect()


# ── AIRTABLE HELPERS ────────────────────────────────────────

def airtable_fetch_empty(table_name):
    records = []
    offset = None
    formula = urllib.parse.quote("OR({Emails}='',{Emails}=BLANK())")
    while True:
        url = (f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{table_name}"
               f"?pageSize=100&filterByFormula={formula}")
        if offset:
            url += f"&offset={offset}"
        r = http_requests.get(url, headers=AIRTABLE_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        time.sleep(0.22)
    return records


def airtable_count_fast(table_name):
    """Quick estimate from 1 page only — no pagination delay."""
    url = (f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{table_name}"
           f"?pageSize=100&fields%5B%5D=Emails")
    r = http_requests.get(url, headers=AIRTABLE_HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    batch = data.get("records", [])
    has_email = sum(1 for rec in batch
                    if str(rec.get("fields", {}).get("Emails", "")).strip()
                    not in ("", "None"))
    has_more = "offset" in data
    return len(batch), has_email, has_more


def airtable_batch_update(table_name, updates):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{table_name}"
    records = [{"id": rid, "fields": fields} for rid, fields in updates]
    for i in range(0, len(records), 10):
        batch = records[i:i + 10]
        r = http_requests.patch(url, headers=AIRTABLE_HEADERS,
                                json={"records": batch}, timeout=15)
        r.raise_for_status()
        time.sleep(0.22)


# ── WORKER ──────────────────────────────────────────────────

def run_worker(task_id, table_name):
    task = tasks[task_id]
    task["status"] = "loading"
    task["events"].append(("status", json.dumps({"status": "loading"})))

    try:
        # Quick estimate (1 page, no delay)
        est_total, est_has, has_more = airtable_count_fast(table_name)
        suffix = '+' if has_more else ''
        task["events"].append(("log", json.dumps({
            "msg": f"Table '{table_name}': ~{est_total}{suffix} total, ~{est_has}{suffix} with email"
        })))

        # Fetch empty records
        task["events"].append(("log", json.dumps({"msg": "Loading records without email..."})))
        pending = airtable_fetch_empty(table_name)
        pending = [r for r in pending if r.get("fields", {}).get("Domain", "").strip()]
        need = len(pending)

        task["total"] = need
        task["events"].append(("counts", json.dumps({
            "need": need, "has": est_has, "total": est_total
        })))
        task["events"].append(("log", json.dumps({"msg": f"Need email: {need} domains"})))

        if need == 0:
            task["status"] = "done"
            task["events"].append(("status", json.dumps({"status": "done"})))
            return

        task["status"] = "running"
        task["events"].append(("status", json.dumps({"status": "running"})))

        # Process
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(
                _process_all(task_id, table_name, pending, need, est_has, loop)
            )
        finally:
            loop.close()

    except Exception as e:
        task["events"].append(("error", json.dumps({"msg": str(e)})))

    finally:
        import gc
        if task.get("stop"):
            task["status"] = "stopped"
            task["events"].append(("status", json.dumps({"status": "stopped"})))
        elif task["status"] != "done":
            task["status"] = "done"
            task["events"].append(("status", json.dumps({"status": "done"})))
        gc.collect()


async def _process_all(task_id, table_name, pending, need, has_email, loop):
    import aiohttp
    from email_finder import crawl, CrawlContext

    task = tasks[task_id]
    processed = 0
    found_count = 0
    not_found_count = 0

    connector = aiohttp.TCPConnector(limit=30, ssl=False)
    session = aiohttp.ClientSession(connector=connector)

    try:
        for i in range(0, len(pending), PARALLEL):
            if task.get("stop"):
                break

            batch = pending[i:i + PARALLEL]
            coros = []
            for rec in batch:
                domain = rec["fields"]["Domain"].strip()
                coros.append(_crawl_one(domain, crawl, CrawlContext, session, task))

            results = await asyncio.gather(*coros, return_exceptions=True)

            at_updates = []
            for j, (rec, result) in enumerate(zip(batch, results)):
                if task.get("stop"):
                    break

                domain = rec["fields"]["Domain"].strip()
                rec_id = rec["id"]

                if isinstance(result, Exception):
                    emails_str = "not found"
                else:
                    emails_str = result

                if not emails_str or emails_str == "not found":
                    not_found_count += 1
                    emails_str = "not found"
                    status_msg = "not_found"
                else:
                    found_count += 1
                    status_msg = "found"

                at_updates.append((rec_id, {"Emails": emails_str}))
                processed += 1
                has_email += 1

                task["processed"] = processed
                task["events"].append(("result", json.dumps({
                    "domain": domain,
                    "emails": emails_str,
                    "status": status_msg,
                    "processed": processed,
                    "total": need,
                    "found": found_count,
                    "not_found": not_found_count,
                })))

            # Save to Airtable
            if at_updates:
                try:
                    await loop.run_in_executor(None, airtable_batch_update,
                                               table_name, at_updates)
                    task["events"].append(("log", json.dumps({
                        "msg": f"Saved batch: {len(at_updates)} records to Airtable"
                    })))
                except Exception as e:
                    task["events"].append(("log", json.dumps({
                        "msg": f"[SAVE ERROR] {e}"
                    })))

    finally:
        await session.close()

    task["events"].append(("log", json.dumps({
        "msg": f"DONE: Processed={processed}, Found={found_count}, Not found={not_found_count}"
    })))
    task["status"] = "done"
    task["events"].append(("status", json.dumps({"status": "done"})))


async def _crawl_one(domain, crawl_fn, CrawlCtx, session, task):
    task["events"].append(("log", json.dumps({"msg": f"Crawling: {domain}"})))

    ctx = CrawlCtx(max_reqs=60, deadline_sec=DEADLINE)
    result = await crawl_fn(domain, ctx, session=session, smtp=False)

    best = result.get("best", [])
    all_found = result.get("all", [])

    task["events"].append(("log", json.dumps({
        "msg": f"  {domain}: {len(all_found)} found, {len(best)} best | Reqs: {result.get('requests', '?')}"
    })))

    if best:
        return ", ".join(best)
    return "not found"


# ── ROUTES ──────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", tables=TABLES)


@app.route("/start", methods=["POST"])
def start():
    data = request.json or {}
    table_name = data.get("table", "Anna")
    if table_name not in TABLES:
        return jsonify({"error": "Invalid table"}), 400

    _cleanup_old_tasks()

    task_id = str(uuid.uuid4())[:8]
    tasks[task_id] = {
        "table": table_name,
        "status": "starting",
        "processed": 0,
        "total": 0,
        "stop": False,
        "events": [],
        "cursor": 0,
        "created": time.time(),
    }

    t = threading.Thread(target=run_worker, args=(task_id, table_name), daemon=True)
    t.start()
    return jsonify({"task_id": task_id})


@app.route("/stop/<task_id>", methods=["POST"])
def stop(task_id):
    if task_id in tasks:
        tasks[task_id]["stop"] = True
        return jsonify({"ok": True})
    return jsonify({"error": "Not found"}), 404


@app.route("/events/<task_id>")
def events(task_id):
    if task_id not in tasks:
        return jsonify({"error": "Not found"}), 404

    def stream():
        cursor = 0
        while True:
            task = tasks.get(task_id)
            if not task:
                break

            while cursor < len(task["events"]):
                evt_type, evt_data = task["events"][cursor]
                cursor += 1
                yield f"event: {evt_type}\ndata: {evt_data}\n\n"

            if task["status"] in ("done", "stopped"):
                yield f"event: done\ndata: {{}}\n\n"
                # Free event memory once streaming is complete
                task["events"] = []
                break

            time.sleep(0.3)

    return Response(
        stream_with_context(stream()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/domain-status/state")
def domain_status_state():
    workers = {}
    with domain_status_lock:
        for mode in DOMAIN_STATUS_MODES:
            process = domain_status_processes.get(mode)
            return_code = process.poll() if process else None
            if process is None:
                status = "idle"
            elif return_code is None:
                status = "running"
            else:
                status = "done" if return_code == 0 else "error"
            workers[mode] = {
                "status": status,
                "pid": process.pid if process else None,
                "return_code": return_code,
                "log": list(domain_status_logs[mode]),
            }
    return jsonify({"workers": workers})


@app.route("/domain-status/start/<mode>", methods=["POST"])
def start_domain_status(mode):
    if mode not in DOMAIN_STATUS_MODES:
        return jsonify({"error": "Invalid checker mode"}), 400

    script_name = "rdap_availability_worker.py" if mode == "rdap" else "airtable_donor_status.py"
    script = DOMAIN_CHECKER_DIR / script_name
    if not script.is_file():
        return jsonify({"error": "Airtable status worker is not installed"}), 503

    child_env = _domain_status_environment()
    if not child_env.get("AIRTABLE_TOKEN", "").strip():
        return jsonify({"error": "AIRTABLE_TOKEN is not configured on the web server"}), 503

    with domain_status_lock:
        for running_mode, process in domain_status_processes.items():
            if process.poll() is None:
                return jsonify({
                    "error": f"A status check is already running ({running_mode})",
                    "mode": running_mode,
                    "pid": process.pid,
                }), 409

        domain_status_logs[mode].clear()
        domain_status_logs[mode].append("Starting checker; connecting to Airtable...")
        command = [
            _domain_status_python(),
            "-u",
            "-W",
            "ignore",
            str(script),
        ]
        if mode == "rdap":
            options_body = request.get_json(silent=True) or {}
            if options_body.get("recheck_existing") and options_body.get("recheck_need_to_check"):
                return jsonify({"error": "RDAP recheck modes are mutually exclusive"}), 400
            if options_body.get("recheck_existing"):
                command.append("--recheck-existing")
            elif options_body.get("recheck_need_to_check"):
                command.append("--recheck-need-to-check")
        else:
            command.append(DOMAIN_STATUS_MODES[mode])
        options = {
            "cwd": str(DOMAIN_CHECKER_DIR),
            "env": child_env,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
            "bufsize": 1,
        }
        if os.name == "nt":
            options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            options["start_new_session"] = True
        try:
            process = subprocess.Popen(command, **options)
        except OSError as error:
            return jsonify({"error": f"Could not start status checker: {error}"}), 500
        domain_status_processes[mode] = process

    threading.Thread(target=_read_domain_status_output, args=(mode, process), daemon=True).start()
    return jsonify({"ok": True, "mode": mode, "pid": process.pid})


@app.route("/domain-status/stop/<mode>", methods=["POST"])
def stop_domain_status(mode):
    if mode not in DOMAIN_STATUS_MODES:
        return jsonify({"error": "Invalid checker mode"}), 400

    with domain_status_lock:
        process = domain_status_processes.get(mode)
        if process is None or process.poll() is not None:
            return jsonify({"error": "No running status check"}), 409
        try:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=True,
                )
            else:
                os.killpg(process.pid, signal.SIGTERM)
        except (OSError, subprocess.SubprocessError) as error:
            return jsonify({"error": f"Could not stop status checker: {error}"}), 500
    return jsonify({"ok": True, "mode": mode})


@app.route("/stats/<table_name>")
def stats(table_name):
    if table_name not in TABLES:
        return jsonify({"error": "Invalid table"}), 400
    try:
        # Quick estimate: only 1 page to avoid timeout
        url = (f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{table_name}"
               f"?pageSize=100&fields%5B%5D=Emails")
        r = http_requests.get(url, headers=AIRTABLE_HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        batch = data.get("records", [])
        has_email = sum(1 for rec in batch
                        if str(rec.get("fields", {}).get("Emails", "")).strip()
                        not in ("", "None"))
        has_more = "offset" in data
        total_est = len(batch)
        suffix = "+" if has_more else ""
        return jsonify({
            "total": f"{total_est}{suffix}",
            "has_email": f"{has_email}{suffix}",
            "need": f"{total_est - has_email}{suffix}",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── CLEANUP old tasks ───────────────────────────────────────

def cleanup_tasks():
    while True:
        time.sleep(600)
        now = time.time()
        to_del = [tid for tid, t in tasks.items()
                  if t["status"] in ("done", "stopped")]
        # keep max 50
        if len(to_del) > 50:
            for tid in to_del[:len(to_del) - 50]:
                tasks.pop(tid, None)

threading.Thread(target=cleanup_tasks, daemon=True).start()


# ── MAIN ────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
