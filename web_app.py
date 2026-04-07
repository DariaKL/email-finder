#!/usr/bin/env python3
"""
Email Finder — Web App (Flask + SSE)
4 Airtable tabs: Anna, Dasha, Mykola, Khrystia
"""

import os
import sys
import json
import time
import uuid
import asyncio
import threading
import urllib.parse
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

TABLES = ["Anna", "Dasha", "Mykola", "Khrystia"]

PARALLEL = 6
DEADLINE = 20

# ── TASK STATE ──────────────────────────────────────────────

tasks = {}  # task_id -> {...}


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
                _process_all(task_id, table_name, pending, need, has_email, loop)
            )
        finally:
            loop.close()

    except Exception as e:
        task["events"].append(("error", json.dumps({"msg": str(e)})))

    finally:
        if task.get("stop"):
            task["status"] = "stopped"
            task["events"].append(("status", json.dumps({"status": "stopped"})))
        elif task["status"] != "done":
            task["status"] = "done"
            task["events"].append(("status", json.dumps({"status": "done"})))


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

    task_id = str(uuid.uuid4())[:8]
    tasks[task_id] = {
        "table": table_name,
        "status": "starting",
        "processed": 0,
        "total": 0,
        "stop": False,
        "events": [],
        "cursor": 0,
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
