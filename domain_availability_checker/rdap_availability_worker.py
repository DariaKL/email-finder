#!/usr/bin/env python3
"""Resumable RDAP classification for available Airtable domains."""

from __future__ import annotations

import argparse
import os
import signal
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote, urlsplit

import requests
from pyairtable import Api


BASE_DIR = Path(__file__).resolve().parent
ENV_FILES = (BASE_DIR.parent / ".env", BASE_DIR / ".env")
BASE_ID = "appwWjm2yYVF7zCUV"
TABLE_ID = "tblCHztOhSwA9apim"
VIEW_ID = "viwHZvMVHew4eal57"
DOMAIN_FIELD = "Domain"
INSTANT_FIELD = "instantdomainsearch.com"
RDAP_FIELD = "RDAP"
BOOTSTRAP_URL = "https://data.iana.org/rdap/dns.json"
MAX_WORKERS = 12
PER_REGISTRY_INTERVAL_SEC = 1.0
MAX_RETRIES = 4
BATCH_SIZE = 10
STOP_REQUESTED = threading.Event()


def load_local_env() -> None:
    for env_file in ENV_FILES:
        if not env_file.exists():
            continue
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("\'")
            if key and value:
                os.environ.setdefault(key, value)


def normalize_domain(value: object) -> str:
    text = str(value or "").strip().lower()
    if text.startswith(("http://", "https://")):
        text = urlsplit(text).hostname or ""
    text = text.split("/", 1)[0].strip(".")
    return text.encode("idna").decode("ascii") if text else ""


def load_bootstrap() -> dict[str, tuple[str, ...]]:
    response = requests.get(BOOTSTRAP_URL, timeout=(10, 30))
    response.raise_for_status()
    bootstrap = response.json()
    services: dict[str, tuple[str, ...]] = {}
    for suffixes, urls in bootstrap.get("services", []):
        endpoints = tuple(str(url).rstrip("/") for url in urls if str(url).startswith("https://"))
        if not endpoints:
            continue
        for suffix in suffixes:
            services[str(suffix).lower().lstrip(".")] = endpoints
    return services


def endpoint_for(domain: str, services: dict[str, tuple[str, ...]]) -> str | None:
    labels = domain.lower().split(".")
    for index in range(len(labels)):
        suffix = ".".join(labels[index:])
        endpoints = services.get(suffix)
        if endpoints:
            return endpoints[0]
    return None


class RegistryRateLimiter:
    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
        self._last_request: dict[str, float] = defaultdict(float)

    def get(self, endpoint: str, domain: str) -> requests.Response:
        host = urlsplit(endpoint).netloc.lower()
        with self._locks[host]:
            wait_for = PER_REGISTRY_INTERVAL_SEC - (time.monotonic() - self._last_request[host])
            if wait_for > 0:
                time.sleep(wait_for)
            response = requests.get(
                f"{endpoint}/domain/{quote(domain, safe='.-')}",
                headers={"Accept": "application/rdap+json, application/json"},
                timeout=(10, 30),
            )
            self._last_request[host] = time.monotonic()
            return response


def retry_delay(response: requests.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After", "").strip()
        if retry_after.isdigit():
            return min(120.0, float(retry_after))
        if retry_after:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                return min(120.0, max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds()))
            except (TypeError, ValueError, OverflowError):
                pass
    return min(60.0, 2.0 ** attempt)


def classify_domain(
    domain: str,
    services: dict[str, tuple[str, ...]],
    limiter: RegistryRateLimiter,
) -> tuple[str | None, str]:
    endpoint = endpoint_for(domain, services)
    if endpoint is None:
        return None, "unsupported"

    for attempt in range(MAX_RETRIES + 1):
        if STOP_REQUESTED.is_set():
            return None, "stopped"
        response = None
        try:
            response = limiter.get(endpoint, domain)
            if response.status_code == 200:
                payload = response.json()
                if payload.get("objectClassName") == "domain":
                    return "busy", "classified"
                return None, "unexpected_200"
            if response.status_code == 404:
                try:
                    payload = response.json()
                except ValueError:
                    return None, "ambiguous_404"
                if payload.get("errorCode") == 404:
                    return "free", "classified"
                return None, "ambiguous_404"
            if response.status_code not in {429, 500, 502, 503, 504}:
                return None, f"http_{response.status_code}"
        except (requests.RequestException, ValueError):
            if attempt == MAX_RETRIES:
                return None, "network_or_json_error"

        if attempt < MAX_RETRIES:
            time.sleep(retry_delay(response, attempt))

    return None, "retry_exhausted"


def load_targets(table, recheck_existing: bool) -> tuple[dict[str, list[str]], int]:
    formula = f"{{{INSTANT_FIELD}}}='available'"
    if not recheck_existing:
        formula = f"AND({formula},{{{RDAP_FIELD}}}=BLANK())"
    records = table.all(
        formula=formula,
        view=VIEW_ID,
        fields=[DOMAIN_FIELD, INSTANT_FIELD, RDAP_FIELD],
    )
    domains: dict[str, list[str]] = defaultdict(list)
    for record in records:
        fields = record.get("fields", {})
        domain = normalize_domain(fields.get(DOMAIN_FIELD))
        if domain:
            domains[domain].append(record["id"])
    return domains, len(records)


def write_batch(table, pending: list[dict]) -> None:
    if pending:
        table.batch_update(pending, typecast=True)
        time.sleep(0.22)


def run(recheck_existing: bool = False) -> int:
    load_local_env()
    token = os.environ.get("AIRTABLE_TOKEN", "").strip()
    if not token:
        print("AIRTABLE_TOKEN is not configured.", flush=True)
        return 1

    STOP_REQUESTED.clear()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: STOP_REQUESTED.set())
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda _signum, _frame: STOP_REQUESTED.set())

    table = Api(token).table(BASE_ID, TABLE_ID)
    targets, candidate_records = load_targets(table, recheck_existing)
    total_domains = len(targets)
    print(
        f"Available records selected: {candidate_records}; unique domains queued: {total_domains}; "
        f"mode: {'recheck existing' if recheck_existing else 'resume blank RDAP only' }.",
        flush=True,
    )
    if not targets:
        print("No available domains need RDAP checks.", flush=True)
        return 0

    print("Loading IANA RDAP bootstrap...", flush=True)
    services = load_bootstrap()
    limiter = RegistryRateLimiter()
    counts: Counter[str] = Counter()
    pending_updates: list[dict] = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(classify_domain, domain, services, limiter): domain
            for domain in targets
        }
        for future in as_completed(futures):
            domain = futures[future]
            status, result = future.result()
            completed += 1
            counts[result] += 1
            if status:
                pending_updates.extend(
                    {"id": record_id, "fields": {RDAP_FIELD: status}}
                    for record_id in targets[domain]
                )
                if len(pending_updates) >= BATCH_SIZE:
                    write_batch(table, pending_updates)
                    counts["written"] += len(pending_updates)
                    pending_updates.clear()

            if completed % 250 == 0:
                print(
                    f"Progress {completed}/{total_domains}; free={counts['free']}; "
                    f"busy={counts['busy']}; written={counts['written']}; "
                    f"unsupported={counts['unsupported']}; unresolved="
                    f"{completed - counts['free'] - counts['busy'] - counts['unsupported']}.",
                    flush=True,
                )
            if STOP_REQUESTED.is_set():
                break

    if pending_updates and not STOP_REQUESTED.is_set():
        write_batch(table, pending_updates)
        counts["written"] += len(pending_updates)

    unresolved = completed - counts["free"] - counts["busy"] - counts["unsupported"]
    print(
        f"Finished: processed={completed}/{total_domains}; written={counts['written']}; "
        f"free={counts['free']}; busy={counts['busy']}; "
        f"unsupported={counts['unsupported']}; unresolved={unresolved}; "
        f"stopped={STOP_REQUESTED.is_set()}.",
        flush=True,
    )
    return 0 if not unresolved and not STOP_REQUESTED.is_set() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Check available Airtable domains through registry RDAP.")
    parser.add_argument(
        "--recheck-existing",
        action="store_true",
        help="Recheck and overwrite existing RDAP values as well as blanks.",
    )
    args = parser.parse_args()
    return run(recheck_existing=args.recheck_existing)


if __name__ == "__main__":
    raise SystemExit(main())
