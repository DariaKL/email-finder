#!/usr/bin/env python3
"""Resumable RDAP classification for available Airtable domains."""

from __future__ import annotations

import argparse
import os
import signal
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from queue import Empty, Queue
from urllib.parse import quote, urlsplit

import requests
from pyairtable import Api


BASE_DIR = Path(__file__).resolve().parent
ENV_FILES = (BASE_DIR.parent / ".env", BASE_DIR / ".env")
BASE_ID = "appwWjm2yYVF7zCUV"
TABLE_ID = "tblR1iWy4L0uIqREg"
DOMAIN_FIELD = "Domain"
INSTANT_FIELD = "instantdomainsearch.com"
RDAP_FIELD = "RDAP"
BOOTSTRAP_URL = "https://data.iana.org/rdap/dns.json"
MAX_WORKERS = 12
PER_REGISTRY_INTERVAL_SEC = 1.0
MAX_RETRIES = 4
MAX_CONSECUTIVE_REGISTRY_FAILURES = 2
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
    response = requests.get(
        BOOTSTRAP_URL,
        headers={"Accept": "application/json"},
        timeout=(10, 30),
    )
    response.raise_for_status()
    bootstrap = response.json()
    if not isinstance(bootstrap, dict):
        raise ValueError("IANA RDAP bootstrap must be a JSON object")

    services: dict[str, tuple[str, ...]] = {}
    for service in bootstrap.get("services", []):
        if not isinstance(service, list) or len(service) != 2:
            continue
        suffixes, urls = service
        if not isinstance(suffixes, list) or not isinstance(urls, list):
            continue
        endpoints = tuple(
            url.rstrip("/")
            for url in urls
            if isinstance(url, str) and url.lower().startswith("https://")
        )
        if not endpoints:
            continue
        for suffix in suffixes:
            services[str(suffix).lower().lstrip(".")] = endpoints
    if not services:
        raise ValueError("RDAP bootstrap contains no usable HTTPS services")
    return services


def endpoints_for(domain: str, services: dict[str, tuple[str, ...]]) -> tuple[str, ...]:
    labels = domain.lower().split(".")
    for index in range(len(labels)):
        suffix = ".".join(labels[index:])
        endpoints = services.get(suffix)
        if endpoints:
            return endpoints
    return ()


def endpoint_for(domain: str, services: dict[str, tuple[str, ...]]) -> str | None:
    endpoints = endpoints_for(domain, services)
    return endpoints[0] if endpoints else None


class RegistryRateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_request: float | None = None

    def get(self, endpoint: str, domain: str) -> requests.Response:
        with self._lock:
            if self._last_request is not None:
                wait_for = PER_REGISTRY_INTERVAL_SEC - (time.monotonic() - self._last_request)
                if wait_for > 0:
                    time.sleep(wait_for)
            self._last_request = time.monotonic()
            response = requests.get(
                f"{endpoint}/domain/{quote(domain, safe='.-')}",
                headers={"Accept": "application/rdap+json, application/json"},
                timeout=(5, 10),
            )
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
    endpoints = endpoints_for(domain, services)
    if not endpoints:
        return "need to check", "unsupported"

    for attempt in range(MAX_RETRIES + 1):
        if STOP_REQUESTED.is_set():
            return None, "stopped"
        response = None
        only_connect_timeouts = True
        last_result = "retry_exhausted"
        for endpoint in endpoints:
            if STOP_REQUESTED.is_set():
                return None, "stopped"
            try:
                response = limiter.get(endpoint, domain)
            except requests.ConnectTimeout:
                last_result = "connect_timeout"
                continue
            except requests.RequestException:
                only_connect_timeouts = False
                last_result = "network_or_json_error"
                continue

            only_connect_timeouts = False
            if response.status_code == 200:
                try:
                    payload = response.json()
                except ValueError:
                    last_result = "network_or_json_error"
                    continue
                if isinstance(payload, dict) and payload.get("objectClassName") == "domain":
                    return "busy", "classified"
                last_result = "unexpected_200"
                continue
            if response.status_code == 404:
                try:
                    payload = response.json()
                except ValueError:
                    return "free", "classified"
                if isinstance(payload, dict) and payload.get("errorCode") == 404:
                    return "free", "classified"
                last_result = "ambiguous_404"
                continue
            if response.status_code not in {429, 500, 502, 503, 504}:
                return "need to check", f"http_{response.status_code}"

        if only_connect_timeouts:
            return "need to check", "connect_timeout"

        if attempt < MAX_RETRIES:
            time.sleep(retry_delay(response, attempt))

    return "need to check", last_result


def load_targets(
    table,
    recheck_existing: bool,
    recheck_need_to_check: bool = False,
) -> tuple[dict[str, list[str]], int]:
    formula = f"{{{INSTANT_FIELD}}}='available'"
    if recheck_need_to_check:
        formula = f"AND({formula},{{{RDAP_FIELD}}}='need to check')"
    elif not recheck_existing:
        formula = f"AND({formula},{{{RDAP_FIELD}}}=BLANK())"
    records = table.all(formula=formula, fields=[DOMAIN_FIELD, INSTANT_FIELD, RDAP_FIELD])
    domains: dict[str, list[str]] = defaultdict(list)
    for record in records:
        fields = record.get("fields", {})
        domain = normalize_domain(fields.get(DOMAIN_FIELD))
        if domain:
            domains[domain].append(record["id"])
    return domains, len(records)


def group_targets_by_registry(
    targets: dict[str, list[str]],
    services: dict[str, tuple[str, ...]],
) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = defaultdict(list)
    for domain in targets:
        endpoint = endpoint_for(domain, services)
        registry = (
            urlsplit(endpoint).netloc.lower()
            if endpoint
            else f"unsupported:{domain.rsplit('.', 1)[-1]}"
        )
        groups[registry].append(domain)
    return groups


def check_registry_group(domains, services, limiter, results) -> None:
    consecutive_failures = 0
    registry_unavailable = False
    host = "unknown"
    if domains:
        endpoint = endpoint_for(domains[0], services)
        if endpoint:
            host = urlsplit(endpoint).netloc.lower()

    for domain in domains:
        if STOP_REQUESTED.is_set():
            return
        if registry_unavailable:
            status, result = "need to check", "registry_unavailable"
        else:
            try:
                status, result = classify_domain(domain, services, limiter)
            except Exception as exc:
                status, result = "need to check", f"worker_error:{type(exc).__name__}"

            if result in {"connect_timeout", "network_or_json_error", "retry_exhausted"}:
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_REGISTRY_FAILURES:
                    registry_unavailable = True
                    print(
                        f"RDAP registry {host} failed {consecutive_failures} consecutive checks; "
                        "marking remaining domains need to check for Netim fallback.",
                        flush=True,
                    )
            else:
                consecutive_failures = 0
        results.put((domain, status, result))


def write_batch(table, pending: list[dict]) -> None:
    if pending:
        table.batch_update(pending, typecast=True)
        time.sleep(0.22)


def run(
    recheck_existing: bool = False,
    recheck_need_to_check: bool = False,
) -> int:
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
    targets, candidate_records = load_targets(
        table,
        recheck_existing,
        recheck_need_to_check,
    )
    total_domains = len(targets)
    mode = (
        "recheck need to check"
        if recheck_need_to_check
        else "recheck existing"
        if recheck_existing
        else "resume blank RDAP only"
    )
    print(
        f"Available records selected: {candidate_records}; unique domains queued: {total_domains}; "
        f"mode: {mode}.",
        flush=True,
    )
    if not targets:
        print("No available domains need RDAP checks.", flush=True)
        return 0

    print("Loading IANA RDAP bootstrap...", flush=True)
    try:
        services = load_bootstrap()
    except (OSError, requests.RequestException, ValueError) as exc:
        print(f"Could not load RDAP bootstrap: {exc}", flush=True)
        return 1
    print(f"RDAP bootstrap loaded: {len(services)} suffixes; starting checks.", flush=True)
    limiter = RegistryRateLimiter()
    registry_groups = group_targets_by_registry(targets, services)
    results = Queue()
    counts: Counter[str] = Counter()
    pending_updates: list[dict] = []
    completed = 0
    print(
        f"Checks scheduled across {len(registry_groups)} registry groups.",
        flush=True,
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            pool.submit(check_registry_group, domains, services, limiter, results)
            for domains in registry_groups.values()
        ]
        while completed < total_domains and not STOP_REQUESTED.is_set():
            try:
                domain, status, result = results.get(timeout=1)
            except Empty:
                if all(future.done() for future in futures):
                    for future in futures:
                        future.result()
                    break
                continue
            completed += 1
            counts[result] += 1
            if status:
                counts[status] += 1
                if recheck_need_to_check and status == "need to check":
                    counts["unchanged"] += len(targets[domain])
                else:
                    pending_updates.extend(
                        {"id": record_id, "fields": {RDAP_FIELD: status}}
                        for record_id in targets[domain]
                    )
                if len(pending_updates) >= BATCH_SIZE:
                    write_batch(table, pending_updates)
                    counts["written"] += len(pending_updates)
                    pending_updates.clear()

            if completed % 10 == 0 or completed == total_domains:
                print(
                    f"Progress {completed}/{total_domains}; free={counts['free']}; "
                    f"busy={counts['busy']}; written={counts['written']}; "
                    f"need_to_check={counts['need to check']}; unchanged={counts['unchanged']}; unresolved="
                    f"{completed - counts['free'] - counts['busy'] - counts['need to check']}.",
                    flush=True,
                )
            if STOP_REQUESTED.is_set():
                break

    if pending_updates and not STOP_REQUESTED.is_set():
        write_batch(table, pending_updates)
        counts["written"] += len(pending_updates)

    unresolved = completed - counts["free"] - counts["busy"] - counts["need to check"]
    print(
        f"Finished: processed={completed}/{total_domains}; written={counts['written']}; "
        f"free={counts['free']}; busy={counts['busy']}; "
        f"need_to_check={counts['need to check']}; unchanged={counts['unchanged']}; unresolved={unresolved}; "
        f"stopped={STOP_REQUESTED.is_set()}.",
        flush=True,
    )
    return 0 if not unresolved and not STOP_REQUESTED.is_set() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Check available Airtable domains through registry RDAP.")
    recheck_group = parser.add_mutually_exclusive_group()
    recheck_group.add_argument(
        "--recheck-existing",
        action="store_true",
        help="Recheck and overwrite existing RDAP values as well as blanks.",
    )
    recheck_group.add_argument(
        "--recheck-need-to-check",
        action="store_true",
        help="Recheck only available domains whose RDAP value is 'need to check'.",
    )
    args = parser.parse_args()
    return run(
        recheck_existing=args.recheck_existing,
        recheck_need_to_check=args.recheck_need_to_check,
    )


if __name__ == "__main__":
    raise SystemExit(main())
