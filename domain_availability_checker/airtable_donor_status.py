#!/usr/bin/env python3
"""Check Airtable donor domains on Instant and Netim without moving records."""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from pyairtable import Api
from playwright.sync_api import sync_playwright

from instant_bulk_to_sheets import scrape_batch_statuses as scrape_instant_batch
from netim_bulk_to_sheets import (
    NetimAccessError,
    NetimRateLimitError,
    scrape_batch_statuses as scrape_netim_batch,
)


BASE_DIR = Path(__file__).resolve().parent
ENV_FILES = (BASE_DIR.parent / ".env", BASE_DIR / ".env")
SETTINGS = {
    "base_id": "appwWjm2yYVF7zCUV",
    "table_id": "tblR1iWy4L0uIqREg",
    "field_domain": "Domain",
    "field_rdap": "RDAP",
    "field_instant": "instantdomainsearch.com",
    "field_netim": "Netim",
    "field_netim_price": "Netim Price",
    "instant_batch_size": 500,
    "netim_batch_size": 100,
    "pause_between_batches_sec": 120,
    "rate_limit_pause_sec": 900,
    "max_batch_attempts": 3,
    "instant_timeout_sec": 60,
    "netim_timeout_sec": 45,
    "headless": True,
}


class InstantNoResultsError(RuntimeError):
    pass


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
            value = value.strip().strip('"').strip("'")
            if key and value:
                os.environ.setdefault(key, value)


def normalize_domain(value: object) -> str:
    text = str(value or "").strip().lower()
    if text.startswith(("http://", "https://")):
        text = text.split("://", 1)[1]
    return text.split("/")[0].strip(".")


def chunks(items: list, size: int) -> list[list]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def update_airtable(table, updates: list[dict]) -> None:
    for batch in chunks(updates, 10):
        table.batch_update(batch, typecast=True)
        time.sleep(0.22)


def save_statuses(table, records: list[dict], field_name: str, statuses: dict[str, str]) -> int:
    updates = []
    for record in records:
        domain = normalize_domain(record.get("fields", {}).get(SETTINGS["field_domain"]))
        status = statuses.get(domain)
        if status:
            updates.append({"id": record["id"], "fields": {field_name: status}})
    update_airtable(table, updates)
    return len(updates)


def save_prices(table, records: list[dict], prices: dict[str, float]) -> int:
    updates = []
    price_field = SETTINGS["field_netim_price"]
    for record in records:
        fields = record.get("fields", {})
        domain = normalize_domain(fields.get(SETTINGS["field_domain"]))
        if not str(fields.get(price_field, "") or "").strip() and domain in prices:
            updates.append({"id": record["id"], "fields": {price_field: prices[domain]}})
    update_airtable(table, updates)
    return len(updates)


def select_netim_records(records: list[dict], mode: str) -> list[dict]:
    domain_field = SETTINGS["field_domain"]
    netim_field = SETTINGS["field_netim"]
    if mode == "rdap_need_to_check_netim":
        return [
            record
            for record in records
            if normalize_domain(record.get("fields", {}).get(domain_field))
            and str(record.get("fields", {}).get(SETTINGS["field_instant"], "")).strip().lower()
            == "available"
            and str(record.get("fields", {}).get(SETTINGS["field_rdap"], "")).strip().lower()
            in {"", "need to check"}
        ]
    return [
        record
        for record in records
        if normalize_domain(record.get("fields", {}).get(domain_field))
        and str(record.get("fields", {}).get(SETTINGS["field_instant"], "") or "").strip().lower()
        == "available"
        and not str(record.get("fields", {}).get(netim_field, "") or "").strip()
    ]


def is_auth_error(error: Exception) -> bool:
    response = getattr(error, "response", None)
    return getattr(response, "status_code", None) in {401, 403}


def check_instant(domains: list[str]) -> dict[str, str] | None:
    for attempt in range(1, SETTINGS["max_batch_attempts"] + 1):
        try:
            results = scrape_instant_batch(
                domains,
                headless=SETTINGS["headless"],
                slow_mo_ms=0,
                wait_timeout_sec=SETTINGS["instant_timeout_sec"],
            )
            return {
                domain: status
                for domain, (status, _details) in results.items()
                if status in {"available", "taken"}
            }
        except Exception as error:
            print(f"  Instant attempt {attempt}/{SETTINGS['max_batch_attempts']} failed: {error}", flush=True)
            if attempt < SETTINGS["max_batch_attempts"]:
                print(f"  Retrying in {SETTINGS['pause_between_batches_sec']} seconds.", flush=True)
                time.sleep(SETTINGS["pause_between_batches_sec"])
    return None


def check_netim_batch(
    context,
    domains: list[str],
    price_results: dict[str, float] | None = None,
) -> dict[str, str] | None:
    attempt = 1
    while attempt <= SETTINGS["max_batch_attempts"]:
        page = context.new_page()
        page.set_default_timeout(10000)
        page.set_default_navigation_timeout(60000)
        try:
            return scrape_netim_batch(
                page,
                domains,
                SETTINGS["netim_timeout_sec"],
                price_results=price_results,
            )
        except NetimAccessError as error:
            print(f"  {error}; stopping Netim to avoid unreliable checks.", flush=True)
            return None
        except NetimRateLimitError as error:
            print(
                f"  {error}; retrying this Airtable batch in {SETTINGS['rate_limit_pause_sec']} seconds.",
                flush=True,
            )
            attempt += 1
            if attempt <= SETTINGS["max_batch_attempts"]:
                time.sleep(SETTINGS["rate_limit_pause_sec"])
        except Exception as error:
            print(f"  Netim attempt {attempt}/{SETTINGS['max_batch_attempts']} failed: {error}", flush=True)
            attempt += 1
            if attempt < SETTINGS["max_batch_attempts"]:
                time.sleep(SETTINGS["pause_between_batches_sec"])
        finally:
            page.close()
    return None


def process_table(table, mode: str = "all") -> None:
    records = table.all()
    domain_field = SETTINGS["field_domain"]
    instant_field = SETTINGS["field_instant"]
    netim_field = SETTINGS["field_netim"]
    check_instant_only = mode in {"all", "instant"}
    check_netim_only = mode in {"all", "netim", "rdap_need_to_check_netim"}

    pending_instant = [
        record
        for record in records
        if check_instant_only
        and normalize_domain(record.get("fields", {}).get(domain_field))
        and not str(record.get("fields", {}).get(instant_field, "") or "").strip()
    ]
    pending_netim = select_netim_records(records, mode) if check_netim_only else []
    pending_prices = [
        record
        for record in records
        if mode in {"all", "netim"}
        and normalize_domain(record.get("fields", {}).get(domain_field))
        and str(record.get("fields", {}).get(netim_field, "") or "").strip()
        and not str(record.get("fields", {}).get(SETTINGS["field_netim_price"], "") or "").strip()
    ]

    print(
        f"Airtable records: {len(records)}; Instant pending: {len(pending_instant)}; "
        f"Netim pending: {len(pending_netim)}"
        + (
            " from RDAP need-to-check."
            if mode == "rdap_need_to_check_netim"
            else f"; Instant available: {len(pending_netim)}."
        ),
        flush=True,
    )

    for batch_index, batch in enumerate(chunks(pending_instant, SETTINGS["instant_batch_size"]), start=1):
        domains = [normalize_domain(record["fields"][domain_field]) for record in batch]
        print(f"Instant batch {batch_index}: {len(batch)} donors", flush=True)
        statuses = check_instant(domains)
        if statuses:
            written = save_statuses(table, batch, instant_field, statuses)
            print(f"  Wrote {written} Instant statuses.", flush=True)
            if check_netim_only:
                pending_netim.extend(
                    record
                    for record in batch
                    if statuses.get(normalize_domain(record["fields"][domain_field])) == "available"
                    and not str(record.get("fields", {}).get(netim_field, "") or "").strip()
                )
        else:
            print("  No confirmed Instant results; leaving this batch blank and stopping.", flush=True)
            raise InstantNoResultsError(
                "Instant returned no confirmed results. Check Instant access before retrying."
            )

        if batch_index < len(chunks(pending_instant, SETTINGS["instant_batch_size"])):
            time.sleep(SETTINGS["pause_between_batches_sec"])

    unique_netim: list[dict] = []
    seen_ids: set[str] = set()
    for record in pending_netim:
        if record["id"] not in seen_ids:
            unique_netim.append(record)
            seen_ids.add(record["id"])
    for record in pending_prices:
        if record["id"] not in seen_ids:
            unique_netim.append(record)
            seen_ids.add(record["id"])

    if not unique_netim:
        print("No available donors need a Netim check.", flush=True)
        return

    print(f"Netim checks pending: {len(unique_netim)} donors.", flush=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=SETTINGS["headless"])
        context = browser.new_context()
        try:
            netim_batches = chunks(unique_netim, SETTINGS["netim_batch_size"])
            for batch_index, batch in enumerate(netim_batches, start=1):
                domains = [normalize_domain(record["fields"][domain_field]) for record in batch]
                print(f"Netim batch {batch_index}/{len(netim_batches)}: {len(batch)} donors", flush=True)
                prices: dict[str, float] = {}
                statuses = check_netim_batch(context, domains, prices)
                if statuses is not None:
                    if mode == "rdap_need_to_check_netim":
                        confirmed = {
                            domain: status
                            for domain, status in statuses.items()
                            if status in {"available", "taken"}
                        }
                        status_batch = [
                            record
                            for record in batch
                            if normalize_domain(record.get("fields", {}).get(domain_field)) in confirmed
                        ]
                        written = save_statuses(table, status_batch, netim_field, confirmed)
                        prices_written = 0
                    else:
                        status_batch = [
                            record
                            for record in batch
                            if not str(record.get("fields", {}).get(netim_field, "") or "").strip()
                        ]
                        written = save_statuses(table, status_batch, netim_field, statuses)
                        prices_written = save_prices(table, batch, prices)
                    print(
                        f"  Wrote {written} Netim statuses and {prices_written} Netim prices.",
                        flush=True,
                    )
                else:
                    print("  Netim batch left blank; stopping for a safe restart.", flush=True)
                    break

                if batch_index < len(netim_batches):
                    time.sleep(SETTINGS["pause_between_batches_sec"])
        finally:
            context.close()
            browser.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Airtable donor domains on Instant and Netim.")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--instant-only", action="store_true", help="Check only Instant Domain Search.")
    modes.add_argument("--netim-only", action="store_true", help="Check only Netim status and price.")
    modes.add_argument(
        "--rdap-need-to-check-netim",
        action="store_true",
        help="Check only RDAP need-to-check donors on Netim and write confirmed statuses.",
    )
    args = parser.parse_args()
    mode = (
        "instant"
        if args.instant_only
        else "netim"
        if args.netim_only
        else "rdap_need_to_check_netim"
        if args.rdap_need_to_check_netim
        else "all"
    )

    load_local_env()
    token = os.environ.get("AIRTABLE_TOKEN", "").strip()
    if not token:
        print("AIRTABLE_TOKEN is missing. Add it to the project .env file.", flush=True)
        return 1

    try:
        table = Api(token).table(SETTINGS["base_id"], SETTINGS["table_id"])
        process_table(table, mode)
    except InstantNoResultsError as error:
        print(f"Instant check failed: {error}", flush=True)
        return 1
    except Exception as error:
        if is_auth_error(error):
            print(
                "Airtable returned 401/403. Replace AIRTABLE_TOKEN in the local .env with a token "
                "that has data.records:read and data.records:write access to this base.",
                flush=True,
            )
            return 1
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())