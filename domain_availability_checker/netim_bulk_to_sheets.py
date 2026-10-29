#!/usr/bin/env python3
"""Check blank Netim Status cells in batches and write results to Google Sheets."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path

import gspread
from playwright.sync_api import sync_playwright


SETTINGS = {
    "sheet_id": "1Srjm1VTXKoE4lAlxZvwtGTchIKOowM5u-p5Z4nvwyW0",
    "worksheet_name": "Netim",
    "credentials_json": "service_account.json",
    "batch_size": 40,
    "pause_between_batches_sec": 120,
    "first_data_row": 8892,
    "headless": True,
    "result_timeout_sec": 45,
    "max_batch_attempts": 3,
    "rate_limit_pause_sec": 900,
}


@dataclass(slots=True)
class RowDomain:
    row: int
    domain: str


class NetimRateLimitError(RuntimeError):
    pass


class NetimAccessError(RuntimeError):
    pass


def normalize_domain(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"^https?://", "", text)
    return text.split("/")[0].strip(".")


def get_status_layout(rows: list[list[str]]) -> tuple[int, int, int]:
    for row_index, row in enumerate(rows):
        if len(row) > 1 and row[0].strip().lower() == "domain" and row[1].strip().lower() == "status":
            return row_index, 0, 1

    for row_index, row in enumerate(rows):
        if not row or row[0].strip().lower() != "domain":
            continue
        for column_index, value in enumerate(row):
            if value.strip().lower() == "status":
                return row_index, 0, column_index
    raise ValueError("Could not find the Domain/Status header row in the Netim worksheet")


def load_pending(
    rows: list[list[str]],
    header_row: int,
    domain_col: int,
    status_col: int,
    first_data_row: int,
) -> list[RowDomain]:
    pending: list[RowDomain] = []
    for row_index, row in enumerate(rows[header_row + 1 :], start=header_row + 2):
        domain = normalize_domain(row[domain_col] if len(row) > domain_col else "")
        status = row[status_col].strip() if len(row) > status_col else ""
        if row_index >= first_data_row and domain and status == "":
            pending.append(RowDomain(row=row_index, domain=domain))
    return pending


def status_from_fragment(fragment: str) -> str | None:
    match = re.search(r"class=['\"]([^'\"]*\bresultat\b[^'\"]*)['\"]", fragment, re.IGNORECASE)
    if not match:
        return None
    classes = match.group(1).lower().split()
    if "success" in classes:
        return "available"
    if "danger" in classes:
        return "taken"
    return "unknown"


def price_from_fragment(fragment: str) -> float | None:
    match = re.search(r"\bdata-currentprice\s*=\s*['\"]([^'\"]+)['\"]", fragment, re.IGNORECASE)
    if not match:
        return None
    raw_price = match.group(1).strip()
    if "," in raw_price and "." not in raw_price:
        raw_price = raw_price.replace(",", ".")
    else:
        raw_price = raw_price.replace(",", "")
    try:
        return float(raw_price)
    except ValueError:
        return None


def scrape_batch_statuses(
    page,
    domains: list[str],
    timeout_sec: int,
    price_results: dict[str, float] | None = None,
) -> dict[str, str]:
    expected = {normalize_domain(domain) for domain in domains}
    results: dict[str, str] = {}
    received_payload = False
    ajax_statuses: list[int] = []
    rate_limited = False
    access_block_status: int | None = None

    def on_response(response) -> None:
        nonlocal received_payload, rate_limited, access_block_status
        if "/ajax/resultats-recherche.php" not in response.url:
            return
        ajax_statuses.append(response.status)
        if response.status == 429:
            rate_limited = True
            return
        if response.status in {403, 503}:
            access_block_status = response.status
            return
        if response.status != 200:
            return
        try:
            payload = json.loads(response.text())
        except Exception:
            return
        if not isinstance(payload, dict) or not payload:
            return
        received_payload = True
        for raw_domain, fragment in payload.items():
            domain = normalize_domain(raw_domain)
            if domain not in expected or not isinstance(fragment, str):
                continue
            status = status_from_fragment(fragment)
            if status:
                results[domain] = status
            if price_results is not None:
                price = price_from_fragment(fragment)
                if price is not None:
                    price_results[domain] = price

    page.on("response", on_response)
    try:
        response = page.goto("https://www.netim.com/en/domain-name/search", wait_until="domcontentloaded")
        if response and response.status == 429:
            raise NetimRateLimitError("Netim returned HTTP 429 Too Many Requests")
        if response and response.status in {403, 503}:
            raise NetimAccessError(f"Netim search page returned HTTP {response.status}")
        page.locator("#recherche-multiple-trigger").wait_for(state="visible", timeout=10000)
        page.locator("#recherche-multiple-trigger").click(timeout=10000)
        textarea = page.locator("#recherche-domaine-input_multiple")
        textarea.wait_for(state="visible", timeout=10000)
        textarea.fill("\n".join(domains))
        page.locator("#trigger-search-multiple").click(timeout=10000)

        deadline = time.time() + timeout_sec
        while (
            time.time() < deadline
            and len(results) < len(expected)
            and not rate_limited
            and access_block_status is None
        ):
            page.wait_for_timeout(500)
    finally:
        page.remove_listener("response", on_response)

    if access_block_status is not None:
        raise NetimAccessError(f"Netim AJAX endpoint returned HTTP {access_block_status}")
    if rate_limited:
        raise NetimRateLimitError("Netim AJAX endpoint returned HTTP 429 Too Many Requests")
    if not received_payload:
        raise RuntimeError(f"Netim returned no bulk result payload (AJAX HTTP statuses: {ajax_statuses})")
    return {domain: results.get(domain, "unknown") for domain in expected}


def write_statuses(ws: gspread.Worksheet, results: list[tuple[int, str]], status_col: int) -> None:
    if not results:
        return
    col = ""
    n = status_col
    while n:
        n, remainder = divmod(n - 1, 26)
        col = chr(65 + remainder) + col

    sorted_results = sorted(results)
    ranges = []
    run: list[tuple[int, str]] = []
    for item in sorted_results:
        if run and item[0] != run[-1][0] + 1:
            ranges.append(run)
            run = []
        run.append(item)
    if run:
        ranges.append(run)

    updates = []
    for current_run in ranges:
        first, last = current_run[0][0], current_run[-1][0]
        updates.append({
            "range": f"{col}{first}:{col}{last}",
            "values": [[status] for _, status in current_run],
        })
    ws.batch_update(updates, value_input_option="RAW")


def main() -> None:
    credentials = Path(__file__).resolve().parent / SETTINGS["credentials_json"]
    if not credentials.exists():
        raise FileNotFoundError(f"Credentials file not found: {credentials}")

    client = gspread.service_account(filename=str(credentials))
    worksheet = client.open_by_key(SETTINGS["sheet_id"]).worksheet(SETTINGS["worksheet_name"])
    rows = worksheet.get_all_values()
    header_row, domain_col, status_col = get_status_layout(rows)
    pending = load_pending(
        rows,
        header_row,
        domain_col,
        status_col,
        int(SETTINGS["first_data_row"]),
    )
    if not pending:
        print("No blank Status cells to check.")
        return

    batches = [pending[index : index + SETTINGS["batch_size"]] for index in range(0, len(pending), SETTINGS["batch_size"])]
    print(f"Queued {len(pending)} domains in {len(batches)} batches of up to {SETTINGS['batch_size']}.", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=SETTINGS["headless"])
        page = browser.new_page()
        page.set_default_navigation_timeout(60000)
        try:
            for batch_index, batch in enumerate(batches, start=1):
                print(f"Batch {batch_index}/{len(batches)}: {len(batch)} domains", flush=True)
                statuses = None
                attempt = 1
                while attempt <= SETTINGS["max_batch_attempts"]:
                    try:
                        statuses = scrape_batch_statuses(
                            page,
                            [item.domain for item in batch],
                            int(SETTINGS["result_timeout_sec"]),
                        )
                        break
                    except NetimAccessError as error:
                        print(f"  {error}; stopping Netim to avoid unreliable checks.", flush=True)
                        return
                    except NetimRateLimitError as error:
                        print(
                            f"  {error}; waiting {SETTINGS['rate_limit_pause_sec']} seconds before retrying this batch.",
                            flush=True,
                        )
                        time.sleep(SETTINGS["rate_limit_pause_sec"])
                    except Exception as error:
                        print(
                            f"  Attempt {attempt}/{SETTINGS['max_batch_attempts']} failed: {error}",
                            flush=True,
                        )
                        if attempt < SETTINGS["max_batch_attempts"]:
                            print(f"  Retrying after {SETTINGS['pause_between_batches_sec']} seconds.", flush=True)
                            time.sleep(SETTINGS["pause_between_batches_sec"])
                        attempt += 1

                if statuses is None:
                    print("  Leaving this batch blank for a later restart.", flush=True)
                else:
                    values = [(item.row, statuses[item.domain]) for item in batch]
                    write_statuses(worksheet, values, status_col + 1)
                    counts = {status: sum(value == status for _, value in values) for status in ("available", "taken", "unknown")}
                    print(
                        f"  Wrote {len(values)} statuses: {counts['available']} available, "
                        f"{counts['taken']} taken, {counts['unknown']} unknown.",
                        flush=True,
                    )

                if batch_index < len(batches):
                    print(f"  Waiting {SETTINGS['pause_between_batches_sec']} seconds.", flush=True)
                    time.sleep(SETTINGS["pause_between_batches_sec"])
        finally:
            browser.close()


if __name__ == "__main__":
    main()