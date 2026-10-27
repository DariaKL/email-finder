#!/usr/bin/env python3
"""Read domains from Google Sheets, check statuses via Instant Domain Search Bulk,
and write status/details back to Google Sheets.

Status mapping from Instant Bulk results:
- Continue -> available
- Lookup -> taken
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
import tempfile

import gspread
from playwright._impl._errors import TargetClosedError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


SETTINGS = {
    "sheet_id": "1Srjm1VTXKoE4lAlxZvwtGTchIKOowM5u-p5Z4nvwyW0",
    "worksheet_name": "",  # empty -> first sheet
    "credentials_json": "service_account.json",
    "domain_column": "",  # empty -> auto detect
    "status_column": "status",
    "details_column": "details",
    # Instant Bulk Search accepts up to 5000 domains per run.
    "batch_size": 500,
    # If true, skip rows where status is already set (resume mode).
    "only_check_missing_status": True,
    # If true, rows with unknown status are retried on next runs.
    "recheck_unknown_status": False,
    "pause_between_batches_sec": 120,
    "headless": True,
    "slow_mo_ms": 0,
    "batch_wait_timeout_sec": 60,
}


@dataclass(slots=True)
class RowDomain:
    row: int
    domain: str


@dataclass(slots=True)
class StatusResult:
    row: int
    domain: str
    status: str
    details: str


def normalize_domain(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"^https?://", "", text)
    text = text.split("/")[0]
    return text.strip(".")


def column_letter(index_1_based: int) -> str:
    result = ""
    n = index_1_based
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def get_or_create_column(ws: gspread.Worksheet, column_name: str) -> int:
    header = ws.row_values(1)
    if column_name in header:
        return header.index(column_name) + 1

    idx = len(header) + 1
    ws.update(f"{column_letter(idx)}1", [[column_name]], value_input_option="RAW")
    return idx


def normalize_header_cell(value: str) -> str:
    text = (value or "").strip()
    low = text.lower()
    if low.startswith("domain") and "@" in low:
        return "Domain"
    return text


def repair_header_if_needed(ws: gspread.Worksheet) -> None:
    header = ws.row_values(1)
    if not header:
        return
    normalized = [normalize_header_cell(x) for x in header]
    if normalized != header:
        ws.update(f"A1:{column_letter(len(normalized))}1", [normalized], value_input_option="RAW")


def get_column_index_if_exists(ws: gspread.Worksheet, column_name: str) -> int | None:
    header = ws.row_values(1)
    if column_name in header:
        return header.index(column_name) + 1
    return None


def detect_domain_column(header: list[str], configured_name: str) -> str:
    if configured_name and configured_name in header:
        return configured_name

    preferred = ["domain", "domains", "domen", "url", "urls", "site", "sites", "домен", "домени"]
    lower_map = {h.strip().lower(): h for h in header}
    for key in preferred:
        if key in lower_map:
            return lower_map[key]

    # Fallback: tolerate noisy headers like "Domainsheets-bot@..."
    for raw in header:
        value = raw.strip().lower()
        if any(token in value for token in ["domain", "domen", "домен", "site", "url"]):
            return raw

    raise ValueError(
        "Cannot auto-detect domain column. Set SETTINGS['domain_column'] manually. "
        f"Header columns: {', '.join(header)}"
    )


def load_domains(ws: gspread.Worksheet, domain_col_name: str) -> list[RowDomain]:
    header = ws.row_values(1)
    if not header:
        raise ValueError("Header row is empty")

    selected_name = detect_domain_column(header, domain_col_name)
    col_idx = header.index(selected_name) + 1
    values = ws.col_values(col_idx)

    rows: list[RowDomain] = []
    for row_i, value in enumerate(values[1:], start=2):
        domain = normalize_domain(value)
        if not domain:
            continue
        rows.append(RowDomain(row=row_i, domain=domain))

    return rows


def write_back(ws: gspread.Worksheet, results: list[StatusResult], status_col_name: str, details_col_name: str) -> None:
    if not results:
        return

    status_col = get_or_create_column(ws, status_col_name)
    details_col = get_or_create_column(ws, details_col_name)

    results = sorted(results, key=lambda x: x.row)

    # Write contiguous row runs only, so untouched rows in between are never blanked.
    runs: list[list[StatusResult]] = []
    for item in results:
        if runs and item.row == runs[-1][-1].row + 1:
            runs[-1].append(item)
        else:
            runs.append([item])

    data = []
    for run in runs:
        first, last = run[0].row, run[-1].row
        data.append({
            "range": f"{column_letter(status_col)}{first}:{column_letter(status_col)}{last}",
            "values": [[x.status] for x in run],
        })
        data.append({
            "range": f"{column_letter(details_col)}{first}:{column_letter(details_col)}{last}",
            "values": [[x.details] for x in run],
        })
    ws.batch_update(data, value_input_option="RAW")


def chunked[T](items: list[T], size: int) -> list[list[T]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def resolve_status(domain: str, results: dict[str, tuple[str, str]]) -> tuple[str, str] | None:
    if domain in results:
        return results[domain]
    # Instant collapses subdomains (loja.x.com.br -> x.com.br); a live subdomain implies a taken parent.
    parts = domain.split(".")
    for i in range(1, len(parts) - 1):
        parent = ".".join(parts[i:])
        if parent in results:
            return ("taken", f"subdomain of {parent}")
    return None


def scrape_batch_statuses(
    domains: list[str],
    headless: bool,
    slow_mo_ms: int,
    wait_timeout_sec: float,
) -> dict[str, tuple[str, str]]:
    joined = "\n".join(domains)
    out: dict[str, tuple[str, str]] = {}
    expected = {normalize_domain(d) for d in domains}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slow_mo_ms)
        page = browser.new_page()
        page.goto("https://instantdomainsearch.com/bulk-domain-search", wait_until="domcontentloaded")

        api_out: dict[str, tuple[str, str]] = {}

        def on_response(response) -> None:
            if "/services/bulk-check" not in response.url:
                return
            try:
                payload = response.json() or {}
            except Exception:
                return
            for item in payload.get("results", []):
                label = (item.get("label") or "").strip().lower()
                tld = (item.get("tld") or "").strip().lower()
                if not label or not tld:
                    continue
                full = normalize_domain(f"{label}.{tld}")

                is_registered = bool(item.get("isRegistered"))
                if is_registered:
                    etld_count = item.get("etldCount")
                    details = f"{etld_count} taken" if isinstance(etld_count, int) else "Lookup"
                    api_out[full] = ("taken", details)
                else:
                    api_out[full] = ("available", "Continue")

        page.on("response", on_response)

        # Main input can be a textarea first, then token input in some UI states.
        page.wait_for_timeout(1500)
        clear_button = page.locator('button:has-text("Clear")')
        if clear_button.count() > 0:
            clear_button.first.click()

        # Primary path: import batch via hidden file input (most reliable with current UI).
        imported = False
        temp_csv_path = None
        file_input = page.locator('input[type="file"]')
        if file_input.count() > 0:
            fd, temp_csv_path = tempfile.mkstemp(prefix="instant_batch_", suffix=".csv")
            with open(fd, "w", encoding="utf-8", closefd=True) as f:
                f.write("domain\n")
                for d in domains:
                    f.write(f"{normalize_domain(d)}\n")
            file_input.first.set_input_files(temp_csv_path)
            imported = True

        # Fallback path: type/paste domains into visible input.
        if not imported:
            textarea = page.locator('textarea[placeholder*="pavilion.com"]')
            token_input = page.locator(
                'input[placeholder*="Type some domains"], input[placeholder*="Type or paste more domains"]'
            )

            if textarea.count() > 0:
                textarea.first.click()
                textarea.first.fill(joined)
            elif token_input.count() > 0:
                token_input.first.click()
                token_input.first.press("Control+a")
                token_input.first.type(joined)
                token_input.first.press("Enter")
            else:
                browser.close()
                return {normalize_domain(d): ("unknown", "bulk input not found") for d in domains}

        # CSV import opens an "Import Preview" dialog that needs its Search button clicked.
        preview_search = page.locator('div[role="dialog"] button:has-text("Search")')
        try:
            preview_search.first.wait_for(state="visible", timeout=30000)
            preview_search.first.click()
        except PlaywrightTimeoutError:
            page.keyboard.press("Control+Enter")
            search_button = page.locator('button:has-text("Search all domains")')
            if search_button.count() > 0:
                search_button.first.click()

        deadline = time.time() + wait_timeout_sec
        last_count = -1
        last_change = time.time()
        last_report = 0.0
        while time.time() < deadline:
            resolved = sum(1 for d in expected if resolve_status(d, api_out) is not None)
            if resolved >= len(expected):
                break
            if resolved != last_count:
                last_count, last_change = resolved, time.time()
            elif resolved > 0 and time.time() - last_change > 25:
                break
            if time.time() - last_report >= 10:
                print(f"  Instant results received: {resolved}/{len(expected)}", flush=True)
                last_report = time.time()

            # Fallback: parse visible table rows if API responses were not captured.
            rows = page.evaluate(
                """
                () => {
                  const trs = Array.from(document.querySelectorAll('table tbody tr'));
                  const result = [];
                  for (const tr of trs) {
                    const tds = tr.querySelectorAll('td');
                                        if (tds.length < 2) continue;
                                        const domainText = (tds[0].innerText || '').split('\\n')[0].trim().toLowerCase();
                    if (!domainText || !domainText.includes('.')) continue;
                                        const extText = tds.length >= 3 ? (tds[2].innerText || '').trim() : '';
                                        const actionText = (tds[tds.length - 1].innerText || '').trim();
                    result.push({ domain: domainText, extText, actionText });
                  }
                  return result;
                }
                """
            )

            out.clear()
            for row in rows:
                domain = normalize_domain(row.get("domain", ""))
                action_text = (row.get("actionText") or "").lower()
                ext_text = row.get("extText") or ""

                if "continue" in action_text:
                    out[domain] = ("available", ext_text or "Continue")
                elif "lookup" in action_text:
                    out[domain] = ("taken", ext_text or "Lookup")
                else:
                    out[domain] = ("unknown", f"action: {row.get('actionText', '')}")

            time.sleep(1.0)

        # API results take precedence over table-scraped ones.
        merged = {k: v for k, v in out.items() if v[0] != "unknown"}
        merged.update(api_out)
        out = merged

        browser.close()
        if temp_csv_path:
            try:
                Path(temp_csv_path).unlink(missing_ok=True)
            except Exception:
                pass

    final: dict[str, tuple[str, str]] = {}
    for d in domains:
        key = normalize_domain(d)
        final[key] = resolve_status(key, out) or ("unknown", "not found in bulk results")

    return final


def main() -> None:
    cfg = SETTINGS

    credentials_json = Path(str(cfg["credentials_json"]))
    if not credentials_json.exists():
        raise FileNotFoundError(
            f"Credentials file not found: {credentials_json}. "
            "Place service_account.json in this folder."
        )

    gc = gspread.service_account(filename=str(credentials_json))
    sh = gc.open_by_key(str(cfg["sheet_id"]))
    worksheet_name = str(cfg["worksheet_name"]).strip()
    ws = sh.worksheet(worksheet_name) if worksheet_name else sh.get_worksheet(0)
    if ws is None:
        raise ValueError("Worksheet not found")

    repair_header_if_needed(ws)

    rows = load_domains(ws, str(cfg["domain_column"]).strip())
    if not rows:
        raise ValueError("No domains found")

    batch_size = max(1, min(int(cfg["batch_size"]), 5000))

    if bool(cfg["only_check_missing_status"]):
        status_col_idx = get_column_index_if_exists(ws, str(cfg["status_column"]))
        if status_col_idx is not None:
            status_values = ws.col_values(status_col_idx)
            filtered: list[RowDomain] = []
            for row_item in rows:
                existing = ""
                if row_item.row - 1 < len(status_values):
                    existing = (status_values[row_item.row - 1] or "").strip()
                if not existing:
                    filtered.append(row_item)
                    continue
                if bool(cfg["recheck_unknown_status"]) and existing.lower() in {"unknown", "timeout"}:
                    filtered.append(row_item)
            rows = filtered

    if not rows:
        print("Nothing to check: all rows already have status")
        return

    all_results: list[StatusResult] = []
    batches = chunked(rows, batch_size)

    for i, batch in enumerate(batches, start=1):
        domains = [x.domain for x in batch]
        print(f"Batch {i}/{len(batches)}: {len(domains)} domains")

        try:
            status_map = scrape_batch_statuses(
                domains=domains,
                headless=bool(cfg["headless"]),
                slow_mo_ms=int(cfg["slow_mo_ms"]),
                wait_timeout_sec=float(cfg["batch_wait_timeout_sec"]),
            )
        except (PlaywrightTimeoutError, TargetClosedError):
            # Retry once for transient browser crashes.
            try:
                status_map = scrape_batch_statuses(
                    domains=domains,
                    headless=bool(cfg["headless"]),
                    slow_mo_ms=int(cfg["slow_mo_ms"]),
                    wait_timeout_sec=float(cfg["batch_wait_timeout_sec"]),
                )
            except (PlaywrightTimeoutError, TargetClosedError):
                status_map = {normalize_domain(d): ("unknown", "timeout") for d in domains}
        except Exception:
            status_map = {normalize_domain(d): ("unknown", "timeout") for d in domains}

        for item in batch:
            status, details = status_map.get(normalize_domain(item.domain), ("unknown", "missing"))
            all_results.append(StatusResult(row=item.row, domain=item.domain, status=status, details=details))

        batch_results = all_results[-len(batch):]
        print(
            "  Batch results: "
            f"{sum(r.status == 'available' for r in batch_results)} available, "
            f"{sum(r.status == 'taken' for r in batch_results)} taken, "
            f"{sum(r.status == 'unknown' for r in batch_results)} unknown; writing to sheet",
            flush=True,
        )

        # Save progress after each batch in case of interruption.
        write_back(
            ws=ws,
            results=batch_results,
            status_col_name=str(cfg["status_column"]),
            details_col_name=str(cfg["details_column"]),
        )

        if i < len(batches):
            time.sleep(float(cfg["pause_between_batches_sec"]))

    available = sum(1 for r in all_results if r.status == "available")
    taken = sum(1 for r in all_results if r.status == "taken")
    unknown = sum(1 for r in all_results if r.status == "unknown")

    print("Done")
    print(f"Checked: {len(all_results)}")
    print(f"Available: {available}")
    print(f"Taken: {taken}")
    print(f"Unknown: {unknown}")


if __name__ == "__main__":
    main()
