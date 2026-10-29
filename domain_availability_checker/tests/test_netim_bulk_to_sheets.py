import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import airtable_donor_status as donor_status
import netim_bulk_to_sheets as netim


class FakeResponse:
    url = "https://www.netim.com/ajax/resultats-recherche.php"

    def __init__(self, status, payload=None):
        self.status = status
        self.payload = payload

    def text(self):
        return json.dumps(self.payload) if self.payload is not None else ""


class FakeLocator:
    def __init__(self, page, selector):
        self.page = page
        self.selector = selector

    def wait_for(self, **_kwargs):
        return None

    def fill(self, _value):
        return None

    def click(self, **_kwargs):
        if self.selector == "#trigger-search-multiple":
            for callback in self.page.callbacks:
                callback(self.page.ajax_response)


class FakePage:
    def __init__(self, ajax_response):
        self.ajax_response = ajax_response
        self.callbacks = []

    def on(self, _event, callback):
        self.callbacks.append(callback)

    def remove_listener(self, _event, callback):
        self.callbacks.remove(callback)

    def goto(self, *_args, **_kwargs):
        return SimpleNamespace(status=200)

    def locator(self, selector):
        return FakeLocator(self, selector)

    def wait_for_timeout(self, _milliseconds):
        return None


class NetimBulkTests(unittest.TestCase):
    def test_bulk_result_payload_maps_confirmed_status(self):
        page = FakePage(
            FakeResponse(
                200,
                {"example.com": '<div class="resultat success">Available</div>'},
            )
        )

        self.assertEqual(netim.scrape_batch_statuses(page, ["example.com"], 1), {"example.com": "available"})

    def test_ajax_429_is_reported_as_rate_limit(self):
        page = FakePage(FakeResponse(429))

        with self.assertRaises(netim.NetimRateLimitError):
            netim.scrape_batch_statuses(page, ["example.com"], 1)

    def test_ajax_403_is_reported_as_access_block(self):
        page = FakePage(FakeResponse(403))

        with self.assertRaises(netim.NetimAccessError):
            netim.scrape_batch_statuses(page, ["example.com"], 1)

    def test_each_retry_uses_and_closes_a_fresh_page(self):
        first_page = Mock()
        second_page = Mock()
        context = Mock()
        context.new_page.side_effect = [first_page, second_page]

        with (
            patch.object(donor_status, "scrape_netim_batch", side_effect=[RuntimeError("stale page"), {"example.com": "available"}]),
            patch.object(donor_status.time, "sleep"),
        ):
            result = donor_status.check_netim_batch(context, ["example.com"])

        self.assertEqual(result, {"example.com": "available"})
        self.assertEqual(context.new_page.call_count, 2)
        first_page.close.assert_called_once_with()
        second_page.close.assert_called_once_with()

    def test_access_block_stops_retries(self):
        page = Mock()
        context = Mock()
        context.new_page.return_value = page

        with (
            patch.object(donor_status, "scrape_netim_batch", side_effect=netim.NetimAccessError("HTTP 403")),
            patch.object(donor_status.time, "sleep"),
        ):
            result = donor_status.check_netim_batch(context, ["example.com"])

        self.assertIsNone(result)
        context.new_page.assert_called_once_with()
        page.close.assert_called_once_with()

    def test_rdap_fallback_skips_price_parsing(self):
        table = Mock()
        table.all.return_value = [
            {"id": "rec1", "fields": {"Domain": "example.com", "RDAP": "need to check"}}
        ]
        context = Mock()
        browser = Mock()
        browser.new_context.return_value = context
        playwright = Mock()
        playwright.chromium.launch.return_value = browser
        playwright_manager = MagicMock()
        playwright_manager.__enter__.return_value = playwright

        with (
            patch.object(donor_status, "sync_playwright", return_value=playwright_manager),
            patch.object(donor_status, "check_netim_batch", return_value={"example.com": "available"}) as check_batch,
            patch.object(donor_status, "save_statuses", return_value=1),
        ):
            donor_status.process_table(table, mode="rdap_need_to_check_netim")

        check_batch.assert_called_once_with(context, ["example.com"], None)


if __name__ == "__main__":
    unittest.main()