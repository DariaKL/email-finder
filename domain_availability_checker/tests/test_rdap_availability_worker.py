import unittest
from unittest.mock import Mock, patch

from domain_availability_checker import rdap_availability_worker as worker


class RdapAvailabilityWorkerTests(unittest.TestCase):
    def setUp(self):
        worker.STOP_REQUESTED.clear()

    def test_load_bootstrap_uses_current_iana_response(self):
        response = Mock()
        response.json.return_value = {
            "services": [
                [["com", "net"], ["https://rdap.example/rdap/"]],
                [["invalid"], ["http://insecure.example/rdap/"]],
            ]
        }

        with patch.object(worker.requests, "get", return_value=response) as get:
            services = worker.load_bootstrap()

        get.assert_called_once_with(
            worker.BOOTSTRAP_URL,
            headers={"Accept": "application/json"},
            timeout=(10, 30),
        )
        self.assertEqual(services, {"com": ("https://rdap.example/rdap",), "net": ("https://rdap.example/rdap",)})
        self.assertNotIn("invalid", services)

    def test_endpoint_lookup_uses_longest_suffix_and_all_urls(self):
        services = {
            "com": ("https://registry.example/",),
            "example.com": ("https://specific.example/rdap/", "https://backup.example/rdap/"),
        }

        self.assertEqual(
            worker.endpoints_for("sub.example.com", services),
            ("https://specific.example/rdap/", "https://backup.example/rdap/"),
        )
        self.assertEqual(worker.endpoint_for("sub.example.com", services), "https://specific.example/rdap/")
        self.assertEqual(worker.endpoints_for("sample.it", services), ())

    def test_valid_domain_object_is_busy(self):
        response = Mock(status_code=200)
        response.json.return_value = {"objectClassName": "domain"}
        limiter = Mock()
        limiter.get.return_value = response

        self.assertEqual(worker.classify_domain("registered.com", {"com": ("https://rdap.example",)}, limiter), ("busy", "classified"))

    def test_http_404_is_free_when_optional_json_body_is_absent(self):
        valid_404 = Mock(status_code=404)
        valid_404.json.return_value = {"errorCode": 404}
        limiter = Mock()
        limiter.get.return_value = valid_404

        self.assertEqual(worker.classify_domain("missing.com", {"com": ("https://rdap.example",)}, limiter), ("free", "classified"))

        html_404 = Mock(status_code=404)
        html_404.headers = {}
        html_404.json.side_effect = ValueError("not JSON")
        limiter.get.return_value = html_404
        self.assertEqual(worker.classify_domain("unknown.com", {"com": ("https://rdap.example",)}, limiter), ("free", "classified"))

        contradictory_404 = Mock(status_code=404)
        contradictory_404.headers = {}
        contradictory_404.json.return_value = {"errorCode": 500}
        limiter.get.return_value = contradictory_404
        with patch.object(worker.time, "sleep"):
            status, result = worker.classify_domain("ambiguous.com", {"com": ("https://rdap.example",)}, limiter)
        self.assertEqual(status, "need to check")
        self.assertEqual(result, "ambiguous_404")

    def test_endpoint_failover_uses_backup_url(self):
        response = Mock(status_code=200)
        response.json.return_value = {"objectClassName": "domain"}
        limiter = Mock()
        limiter.get.side_effect = [
            worker.requests.ConnectTimeout("primary timed out"),
            response,
        ]

        result = worker.classify_domain(
            "registered.example",
            {"example": ("https://primary.example/rdap", "https://backup.example/rdap")},
            limiter,
        )

        self.assertEqual(result, ("busy", "classified"))
        self.assertEqual(limiter.get.call_count, 2)

    def test_rate_limiter_is_global_across_hosts(self):
        clock = [10.0]
        starts = []

        def fake_sleep(seconds):
            clock[0] += seconds

        def fake_get(url, **kwargs):
            starts.append(clock[0])
            return Mock()

        limiter = worker.RegistryRateLimiter()
        with (
            patch.object(worker.time, "monotonic", side_effect=lambda: clock[0]),
            patch.object(worker.time, "sleep", side_effect=fake_sleep),
            patch.object(worker.requests, "get", side_effect=fake_get),
        ):
            limiter.get("https://one.example/rdap", "one.com")
            limiter.get("https://two.example/rdap", "two.net")

        self.assertEqual(starts, [10.0, 11.0])


if __name__ == "__main__":
    unittest.main()