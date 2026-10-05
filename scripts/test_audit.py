import unittest
from datetime import date
from audit import findings

class AuditPolicy(unittest.TestCase):
    def report(self, kind="unmaintained", name="paste", version="1.0.15", advisory="RUSTSEC-2024-0436"):
        return {"settings": {"ignore": []}, "vulnerabilities": {"found": False, "count": 0},
                "warnings": {kind: [{"package": {"name": name, "version": version},
                                     "advisory": {"id": advisory, "informational": kind}}]}}
    def test_exact_exception_and_expiry(self):
        self.assertEqual(findings(self.report(), date(2026, 12, 31)), [])
        self.assertTrue(findings(self.report(), date(2027, 1, 1)))
    def test_other_versions_and_warnings_fail(self):
        for changes in [{"version": "1.0.16"}, {"name": "other"}, {"kind": "yanked"}, {"advisory": "RUSTSEC-2099-0001"}]:
            self.assertTrue(findings(self.report(**changes), date(2026, 10, 5)))
    def test_vulnerabilities_always_fail(self):
        report = self.report()
        report["vulnerabilities"] = {"found": True, "count": 1}
        self.assertTrue(findings(report, date(2026, 10, 5)))
    def test_hidden_ignores_fail(self):
        report = self.report()
        report["settings"]["ignore"] = ["RUSTSEC-2000-0000"]
        self.assertTrue(findings(report, date(2026, 10, 5)))
    def test_missing_report_fields_fail_closed(self):
        with self.assertRaises(KeyError):
            findings({}, date(2026, 10, 5))

if __name__ == "__main__":
    unittest.main()
