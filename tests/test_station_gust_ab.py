"""Offline tests for station_gust_ab.py (no network, no DB): python -m unittest tests/test_station_gust_ab.py"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import station_gust_ab as ab  # noqa: E402


def row(rule, g, **kw):
    r = {"stid": "XXX", "date": "2026-10-01", "g": g, "rule": rule, "max50": None, "top50": None, "max75": None,
         "top75": None, "n75_100": 0, "max100": None, "wind15": 0, "any50": 0, "meas25": None, "hurricane": None,
         "storm_day": False, "glitch_record": False, "hrrr": None}
    r.update(kw)
    return r


class DecideTests(unittest.TestCase):
    def test_rule_a(self):
        self.assertEqual(ab.decide(row("A", 70, max50=30), True, False, "2025-11-01")[0], "REMOVE")
        self.assertEqual(ab.decide(row("A", 70, max50=45), True, False, "2025-11-01")[0], "KEEP")      # >= 60%
        self.assertEqual(ab.decide(row("A", 70, wind15=1), True, False, "2025-11-01")[0], "KEEP")
        self.assertEqual(ab.decide(row("A", 70, any50=2), True, True, "2025-11-01")[0], "KEEP")         # thunder + report
        self.assertEqual(ab.decide(row("A", 70, max50=45), False, False, "2025-11-01")[0], "REMOVE")   # not in own reports

    def test_rule_a2_obvious_only(self):
        self.assertEqual(ab.decide(row("A2", 70, max50=30), True, False, "2025-11-01")[0], "REMOVE")   # under half
        self.assertEqual(ab.decide(row("A2", 70, max50=38), True, False, "2025-11-01")[0], "KEEP")     # borderline
        self.assertEqual(ab.decide(row("A2", 70), True, False, "2025-11-01")[0], "KEEP")                # nobody reported
        self.assertEqual(ab.decide(row("A2", 70, max50=20), False, False, "2025-11-01")[0], "KEEP")    # not in own reports

    def test_rule_b(self):
        self.assertEqual(ab.decide(row("B", 140), True, False, "2025-11-01")[0], "REMOVE")
        self.assertEqual(ab.decide(row("B", 140, hurricane="IAN"), False, False, "2025-11-01")[0], "KEEP")
        self.assertEqual(ab.decide(row("B", 110, max75=92), True, False, "2025-11-01")[0], "KEEP")
        self.assertEqual(ab.decide(row("B", 110, n75_100=2), True, False, "2025-11-01")[0], "KEEP")
        self.assertEqual(ab.decide(row("B", 110, meas25=90), True, False, "2025-11-01")[0], "KEEP")
        self.assertEqual(ab.decide(row("B", 110), True, False, "2025-11-01")[0], "REMOVE")
        self.assertEqual(ab.decide(row("B", 110), False, False, "2025-11-01")[0], "REMOVE")

    def test_holds(self):
        # no published hurricane tracks + big wind event -> hold
        self.assertEqual(ab.decide(row("B", 107, date="2026-06-17", n75_100=1), False, False, "2025-11-01")[0], "REMOVE")
        r = row("B", 107, n75_100=2, max75=60); r["date"] = "2026-06-17"
        self.assertEqual(ab.decide(r, False, False, "2025-11-01")[0], "HOLD")
        # storm day at a clean station -> hold; at a glitch station -> remove
        self.assertEqual(ab.decide(row("B", 112, storm_day=True), True, False, "2025-11-01")[0], "HOLD")
        self.assertEqual(ab.decide(row("B", 112, storm_day=True, glitch_record=True), True, False, "2025-11-01")[0], "REMOVE")

    def test_hrrr_grid_veto(self):
        self.assertEqual(ab.decide(row("A", 80, max50=30, hrrr=60), True, False, "2025-11-01")[0], "KEEP")
        self.assertEqual(ab.decide(row("A", 80, max50=30, hrrr=46), True, False, "2025-11-01")[0], "REMOVE")  # < 58
        self.assertEqual(ab.decide(row("A", 120, max50=30, hrrr=60), True, False, "2025-11-01")[0], "REMOVE")  # < 60%
        self.assertEqual(ab.decide(row("A", 80, max50=30, hrrr=60), False, False, "2025-11-01")[0], "REMOVE")  # own reports


if __name__ == "__main__":
    unittest.main()
