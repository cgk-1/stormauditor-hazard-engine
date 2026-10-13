"""Offline tests for station_gust_qc.py (no network, no DB): python -m unittest tests/test_station_gust_qc.py"""
import datetime as dt
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import station_gust_qc as q  # noqa: E402

UTC = dt.timezone.utc


class FakeResp:
    def __init__(self, j):
        self._j = j

    def json(self):
        return self._j


class FakeRun:
    def __init__(self, reply, dry_run=False):
        self.reply, self.dry_run, self.secret = reply, dry_run, "s"
        self.meta, self.writes, self.warns, self.notes, self.done = {}, [], [], [], []

    def rpc_read(self, name, payload, timeout=60):
        assert name == "hz_gust_qc_candidates"
        return FakeResp(self.reply(payload["p_d0"], payload["p_d1"]))

    def write(self, key, scope, calls, timeout=120):
        self.writes.append((scope, calls))

    def written(self, key, scope):
        self.done.append(scope)

    def warn(self, key, msg):
        self.warns.append(msg)

    def note(self, key, msg):
        self.notes.append(msg)


def cand(stid, date, g=60.0):
    return {"stid": stid, "date": date, "g": g, "lat": 30.0, "lon": -90.0, "n40": 3, "hrrr": 15, "echo": 0,
            "peak_time_utc": None, "tz": "America/Chicago"}


def obs(t, sk, gust=None, metar="AO2"):
    return {"t": t, "sk": sk, "gust": gust, "pk": None, "pkt": None, "tmpf": 70.0, "dwpf": 60.0, "wx": "",
            "metar": metar}


class RunQcTests(unittest.TestCase):
    def test_window_and_hrrr_wait(self):
        reply = lambda d0, d1: {"hrrr": {"2026-10-10": 2715, "2026-10-11": 2715, "2026-10-12": 0},
                                "rows": [cand("AAA", "2026-10-11"), cand("BBB", "2026-10-12")]}
        run = FakeRun(reply)
        seen = []
        s = q.run_qc(run, [dt.date(2026, 10, 12)], False, classify_fn=lambda r: seen.append(r["stid"]) or ("REAL", {"why": "real:x"}))
        self.assertEqual(s["dates"], ["2026-10-10", "2026-10-11"])
        self.assertEqual(s["waiting_hrrr"], ["2026-10-12"])
        self.assertEqual(seen, ["AAA"])          # a date without HRRR backgrounds is not checked yet
        self.assertEqual(run.writes, [])

    def test_fault_is_quarantined_in_batches(self):
        rows = [cand(f"S{i:02d}", "2026-10-11") for i in range(3)]
        run = FakeRun(lambda d0, d1: {"hrrr": {"2026-10-11": 2715}, "rows": rows})
        q.run_qc(run, [dt.date(2026, 10, 11)], True, classify_fn=lambda r: ("FAULT", {"why": "fault:calm_both", "metar": "X"}))
        self.assertEqual(len(run.writes), 1)
        scope, calls = run.writes[0]
        self.assertEqual(calls[0][0], "hz_gust_qc_quarantine")
        self.assertEqual([r["stid"] for r in calls[0][1]["p_rows"]], ["S00", "S01", "S02"])
        self.assertEqual(set(calls[0][1]["p_rows"][0]), {"stid", "date", "why"})

    def test_too_many_faults_on_one_date_are_held(self):
        rows = [cand(f"S{i:02d}", "2026-10-11") for i in range(q.MAX_FAULTS_PER_DAY + 1)]
        run = FakeRun(lambda d0, d1: {"hrrr": {"2026-10-11": 2715}, "rows": rows})
        s = q.run_qc(run, [dt.date(2026, 10, 11)], True, classify_fn=lambda r: ("FAULT", {"why": "fault:ratio"}))
        self.assertEqual(run.writes, [])
        self.assertEqual(len(s["held"]), q.MAX_FAULTS_PER_DAY + 1)
        self.assertTrue(run.warns)

    def test_fetch_failure_is_skipped(self):
        def boom(r):
            raise q.FetchFailed("IEM down")
        run = FakeRun(lambda d0, d1: {"hrrr": {"2026-10-11": 2715}, "rows": [cand("AAA", "2026-10-11")]})
        s = q.run_qc(run, [dt.date(2026, 10, 11)], True, classify_fn=boom)
        self.assertEqual(s["fetch_failed"], ["AAA 2026-10-11 60.0 mph"])
        self.assertEqual(run.writes, [])

    def test_enabled(self):
        os.environ.pop("GUST_QC", None)
        self.assertTrue(q.enabled(False))
        self.assertFalse(q.enabled(True))
        os.environ["GUST_QC"] = "1"
        self.assertTrue(q.enabled(True))
        os.environ["GUST_QC"] = "0"
        self.assertFalse(q.enabled(False))
        os.environ.pop("GUST_QC")


class ClassifyTests(unittest.TestCase):
    def _run(self, observations, r=None, one=None):
        r = r or cand("DED", "2026-10-12", 81.7)
        return q.classify(r, fetch_metars=lambda *a: observations, fetch_onemin=lambda *a: one)

    def test_calm_both_sides_is_fault(self):
        t = dt.datetime(2026, 10, 12, 16, 10, tzinfo=UTC)
        o = [obs(t - dt.timedelta(minutes=15), 4), obs(t, 5, 71.0, "KDED 121610Z 05005G71KT AO2"),
             obs(t + dt.timedelta(minutes=15), 3)]
        v, f = self._run(o)
        self.assertEqual((v, f["why"]), ("FAULT", "fault:calm_both"))

    def test_storm_signs_win(self):
        t = dt.datetime(2026, 10, 12, 16, 10, tzinfo=UTC)
        o = [obs(t - dt.timedelta(minutes=15), 4), obs(t, 5, 71.0, "KDED 121610Z 05005G71KT +TSRA AO2"),
             obs(t + dt.timedelta(minutes=15), 3)]
        o[1]["wx"] = "+TSRA"
        v, f = self._run(o)
        self.assertEqual(v, "REAL")

    def test_ratio_with_regional_wind_is_unclear(self):
        t = dt.datetime(2026, 10, 12, 16, 10, tzinfo=UTC)
        o = [obs(t - dt.timedelta(minutes=60), 9), obs(t, 8, 52.0, "KXXX 121610Z 27008G52KT AO2"),
             obs(t + dt.timedelta(minutes=60), 9)]
        r = cand("XXX", "2026-10-12", 59.8)
        self.assertEqual(self._run(o, r)[0], "FAULT")
        r["hrrr"] = 30                            # >= 0.4 x 59.8 mph
        self.assertEqual(self._run(o, r), ("UNCLEAR", self._run(o, r)[1]))
        self.assertEqual(self._run(o, r)[1]["why"], "unclear:ratio_regional_wind")

    def test_no_matching_report(self):
        t = dt.datetime(2026, 10, 12, 16, 10, tzinfo=UTC)
        v, f = self._run([obs(t, 10, 30.0)])
        self.assertEqual(v, "UNSUPPORTED")


if __name__ == "__main__":
    unittest.main()
