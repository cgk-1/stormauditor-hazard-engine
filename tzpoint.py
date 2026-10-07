"""tzpoint.py - offline mirror of the DB function hz_tz_at(lat, lon) for points
(LSR reports, ASOS/AWOS stations, NCEI events). Archive Phase 5 Stage 3, 2026-10-07.

Why a vendored copy (decision, see docs/phase5-stage3-report.md in the site repo):
the feed needs a zone for every LSR row and every station; one DB call per row
would be ~5,000 calls a night and would make dry runs depend on the database.
The tiles in data/tz/tz_poly.json.gz are exactly the rows of hz_tz_poly
(timezone-boundary-builder 2026d, (c) OpenStreetMap contributors, ODbL 1.0;
coverage-simplified 0.001 deg, 1-degree tiles): the loader below recomputes
md5(string_agg(id:zone_id:iana:md5(wkb), ',' order by id)) and refuses to run
unless it equals DB_FINGERPRINT, which is what
  select md5(string_agg(id||':'||zone_id||':'||iana||':'||md5(st_asbinary(geom)),
                        ',' order by id)) from hz_tz_poly
returns on prod (38935e2c..., checked 2026-10-07). The rule is the SQL rule:
  1. the polygon containing the point (lowest zone id on an exact shared border);
  2. else the nearest polygon within 1.0 degree (planar degrees, as ST_DWithin /
     ST_Distance on geometry(4326)); ties -> lowest zone id;
  3. else None (not a US location). Longitudes may be -180..180 or 0..360.
Agreement with the DB function is re-checked by the Stage 3 parity test on every
LSR row and station of the test days (SELECT-only).

STATE_TZ_DB mirrors hz_state_tz(state) (the v3 LSR zone, also the v4 fallback
for points with no US zone, e.g. Guam/American Samoa/far-offshore reports).
"""
import gzip
import hashlib
import json
import os

import shapely
from shapely import STRtree

import feedguard as fg

HERE = os.path.dirname(os.path.abspath(__file__))
POLY_FILE = os.path.join(HERE, "data", "tz", "tz_poly.json.gz")
POLY_JSON_MD5 = "7bb098d708b11b100f218cd000548818"
DB_FINGERPRINT = "38935e2cacc6eee57dff9ba5dd7aeb3d"
FALLBACK_DEG = 1.0

STATE_TZ_DB = {
    "AL": "America/Chicago", "AZ": "America/Phoenix", "AR": "America/Chicago", "CA": "America/Los_Angeles",
    "CO": "America/Denver", "CT": "America/New_York", "DE": "America/New_York", "FL": "America/New_York",
    "GA": "America/New_York", "ID": "America/Boise", "IL": "America/Chicago",
    "IN": "America/Indiana/Indianapolis", "IA": "America/Chicago", "KS": "America/Chicago",
    "KY": "America/New_York", "LA": "America/Chicago", "ME": "America/New_York", "MD": "America/New_York",
    "MA": "America/New_York", "MI": "America/Detroit", "MN": "America/Chicago", "MS": "America/Chicago",
    "MO": "America/Chicago", "MT": "America/Denver", "NE": "America/Chicago", "NV": "America/Los_Angeles",
    "NH": "America/New_York", "NJ": "America/New_York", "NM": "America/Denver", "NY": "America/New_York",
    "NC": "America/New_York", "ND": "America/Chicago", "OH": "America/New_York", "OK": "America/Chicago",
    "OR": "America/Los_Angeles", "PA": "America/New_York", "RI": "America/New_York",
    "SC": "America/New_York", "SD": "America/Chicago", "TN": "America/Chicago", "TX": "America/Chicago",
    "UT": "America/Denver", "VT": "America/New_York", "VA": "America/New_York",
    "WA": "America/Los_Angeles", "WV": "America/New_York", "WI": "America/Chicago", "WY": "America/Denver",
    "DC": "America/New_York", "AK": "America/Anchorage", "HI": "Pacific/Honolulu",
    "PR": "America/Puerto_Rico", "VI": "America/Puerto_Rico",
}


def state_tz_db(state):
    """hz_state_tz(state): upper-cased lookup, 'UTC' for anything else."""
    return STATE_TZ_DB.get((state or "").upper(), "UTC")


class Lookup:
    def __init__(self, path=POLY_FILE):
        with open(path, "rb") as fh:
            raw = gzip.decompress(fh.read())
        got = hashlib.md5(raw).hexdigest()
        if got != POLY_JSON_MD5:
            raise fg.ValidationError(f"tz polygon file md5 {got} != pinned {POLY_JSON_MD5}")
        doc = json.loads(raw)
        rows = doc["rows"]
        fp = hashlib.md5(",".join(f"{i}:{z}:{n}:{hashlib.md5(bytes.fromhex(h)).hexdigest()}"
                                  for i, z, n, h in rows).encode()).hexdigest()
        if fp != DB_FINGERPRINT or doc.get("db_fingerprint") != DB_FINGERPRINT:
            raise fg.ValidationError(f"tz polygon fingerprint {fp} != hz_tz_poly {DB_FINGERPRINT}")
        self.zone = [int(z) for _, z, _, _ in rows]
        self.iana = [n for _, _, n, _ in rows]
        self.geom = [shapely.from_wkb(bytes.fromhex(h)) for _, _, _, h in rows]
        self.tree = STRtree(self.geom)
        self.fingerprint = fp

    def at(self, lat, lon):
        """IANA zone of the point, or None (same rule as hz_tz_at)."""
        lat, lon = float(lat), float(lon)
        if not (-90 <= lat <= 90 and -180 <= lon <= 360):
            return None
        if lon > 180:
            lon -= 360
        p = shapely.Point(lon, lat)
        hit = self.tree.query(p, predicate="intersects")
        if len(hit):
            return self.iana[min(hit.tolist(), key=lambda i: (self.zone[i], i))]
        near = self.tree.query(p, predicate="dwithin", distance=FALLBACK_DEG)
        if len(near):
            return self.iana[min(near.tolist(), key=lambda i: (shapely.distance(self.geom[i], p), self.zone[i], i))]
        return None


_LOOKUP = None


def lookup():
    global _LOOKUP
    if _LOOKUP is None:
        _LOOKUP = Lookup()
    return _LOOKUP
