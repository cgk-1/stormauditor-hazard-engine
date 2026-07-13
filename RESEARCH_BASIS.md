# SAWE-2 / SAHE-2 — Published Research Basis

Every design decision in the v2 estimators traces to a published method,
government guideline, or peer-reviewed finding. This document is the map.

## Wind — SAWE-2

| Design element | Published basis |
|---|---|
| Estimating a point value from irregular observations via a background field plus distance-weighted corrections | **Cressman (1959)**, successive correction method, *Mon. Wea. Rev.*; **Barnes (1964, 1973)** objective analysis with Gaussian weights; **Koch, desJardins & Kocin (1983)**, *J. Climate Appl. Meteor.* — the standard interactive Barnes scheme. This is the same family of methods NOAA's own RTMA/URMA is built on (De Pondeca et al. 2011, *Wea. Forecasting*). |
| Background = NWS 2.5-km analysis of record (URMA), RTMA where URMA unavailable | NWS designates URMA the analysis of record; De Pondeca et al. and subsequent NOAA quality assessments document its characteristics, including low bias in stronger winds — the justification for a one-sided (upward-only) correction. |
| Innovations computed as observation minus background AT the observation location (never copying the raw observation to the property) | Core of all successive-correction / optimal-interpolation practice (Cressman 1959; Daley, *Atmospheric Data Analysis*, 1991). The engine stores the background at every station daily (`hz_station_bg`) so innovations are exact. |
| Background weight w0 = 1 in the denominator (correction = Σw·I / (1+Σw)) | Equivalent to an observation-to-background error-variance ratio in optimal interpolation (Daley 1991); prevents a single distant observation from transferring its full innovation. |
| Only MEASURED observations carry magnitude; estimated reports are event-detection only | **Edwards, Allen & Carbin (2018)**, *J. Appl. Meteor. Climatol.* 57:1825 — EG:MG ratio ≈ 9:1 nationally; estimated gusts documented biased high and often assigned remotely/arbitrarily. Peer-reviewed radar-verification studies exclude EGs outright for quality (NOAA repository, radar outflow comparison studies). |
| Event-type-dependent length scale: 10 km convective, 50 km tropical/synoptic | Convective outflows (downbursts/microbursts) are compact (Fujita's microburst scale < 4 km); tropical/synoptic wind fields are broad and well-correlated — **WMO TD-No. 1555 (Harper, Kepert & Ginger 2010)** provides the turbulence/gust framework distinguishing regimes; Vickery & Skerlj (2005), *J. Struct. Eng.*, hurricane gust factors. |
| Multi-source design (analysis + model + observations with bias correction) | Mirrors the publicly described architecture of commercial wind verification: radar + observations with observation bias-correction and ground-reach screening (CoreLogic Wind Verification, 2015–2017 public descriptions). |
| Correction cap at the largest observed innovation | Conservatism principle: the analysis error claimed at the property cannot exceed the largest error any instrument actually measured that day. |

## Hail — SAHE-2

| Design element | Published basis |
|---|---|
| MESH as the primary size field | **Witt et al. (1998)**, *Wea. Forecasting* — the MESH algorithm; operational in MRMS (Smith et al. 2016). |
| Sizes reported WITH a category, not as a precise point value | MESH "can only be used to group storms into general categories" (NOAA METplus verification guidance); skill greatest above ~19 mm with overprediction of large values (**Wilson et al. 2009**); good severe/sub-severe discrimination, weaker for significant hail (**Ortega 2018**, *EJSSM*). |
| MESH preferred to reports for detection; reports reconcile | MESH swaths more skillful than LSRs at observing hail objects (**Melick et al. 2014**); report databases have documented population, size-rounding, and location biases (Schaefer et al. 2004; Cintineo et al. 2012; **Murillo & Homeyer 2019**, *JAMC*). |
| Report size floors capped at the 3-mile MESH max | Reports confirm occurrence; transferring a distant report's size beyond radar support at that range would reintroduce the report biases documented above. |
| Regional caution (Southeast US) | Murillo & Homeyer (2019); Murillo et al. (2021) — reduced MESH hail-occurrence skill in the Southeast; disclosed in limitations. |

## What changed from v1 and why

- SAWE-1's ad-hoc "+50% of HRRR upward disagreement" and 1-mile bound are
  replaced by the citable OA formulation; HRRR now serves detection/day-typing
  and the range upper bound (its published strength: resolving convective
  structure), not an uncited blend coefficient.
- SAWE-1's residual rule approximated the background near stations from
  stored ≥40 mph cells; v2 stores the exact background at every station daily,
  making innovations textbook-correct (a nearest-cell fallback covers history
  ingested before the change).
- SAHE's 0.85 adjacent-cell coefficient (uncited) is removed; a 1-mile MESH
  value is now reported as what it is — the 1-mile maximum — with the verdict
  and category carrying the uncertainty, per the categorical-skill literature.

## Verified behavior (blind trace, 8241 Marbella View Ct, Orlando, 2025-08-05)

Live inputs: URMA at property 34.3 mph; HRRR 20.8; measured ORL gust 58 mph
at 5.2 mi; measured 59 mph LSR at 4.8 mi. SAWE-2 output: background 34,
OA correction +13.2 (2 measured observations, convective L = 10 km),
**estimate 47 mph, range 21–58, grade A** — versus 34 mph from the raw
analysis alone. The estimate moves toward the measured evidence exactly as
far as the published weighting allows, and no further.
