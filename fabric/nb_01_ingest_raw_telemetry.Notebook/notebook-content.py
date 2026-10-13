# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f2e65aa9-564a-4d6c-81db-13f1103b3395",
# META       "default_lakehouse_name": "lh_pulsestream_bronze",
# META       "default_lakehouse_workspace_id": "f7c6ca72-4252-4487-97b1-4bc58fdab6b4",
# META       "known_lakehouses": [
# META         {
# META           "id": "f2e65aa9-564a-4d6c-81db-13f1103b3395"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

"""
PulseStream - synthetic Alpe d'Huez telemetry generator
All rider and team names are FICTIONAL. The route geometry is parametric and approximate
(derived from publicly available figures); it is not a real GPS track.

Fabric notebook version (nb_01_ingest_raw_telemetry).
Output: Delta tables in lh_pulsestream_bronze
  dim_turn_bronze, dim_team_bronze, dim_rider_bronze, dim_ride_bronze, raw_telemetry_bronze
"""
import numpy as np
import pandas as pd

SEED = 42
rng = np.random.default_rng(SEED)

# ---------------------------------------------------------------- Route parameters
# Sources (alpedhuez.com, bike-oisans.com): start 737 m, bend 21 = 806 m, bend 17 = 965 m,
# bend 12 = 1161 m, bend 1 = 1713 m, finish ~1850 m, distance 13.8 km, average gradient ~8.1%.
# Bend ALTITUDES are calibrated to the published values; bend POSITIONS (km) and the sections
# in between are approximate / fictional. This is not a real GPS track.
LENGTH_KM = 13.8
START_ALT = 737.0
FINISH_ALT = 1850.0
STEP_M = 25             # route resolution (metres)

# (metres from start, altitude): published altitude anchors
ANCHORS = [(0, 737.0), (650, 806.0), (2150, 965.0), (4800, 1161.0), (12000, 1713.0), (13800, 1850.0)]
# Zones with reduced gradient: (start m, end m, gradient multiplier)
FLAT_ZONES = [(2350, 2550, 0.25),   # La Garde (around bend 16, ~200 m easier)
              (8100, 8500, 0.55)]   # Huez village (around bend 7)

# Bend positions (km): bend 21 down to bend 1. Anchor bends: 21, 17, 12, 1
TURN_KM = [0.65, 1.025, 1.4, 1.775, 2.15, 2.5, 3.15, 3.8, 4.3, 4.8, 5.4,
           6.0, 6.6, 7.4, 8.3, 8.92, 9.53, 10.15, 10.77, 11.38, 12.0]
TURN_NOS = list(range(21, 0, -1))  # counting down from 21 to 1


def build_route():
    d = np.arange(0, LENGTH_KM * 1000 + STEP_M, STEP_M)
    alt = np.zeros_like(d)
    for (d0, a0), (d1, a1) in zip(ANCHORS[:-1], ANCHORS[1:]):
        m = (d >= d0) & (d <= d1)
        w = np.ones(m.sum())
        for z0, z1, f in FLAT_ZONES:
            w[(d[m] >= z0) & (d[m] <= z1)] *= f
        w *= 1 + np.convolve(rng.normal(0, 0.07, len(w)), np.ones(7) / 7, mode="same")
        cum = np.r_[0, np.cumsum(w[:-1])]
        alt[m] = a0 + (a1 - a0) * cum / cum[-1]            # each section ends exactly on its anchor
    alt[-1] = FINISH_ALT
    grade = np.gradient(alt, d)
    return pd.DataFrame({"dist_m": d, "alt_m": alt, "grade": grade})


ROUTE = build_route()


def dim_turn():
    rows = []
    for no, km in zip(TURN_NOS, TURN_KM):
        alt = float(np.interp(km * 1000, ROUTE.dist_m, ROUTE.alt_m))
        g = float(np.interp(km * 1000, ROUTE.dist_m, ROUTE.grade))
        rows.append({"turn_id": no, "turn_name": f"Bend {no}", "km_from_start": km,
                     "altitude_m": round(alt, 1), "local_grade_pct": round(g * 100, 2)})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- Fictional teams / riders
TEAMS = ["Aurora Velo", "Nordkamm Racing", "Solaris Cycling", "Pico Rojo", "Bluepeak Pro"]
FIRST = ["Aldric", "Bastien", "Corvin", "Dario", "Elio", "Fenwick", "Gaspar", "Hollis", "Ivo", "Jorik",
         "Kellan", "Lorcan", "Matteo", "Nilo", "Orsin", "Perrin", "Quill", "Rafael", "Soren", "Tobias"]
LAST = ["Vandermark", "Okafor-Lind", "Delacroix", "Ruiz-Hale", "Kowal", "Brandt", "Marchetti",
        "Aalto", "Vasco", "Thorne", "Lemaire", "Sandoval", "Eklund", "Moreau", "Zielinski"]


def make_riders(n=20):
    riders = []
    for i in range(n):
        wkg = float(np.clip(rng.normal(5.3, 0.45), 4.2, 6.3))        # ~40-minute threshold W/kg
        mass = float(np.clip(rng.normal(66, 4), 56, 76))
        riders.append({
            "rider_id": i + 1,
            "rider_name": f"{FIRST[i % len(FIRST)]} {LAST[(i * 7) % len(LAST)]}",
            "team_id": i % len(TEAMS) + 1,
            "body_mass_kg": round(mass, 1),
            "ftp_wkg": round(wkg, 2),
            "max_hr_bpm": int(np.clip(rng.normal(190, 6), 176, 202)),
            "rest_hr_bpm": int(np.clip(rng.normal(46, 4), 38, 58)),
        })
    return pd.DataFrame(riders)


# ---------------------------------------------------------------- Physics
G, RHO, CRR, CDA, BIKE_KG = 9.81, 1.0, 0.004, 0.33, 7.0     # air density at altitude ~1.0


def speed_from_power(p_w, grade, mass_total):
    """Solve P = (m g (sinθ + Crr) + 0.5 ρ CdA v²) v for v (Newton's method)."""
    th = np.arctan(grade)
    a = 0.5 * RHO * CDA
    b = mass_total * G * (np.sin(th) + CRR * np.cos(th))
    v = np.full_like(p_w, 5.0)
    for _ in range(25):
        f = a * v**3 + b * v - p_w
        v = np.clip(v - f / (3 * a * v**2 + b), 0.8, 20)
    return v


def simulate_ride(ride_id, rider, effort):
    """effort: fraction of threshold power (0.85 - 1.02). Returns 1 Hz telemetry."""
    mass = rider.body_mass_kg + BIKE_KG
    base_p = rider.ftp_wkg * rider.body_mass_kg * effort
    d, grade = ROUTE.dist_m.values, ROUTE.grade.values

    t, pos, rows = 0, 0.0, []
    hr = rider.rest_hr_bpm + 40.0
    fatigue = 0.0
    while pos < d[-1]:
        g = float(np.interp(pos, d, grade))
        # fatigue + small power fluctuation + a "sprint" boost over the last 1.5 km
        fatigue += 0.00004
        p = base_p * (1 - fatigue) * (1 + rng.normal(0, 0.03))
        if pos > d[-1] - 1500:
            p *= 1.05
        p = max(p, 120)
        v = float(speed_from_power(np.array([p]), g, mass)[0])
        cad = float(np.clip(rng.normal(78 + 40 * (0.07 - g), 4) , 55, 100))
        # HR: lagged approach to the target for the current power + cardiac drift
        target = rider.rest_hr_bpm + (rider.max_hr_bpm - rider.rest_hr_bpm) * np.clip(p / (rider.ftp_wkg * rider.body_mass_kg) * 0.88, 0.3, 0.97)
        hr += (target - hr) / 45.0 + rng.normal(0, 0.4)
        hr_val = float(np.clip(hr + 0.004 * t / 10, rider.rest_hr_bpm, rider.max_hr_bpm))
        rows.append((ride_id, rider.rider_id, t, pos, float(np.interp(pos, d, ROUTE.alt_m)),
                     g * 100, v * 3.6, p, cad, hr_val))
        pos += v
        t += 1
    cols = ["ride_id", "rider_id", "elapsed_s", "dist_m", "altitude_m", "grade_pct",
            "speed_kmh", "power_w", "cadence_rpm", "heart_rate_bpm"]
    df = pd.DataFrame(rows, columns=cols)
    df["turn_id"] = assign_turn(df.dist_m.values)
    return df


def assign_turn(dist):
    """Tag each sample with the number of the NEXT bend (0 = run-in after the last bend)."""
    km = np.array(TURN_KM) * 1000
    idx = np.searchsorted(km, dist, side="left")
    nos = np.array(TURN_NOS + [0])
    return nos[np.clip(idx, 0, len(nos) - 1)]


def inject_quality_issues(df, rate=0.002):
    """Add DELIBERATELY corrupted rows (flagged with is_injected_issue) to demonstrate the validation rules."""
    df = df.copy()
    df["is_injected_issue"] = False
    n = int(len(df) * rate)
    idx = rng.choice(len(df), n, replace=False)
    third = n // 3
    df.loc[df.index[idx[:third]], "heart_rate_bpm"] = np.nan                 # sensor dropout
    df.loc[df.index[idx[third:2 * third]], "power_w"] = rng.integers(1800, 2500, third)  # impossible wattage
    df.loc[df.index[idx[2 * third:]], "speed_kmh"] = rng.uniform(60, 90, n - 2 * third)  # GPS speed spike
    df.loc[df.index[idx], "is_injected_issue"] = True
    return df


def generate(n_riders=20, rides_per_rider=3):
    turns = dim_turn()
    teams = pd.DataFrame({"team_id": range(1, len(TEAMS) + 1), "team_name": TEAMS})
    riders = make_riders(n_riders)

    rides, tele = [], []
    ride_id = 1
    for r in riders.itertuples():
        for k in range(rides_per_rider):
            effort = float(np.clip(rng.normal(0.95, 0.04), 0.82, 1.03))
            df = simulate_ride(ride_id, r, effort)
            rides.append({"ride_id": ride_id, "rider_id": r.rider_id,
                          "ride_date": (pd.Timestamp("2026-07-01") + pd.Timedelta(days=int(rng.integers(0, 90)))).date(),
                          "effort_ratio": round(effort, 3)})
            tele.append(df)
            ride_id += 1

    telemetry = inject_quality_issues(pd.concat(tele, ignore_index=True))
    telemetry = telemetry.round({"dist_m": 1, "altitude_m": 1, "grade_pct": 2, "speed_kmh": 2,
                                 "power_w": 0, "cadence_rpm": 0, "heart_rate_bpm": 0})
    return {"dim_turn_bronze": turns, "dim_team_bronze": teams, "dim_rider_bronze": riders,
            "dim_ride_bronze": pd.DataFrame(rides), "raw_telemetry_bronze": telemetry}


# ---------------------------------------------------------------- Write to Delta (Fabric)
from pyspark.sql import functions as F

tables = generate()
for name, pdf in tables.items():
    sdf = spark.createDataFrame(pdf)
    sdf = sdf.replace(float("nan"), None)                    # NaN -> real NULL
    sdf = sdf.withColumn("ingested_at", F.current_timestamp())
    sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    print(f"{name}: {sdf.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
