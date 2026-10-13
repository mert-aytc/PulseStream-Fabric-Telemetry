# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "73747392-99a3-47fa-8d40-aac7e1374d1f",
# META       "default_lakehouse_name": "lh_pulsestream_gold",
# META       "default_lakehouse_workspace_id": "f7c6ca72-4252-4487-97b1-4bc58fdab6b4",
# META       "known_lakehouses": [
# META         {
# META           "id": "73747392-99a3-47fa-8d40-aac7e1374d1f"
# META         },
# META         {
# META           "id": "f2e65aa9-564a-4d6c-81db-13f1103b3395"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

"""
PulseStream - nb_03_gold_aggregations  (star schema)
Default lakehouse: lh_pulsestream_gold   (lh_pulsestream_bronze must also be attached in the Explorer)

Input  (bronze lakehouse): fct_telemetry_silver, dim_*_bronze, silver_dq_summary
Output (gold lakehouse)  :
  dim_rider (team name denormalised), dim_turn, dim_date, dim_route_profile
  fact_telemetry  (1 row = 1 ride x 1 second)
  fact_turn_split (1 row = 1 ride x 1 bend section)
  fact_ride       (1 row = 1 ride)
  dq_summary
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SRC = "lh_pulsestream_bronze"          # source lakehouse (read only)
c = F.col


def src(name):
    return spark.table(f"{SRC}.{name}")


def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    print(f"{name}: {spark.table(name).count():,} rows")


silver = src("fct_telemetry_silver")
rides = src("dim_ride_bronze")
riders = src("dim_rider_bronze")
teams = src("dim_team_bronze")
turns = src("dim_turn_bronze")

# ---------------------------------------------------------------- Dimensions
dim_rider = (
    riders.join(teams, "team_id", "left")
          .select("rider_id", "rider_name", "team_id", "team_name",
                  "body_mass_kg", "ftp_wkg", "max_hr_bpm", "rest_hr_bpm")
)

FINISH_KM = 13.8   # route length in km
# turn_id = 0: the run-in to the finish after the last bend (telemetry has 'no next bend' there)
finish_row = spark.createDataFrame(
    [(0, "Finish run-in", FINISH_KM, 1850.0, None)],
    "turn_id int, turn_name string, km_from_start double, altitude_m double, local_grade_pct double",
)
dim_turn = (
    turns.select("turn_id", "turn_name", "km_from_start", "altitude_m", "local_grade_pct")
         .unionByName(finish_row)
         .withColumn("turn_order", F.row_number().over(Window.orderBy("km_from_start")))
         # length of the section that ENDS at this bend (telemetry's turn_id = the next bend)
         .withColumn("section_km", F.round(F.col("km_from_start") - F.coalesce(F.lag("km_from_start").over(Window.orderBy("km_from_start")), F.lit(0.0)), 3))
         # stylised serpentine layout used by the route graphic (0-100 canvas)
         .withColumn("x_pos", F.when(F.col("turn_order") % 2 == 1, F.lit(100.0)).otherwise(F.lit(0.0)))
         .withColumn("y_pos", F.round(F.col("turn_order") / F.max("turn_order").over(Window.partitionBy()) * 100, 2))
)

bounds = rides.agg(F.min("ride_date").alias("d0"), F.max("ride_date").alias("d1")).first()
dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{bounds.d0}'), to_date('{bounds.d1}'), interval 1 day)) AS date")
         .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
         .withColumn("year", F.year("date"))
         .withColumn("month", F.month("date"))
         .withColumn("month_name", F.date_format("date", "MMMM"))
         .withColumn("day", F.dayofmonth("date"))
         .withColumn("weekday", F.date_format("date", "EEEE"))
         .select("date_key", "date", "year", "month", "month_name", "day", "weekday")
)

# ---------------------------------------------------------------- Facts
fact_telemetry = (
    silver.withColumn("date_key", F.date_format("ride_date", "yyyyMMdd").cast("int"))   # ride_date silver'da zaten var
          .select("ride_id", "rider_id", "date_key", "turn_id", "elapsed_s", "event_ts",
                  "dist_m", "altitude_m", "grade_pct", "speed_kmh", "power_w",
                  "cadence_rpm", "heart_rate_bpm", "watts_per_kg")
)

fact_turn_split = (
    silver.groupBy("ride_id", "rider_id", "turn_id")
          .agg(F.count("*").alias("segment_seconds"),
               F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
               F.round(F.avg("power_w"), 1).alias("avg_power_w"),
               F.round(F.avg("heart_rate_bpm"), 1).alias("avg_heart_rate_bpm"),
               F.round(F.avg("grade_pct"), 2).alias("avg_grade_pct"),
               F.round(F.max("altitude_m") - F.min("altitude_m"), 1).alias("elevation_gain_m"))
)

fact_ride = (
    silver.groupBy("ride_id", "rider_id")
          .agg(F.max("elapsed_s").alias("total_time_s"),
               F.round(F.max("dist_m") / 1000, 2).alias("distance_km"),
               F.round(F.max("altitude_m") - F.min("altitude_m"), 1).alias("elevation_gain_m"),
               F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
               F.round(F.avg("power_w"), 1).alias("avg_power_w"),
               F.round(F.max("power_w"), 1).alias("max_power_w"),
               F.round(F.avg("watts_per_kg"), 2).alias("avg_watts_per_kg"),
               F.round(F.avg("heart_rate_bpm"), 1).alias("avg_heart_rate_bpm"),
               F.max("heart_rate_bpm").alias("max_heart_rate_bpm"))
          .join(rides.select("ride_id", "ride_date", "effort_ratio"), "ride_id", "left")
          .withColumn("date_key", F.date_format("ride_date", "yyyyMMdd").cast("int"))
          # VAM: vertical ascent rate (metres per hour)
          .withColumn("vam_m_per_h", F.round(c("elevation_gain_m") / (c("total_time_s") / 3600), 0))
          .drop("ride_date")
)

# ---------------------------------------------------------------- Write
for df, name in [(dim_rider, "dim_rider"), (dim_turn, "dim_turn"), (dim_date, "dim_date"),
                 (fact_telemetry, "fact_telemetry"), (fact_turn_split, "fact_turn_split"),
                 (fact_ride, "fact_ride"), (src("silver_dq_summary"), "dq_summary")]:
    save(df, name)

# ---------------------------------------------------------------- Checks (star schema integrity)
def orphans(fact, key, dim, dkey):
    keys = spark.table(dim).select(F.col(dkey).alias("_dim_key"))
    return spark.table(fact).join(keys, F.col(key) == F.col("_dim_key"), "left_anti").count()

print("--- Orphan key check (all should be 0) ---")
print("fact_telemetry.rider_id  :", orphans("fact_telemetry", "rider_id", "dim_rider", "rider_id"))
print("fact_telemetry.turn_id   :", orphans("fact_telemetry", "turn_id", "dim_turn", "turn_id"))
print("fact_telemetry.date_key  :", orphans("fact_telemetry", "date_key", "dim_date", "date_key"))
print("fact_ride.rider_id       :", orphans("fact_ride", "rider_id", "dim_rider", "rider_id"))
print("fact_ride.date_key       :", orphans("fact_ride", "date_key", "dim_date", "date_key"))
print("fact_turn_split.turn_id  :", orphans("fact_turn_split", "turn_id", "dim_turn", "turn_id"))
print("fact_telemetry.ride_id   :", orphans("fact_telemetry", "ride_id", "fact_ride", "ride_id"))
print("fact_turn_split.ride_id  :", orphans("fact_turn_split", "ride_id", "fact_ride", "ride_id"))
display(spark.table("fact_ride").orderBy("total_time_s").limit(10))

# ---------------------------------------------------------------- Route profile (100 m resolution)
# Built in the lakehouse because Direct Lake semantic models do not support calculated tables.
route = (spark.table("fact_telemetry")
         .withColumn("km", F.round(F.col("dist_m") / 100) / 10)       # nearest 100 m, so the profile spans 0 - 13.8 km
         .groupBy("km")
         .agg(F.round(F.avg("altitude_m"), 1).alias("altitude_m"),
              F.round(F.avg("grade_pct"), 2).alias("grade_pct"))
         .orderBy("km"))
save(route, "dim_route_profile")

# ---------------------------------------------------------------- Route trail (stylised, 25 m resolution)
# A schematic "switchback" layout for the route graphic: the path zig-zags between x = 0 and x = 100,
# one straight section per bend, every section has the same height. It is NOT a GPS track.
import numpy as np

section_bounds = np.r_[0.0, spark.table("dim_turn").toPandas().sort_values("km_from_start")["km_from_start"].to_numpy()]
trail = (spark.table("fact_telemetry")
         .withColumn("km", F.round(F.col("dist_m") / 25) * 25 / 1000)
         .groupBy("km")
         .agg(F.round(F.avg("altitude_m"), 1).alias("altitude_m"),
              F.round(F.avg("grade_pct"), 2).alias("grade_pct"))
         .orderBy("km")
         .toPandas())

sec = np.clip(np.searchsorted(section_bounds, trail["km"].to_numpy(), side="right") - 1, 0, len(section_bounds) - 2)
frac = (trail["km"].to_numpy() - section_bounds[sec]) / (section_bounds[sec + 1] - section_bounds[sec])
ease = (1 - np.cos(np.pi * frac)) / 2                      # rounded hairpins
trail["x_pos"] = np.round(np.where(sec % 2 == 0, ease, 1 - ease) * 100, 2)
trail["y_pos"] = np.round((sec + frac) / (len(section_bounds) - 1) * 100, 2)   # every section gets the same height
save(spark.createDataFrame(trail), "dim_route_trail")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
