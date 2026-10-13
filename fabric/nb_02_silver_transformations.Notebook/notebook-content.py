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
PulseStream - nb_02_silver_transformations
Bronze -> Silver: validation rules, quarantine and a data-quality summary.
Default lakehouse: lh_pulsestream_bronze

Input : raw_telemetry_bronze, dim_ride_bronze, dim_rider_bronze
Output: fct_telemetry_silver        (rows that pass every rule)
        quarantine_telemetry_silver (rejected rows + the rules they failed)
        silver_dq_summary           (data-quality summary per rule)
"""
from pyspark.sql import functions as F

raw = spark.table("raw_telemetry_bronze")
rides = spark.table("dim_ride_bronze").select("ride_id", "ride_date")
riders = spark.table("dim_rider_bronze").select("rider_id", "body_mass_kg")

# ---------------------------------------------------------------- Validation rules
# Each rule defines its "violation condition". NULL values count as violations too.
c = F.col
RULES = {
    "HR_MISSING":            c("heart_rate_bpm").isNull(),
    "HR_OUT_OF_RANGE":       c("heart_rate_bpm").isNotNull() & ~c("heart_rate_bpm").between(30, 215),
    "POWER_OUT_OF_RANGE":    c("power_w").isNull() | ~c("power_w").between(0, 1500),
    "SPEED_OUT_OF_RANGE":    c("speed_kmh").isNull() | ~c("speed_kmh").between(0, 45),
    "CADENCE_OUT_OF_RANGE":  c("cadence_rpm").isNull() | ~c("cadence_rpm").between(0, 150),
    "GRADE_OUT_OF_RANGE":    c("grade_pct").isNull() | ~c("grade_pct").between(-5, 20),
    "ALTITUDE_OUT_OF_RANGE": c("altitude_m").isNull() | ~c("altitude_m").between(700, 1900),
}

failed = F.filter(
    F.array(*[F.when(cond, F.lit(name)) for name, cond in RULES.items()]),
    lambda x: x.isNotNull(),
)
checked = (
    raw.withColumn("failed_rules", failed)
       .withColumn("is_valid", F.size("failed_rules") == 0)
       .cache()
)

# ---------------------------------------------------------------- Quarantine and clean data
quarantine = (
    checked.filter(~c("is_valid"))
           .withColumn("failed_rules", F.concat_ws(",", "failed_rules"))
           .drop("is_valid")
)

silver = (
    checked.filter(c("is_valid"))
           .drop("failed_rules", "is_valid", "is_injected_issue", "ingested_at")
           .join(rides, "ride_id", "left")
           .join(riders, "rider_id", "left")
           .withColumn("event_ts", (c("ride_date").cast("timestamp").cast("long") + c("elapsed_s")).cast("timestamp"))
           .withColumn("watts_per_kg", F.round(c("power_w") / c("body_mass_kg"), 2))
           .drop("body_mass_kg")
)

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fct_telemetry_silver")
quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("quarantine_telemetry_silver")

# ---------------------------------------------------------------- Data-quality summary
total = checked.count()
rows = []
for name, cond in RULES.items():
    rows.append((name, checked.filter(cond).count()))
rows.append(("DUPLICATE_RIDE_SECOND", total - checked.select("ride_id", "elapsed_s").distinct().count()))
rows.append(("TOTAL_REJECTED_ROWS", quarantine.count()))

dq = (
    spark.createDataFrame(rows, ["rule", "failed_rows"])
         .withColumn("total_rows", F.lit(total))
         .withColumn("failed_pct", F.round(c("failed_rows") / c("total_rows") * 100, 3))
         .withColumn("checked_at", F.current_timestamp())
)
dq.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_dq_summary")

# ---------------------------------------------------------------- Sanity output
injected = raw.filter(c("is_injected_issue")).count()
caught = checked.filter(c("is_injected_issue") & ~c("is_valid")).count()
print(f"Bronze rows        : {total:,}")
print(f"Silver (clean)     : {silver.count():,}")
print(f"Quarantined        : {quarantine.count():,}")
print(f"Injected bad rows  : {injected} | caught by the rules: {caught}")
display(spark.table("silver_dq_summary"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
