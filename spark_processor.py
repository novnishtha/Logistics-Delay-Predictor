import os
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import joblib
import pandas as pd
import psycopg2

# 1. Spark Session

spark = SparkSession.builder \
    .appName("LogisticsDelayPredictor") \
    .config("spark.sql.session.timeZone", "Asia/Kolkata") \
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata") \
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Kolkata") \
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,org.postgresql:postgresql:42.7.3")\
    .config("spark.sql.shuffle.partitions", "10")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Load ML Model
model = joblib.load("rf_model.pkl")
model_columns = joblib.load("model_columns.pkl")

# 3. Schema
schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("distance_km", FloatType()),
    StructField("speed_kmh", DoubleType()),
    StructField("weather", StringType()),
    StructField("traffic", StringType()),
    StructField("delay_minutes", DoubleType()),
    StructField("timestamp", DoubleType())
])

# 4. Read + Parse + Event Time
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logistics_events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withWatermark("event_time", "10 minutes")

# 5. Prediction UDF
@pandas_udf("double")
def predict_udf(distance_km, speed_kmh, weather, traffic):
    pdf = pd.DataFrame({
        "distance_km": distance_km,
        "speed_kmh": speed_kmh,
        "weather": weather,
        "traffic": traffic
    })

    pdf = pd.get_dummies(pdf)

    for c in model_columns:
        if c not in pdf:
            pdf[c] = 0

    pdf = pdf[model_columns]

    return pd.Series(model.predict(pdf))

# 6. Prediction + Status
processed_df = stream_df \
    .withColumn("predicted_delay", predict_udf(col("distance_km"), col("speed_kmh"), col("weather"), col("traffic"))) \
    .withColumn("status", when(col("distance_km") <= 2, "COMPLETED").otherwise("IN_PROGRESS"))

# create status_df for truck_status table
status_df = processed_df.groupBy("vehicle_id").agg(
    max("event_time").alias("last_update"),
    last("status", ignorenulls=True).alias("status")
)

# 7. Aggregation
state_df = processed_df.groupBy("vehicle_id").agg(
    max("event_time").alias("last_update"),
    last("speed_kmh", ignorenulls=True).alias("current_speed"),
    last("distance_km", ignorenulls=True).alias("remaining_distance_km"),
    last("predicted_delay", ignorenulls=True).alias("predicted_delay")
)

# 8. ETA + Formatting
state_df = state_df.select(
    col("vehicle_id"),
    col("last_update"),
    round(col("current_speed"), 2).alias("current_speed"),
    round(col("remaining_distance_km"), 2).alias("remaining_distance_km"),
    round(col("predicted_delay"), 2).alias("predicted_delay"),
    round((col("remaining_distance_km") / col("current_speed")) * 60 + col("predicted_delay"),2).alias("eta_minutes")
)

# 9. Write to PostgreSQL (staging -> final tables with no duplicates)

def upsert_to_postgres():

    conn = psycopg2.connect(
        host="localhost",
        database="logistics_db",
        user="admin",
        password="admin"
    )

    cur = conn.cursor()

    # UPSERT truck_eta

    cur.execute("""
        INSERT INTO truck_eta AS t (
            vehicle_id,
            last_update,
            current_speed,
            remaining_distance_km,
            predicted_delay,
            eta_minutes
        )

        SELECT DISTINCT ON (vehicle_id)
            vehicle_id,
            last_update,
            current_speed,
            remaining_distance_km,
            predicted_delay,
            eta_minutes

        FROM truck_eta_staging

        ORDER BY vehicle_id, last_update DESC

        ON CONFLICT (vehicle_id)

        DO UPDATE SET
            last_update = EXCLUDED.last_update,
            current_speed = EXCLUDED.current_speed,
            remaining_distance_km = EXCLUDED.remaining_distance_km,
            predicted_delay = EXCLUDED.predicted_delay,
            eta_minutes = EXCLUDED.eta_minutes;
    """)

    # clear ETA staging table
    cur.execute("TRUNCATE truck_eta_staging;")

    # upsert truck_status
    cur.execute("""
        INSERT INTO truck_status AS s (
            vehicle_id,
            last_update,
            status
        )

        SELECT DISTINCT ON (vehicle_id)
            vehicle_id,
            last_update,
            status

        FROM truck_status_staging

        ORDER BY vehicle_id, last_update DESC

        ON CONFLICT (vehicle_id)

        DO UPDATE SET
            last_update = EXCLUDED.last_update,
            status = EXCLUDED.status;
    """)

    # clear status staging table
    cur.execute("TRUNCATE truck_status_staging;")

    conn.commit()
    cur.close()
    conn.close()

def write_to_postgres(batch_df, batch_id):

    try:

        if batch_df.count() == 0:
            return

        print(f"Writing batch {batch_id}...")

        # SPARK-SIDE DEDUPLICATION
        batch_df = batch_df.groupBy("vehicle_id").agg(
            max("last_update").alias("last_update"),
            first("current_speed").alias("current_speed"),
            first("remaining_distance_km").alias("remaining_distance_km"),
            first("predicted_delay").alias("predicted_delay"),
            first("eta_minutes").alias("eta_minutes")
        )

        # WRITE ETA STAGING
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/logistics_db") \
            .option("dbtable", "truck_eta_staging") \
            .option("user", "admin") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()


        # CREATE STATUS BATCH
        status_batch = batch_df.select(
            col("vehicle_id"),
            col("last_update"),

            when(col("remaining_distance_km") <= 2,
                 "COMPLETED")
            .otherwise("IN_PROGRESS")
            .alias("status")
        )

        # STATUS DEDUPLICATION

        status_batch = status_batch.groupBy("vehicle_id").agg(
            max("last_update").alias("last_update"),
            first("status").alias("status")
        )

        # WRITE STATUS STAGING

        status_batch.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/logistics_db") \
            .option("dbtable", "truck_status_staging") \
            .option("user", "admin") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        # UPSERT INTO FINAL TABLES
        upsert_to_postgres()

        print(f"Batch {batch_id} upserted successfully")


    except Exception as e:

        print("ERROR:", e)
        raise e

# query = state_df.writeStream \
#     .outputMode("update") \
#     .foreachBatch(write_to_postgres) \
#     .option("checkpointLocation", "./checkpoints/spark_checkpoint") \
#     .start()

postgres_query = state_df.writeStream\
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "./checkpoints/postgres_checkpoint") \
    .start()

console_query = state_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .option("checkpointLocation", "./checkpoints/console_checkpoint") \
    .start()


postgres_query.awaitTermination(160)
console_query.awaitTermination(160)

