import os
os.environ["PYSPARK_PYTHON"] = "./env/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "./env/Scripts/python.exe"

import pyspark
from delta import *
from pyspark.sql.functions import lit
import json

# Check for new files in the data directory
DATA_DIR = "data"
TRACKER_FILE = "file_tracker_bronze.json"

# Load previously seen files (if any)
if os.path.exists(TRACKER_FILE):
    with open(TRACKER_FILE, "r") as f:
        seen_files = set(json.load(f))
else:
    seen_files = set()

# Scan current files
current_files = set(
    os.path.join(DATA_DIR, file)
    for file in os.listdir(DATA_DIR)
    if file.endswith(".csv") and os.path.isfile(os.path.join(DATA_DIR, file))
)

# Identify new files
new_files = current_files - seen_files

# Store relative paths in `path`
path = sorted(new_files)

# Save current state for next run
with open(TRACKER_FILE, "w") as f:
    json.dump(sorted(current_files), f)


# One-time creation of base directories
os.makedirs("data_lake/bronze/products", exist_ok=True)

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

from pyspark.sql.functions import col, to_timestamp, to_date

# Process new files
if len(path) == 0:
    print("No new files to process.")
else:
    for file in path:
        # Read the CSV file
        df = spark.read.option("header", "true").csv(file, inferSchema=False)

        # Extract date column from timestamp
        df = df.withColumn(
        "timestamp_parsed",
        to_timestamp(col("TIMESTAMP"), "yyyy-MM-dd HH:mm:ss")
        )


        df = df.withColumn(
        "scrape_date",
        to_date(col("timestamp_parsed"))
        )

        df = df.drop("timestamp_parsed")
        
        df = df.dropna(subset=["TIMESTAMP", "scrape_date"])

        # Write to Delta Lake & partition by date
        df.write.format("delta").mode("append").partitionBy("scrape_date").save("data_lake/bronze/products/")
        print(f"Processed and saved {file} to Delta Lake..")

spark.stop()  # Stop the Spark session when done