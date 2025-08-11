import os
import platform
if platform.system() == "Windows":
    os.environ["PYSPARK_PYTHON"] = "./env/Scripts/python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "./env/Scripts/python.exe"

# Sets up a PySpark session with Delta Lake support
import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "/opt/spark/work-dir/spark-warehouse") \
    .master("local[*]") \
    .enableHiveSupport() \

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create the database if it does not exist

spark.sql("CREATE DATABASE IF NOT EXISTS Ecom_Products_Data_Pipeline")

spark.sql("USE Ecom_Products_Data_Pipeline")

# Define the path for the silver table

silver_path = os.path.abspath("data_lake/silver/products").replace("\\", "/")

print(f"Silver path: {silver_path}")
print("Files in silver path:", os.listdir(silver_path))

# Create the silver table if it does not exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS Ecom_Products_Data_Pipeline.products_silver
    USING DELTA
    LOCATION '{silver_path}'
""")

# Load the new unprocessed data from silver layer

last_processed_timestamp_file = "last_processed_timestamp_gold.txt"
try:
    with open(last_processed_timestamp_file, 'r') as f:
        last_processed_timestamp = f.read().strip()
    print(f"Previous high-watermark loaded: {last_processed_timestamp}")
except FileNotFoundError:
    # Set a very old timestamp for the first run
    last_processed_timestamp = "1970-01-01"
    print(f"No previous high-watermark found. Initializing to: {last_processed_timestamp}")

# Reads the silver data from the Delta Lake
silver_df = spark.sql(f"""
                      SELECT 
                      Product_Name, MRP, Price_After_Discount AS Current_Price, (MRP - Current_Price) AS Discount_Amount, (Discount_Amount / MRP) AS Discount_Percentage,
                      Bought_Last_Month AS Last_Month_Sales, Number_of_Reviews AS Total_Reviews, Rating AS Avg_Rating,
                      5_Star_Percentage AS Five_Star_Percentage, 4_Star_Percentage AS Four_Star_Percentage,
                      3_Star_Percentage AS Three_Star_Percentage, 2_Star_Percentage AS Two_Star_Percentage,
                      1_Star_Percentage AS One_Star_Percentage, category AS Product_Category, Image_URL AS Image,
                      scrape_date AS Scrape_Date
                      FROM Ecom_Products_Data_Pipeline.products_silver AS products_silver
                      WHERE scrape_date > DATE('{last_processed_timestamp}')
                      """)

if silver_df.rdd.isEmpty():
    print("No new data")
    spark.stop()
    exit(0)
    
else:
    # One-time creation of base directories
    os.makedirs("data_lake/gold/dashboard_mart", exist_ok=True)

    from pyspark.sql.functions import col, lit, max as spark_max
    import datetime # For default high-watermark and example update

    # Write the cleaned DataFrame to Delta Lake in the silver layer
    silver_df.write.format("delta").mode("append").partitionBy("scrape_date").save("data_lake/gold/dashboard_mart/")

    new_max_date_obj = silver_df.agg(spark_max("scrape_date")).collect()[0][0] # This is a datetime.date object or None

    new_max_date_str = None # Initialize a string variable

    if new_max_date_obj is None:
        # If no data was processed, use today's date as a string
        new_max_date_str = datetime.date.today().strftime('%Y-%m-%d')
    else:
        # If data was processed, convert the datetime.date object to a string
        new_max_date_str = new_max_date_obj.strftime('%Y-%m-%d')

    # Persist the new high-watermark string
    with open(last_processed_timestamp_file, 'w') as f:
        f.write(new_max_date_str) # <--- Always a string now
    print(f"High-watermark updated to: {new_max_date_str}")

    # Path to gold mart
    gold_path = os.path.abspath("data_lake/gold/dashboard_mart").replace("\\", "/")

    # Create the gold table if it does not exist

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS Ecom_Products_Data_Pipeline.products_gold
        USING DELTA
        LOCATION '{gold_path}'
    """)

    spark.sql("""
        CREATE OR REPLACE VIEW Ecom_Products_Data_Pipeline.products_gold_latest AS
        SELECT *
        FROM Ecom_Products_Data_Pipeline.products_gold
        WHERE scrape_date = (
            SELECT MAX(scrape_date)
            FROM Ecom_Products_Data_Pipeline.products_gold
        )
    """)

    spark.stop()