import os
os.environ["PYSPARK_PYTHON"] = "./env/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "./env/Scripts/python.exe"

# Sets up a PySpark session with Delta Lake support
import pyspark
from delta import *
from pyspark.sql.functions import col, trim, when, sum as _sum

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load the new unprocessed data in bronze layer

last_processed_timestamp_file = "last_processed_timestamp.txt"
try:
    with open(last_processed_timestamp_file, 'r') as f:
        last_processed_timestamp = f.read().strip()
    print(f"Previous high-watermark loaded: {last_processed_timestamp}")
except FileNotFoundError:
    # Set a very old timestamp for the first run
    last_processed_timestamp = "1970-01-01"
    print(f"No previous high-watermark found. Initializing to: {last_processed_timestamp}")

# Reads the bronze data from the Delta Lake
bronze_df = spark.read.format("delta").load("data_lake/bronze/products")\
    .filter(f"scrape_date > '{last_processed_timestamp}'")

# Check if there is any new data to process
if bronze_df.rdd.isEmpty():
    print("No new data")
    spark.stop()
    exit(0)
    
else:
        
    # Script to parse the "Star_Rating_Percentage" column and extract star ratings
    # This assumes the column is a string representation of a dictionary, e.g., "{'5': '20%', '4': '30%', '3': '25%', '2': '15%', '1': '10%'}"

    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import MapType, StringType
    import ast

    # Step 1: Define a UDF to parse the stringified dictionary
    def parse_dict(s):
        try:
            if s is not None:
                d = ast.literal_eval(s)
                return {str(k): v for k, v in d.items()}  # Ensure keys are strings
            else:
                return {}
        except:
            return {}

    # Step 2: Register the UDF
    parse_dict_udf = udf(parse_dict, MapType(StringType(), StringType()))

    # Step 3: Apply the UDF to transform the column into a MapType column
    bronze_df = bronze_df.withColumn("parsed_rating", parse_dict_udf(col("Star_Rating_Percentage")))

    # Step 4: Extract each key into a separate column
    bronze_df = bronze_df.withColumn("5_Star_Percentage", col("parsed_rating")["5"]) \
                        .withColumn("4_Star_Percentage", col("parsed_rating")["4"]) \
                        .withColumn("3_Star_Percentage", col("parsed_rating")["3"]) \
                        .withColumn("2_Star_Percentage", col("parsed_rating")["2"]) \
                        .withColumn("1_Star_Percentage", col("parsed_rating")["1"])

    # Step 5: Drop the old columns (optional)
    bronze_df = bronze_df.drop("Star_Rating_Percentage", "parsed_rating")

    # Function to standardize nulls in a DataFrame
    def standardize_nulls(df):
        return df.select([
            when(col(c) == "null", None).otherwise(col(c)).alias(c)
            for c in df.columns
        ])

    # Standardize nulls in the DataFrame
    bronze_df = standardize_nulls(bronze_df)

    # Drop rows where "Title" is null or empty
    bronze_df = bronze_df.dropna(subset=["Title"])

    # Clean the "Number_of_Reviews" column

    from pyspark.sql.functions import regexp_replace
    # Step 1: Remove commas
    bronze_df = bronze_df.withColumn(
        "Number_of_Reviews",
        regexp_replace("Number_of_Reviews", ",", "")
    )

    # Step 2: Convert to float (optional) then fill nulls with 0
    bronze_df = bronze_df.withColumn(
        "Number_of_Reviews",
        when(col("Number_of_Reviews").isNull(), 0)
        .otherwise(col("Number_of_Reviews").cast("float"))
    )

    # Step 3: Convert to int
    bronze_df = bronze_df.withColumn(
        "Number_of_Reviews",
        col("Number_of_Reviews").cast("int")
    )

    # Clean the "Bought_Last_Month" column to handle "K" notation and convert to numeric

    from pyspark.sql.functions import regexp_extract

    # Extract number part and handle "K" notation
    bronze_df = bronze_df.withColumn(
        "Bought_Last_Month_Cleaned",
        when(col("Bought_Last_Month").isNull(), 0)
        .otherwise(
            when(regexp_extract(col("Bought_Last_Month"), r"(\d+\.?\d*)K", 1) != "",  # Handles "1K", "2.5K", etc.
                regexp_extract(col("Bought_Last_Month"), r"(\d+\.?\d*)K", 1).cast("int") * 1000)
            .otherwise(
                regexp_extract(col("Bought_Last_Month"), r"(\d+)", 1).cast("int")  # Handles plain numbers like "400"
            )
        )
    )

    bronze_df = bronze_df.drop("Bought_Last_Month").withColumnRenamed("Bought_Last_Month_Cleaned", "Bought_Last_Month")

    # Clean the "Rating" column to extract numeric values

    from pyspark.sql.functions import col, when, regexp_extract, round as spark_round

    bronze_df = bronze_df.withColumn(
        "Rating_Cleaned",
        when(
            col("Rating").isNull() & (col("Number_of_Reviews") == 0), 0.0  # Set to 0 if no reviews and null rating
        ).otherwise(
            spark_round(
                regexp_extract(col("Rating"), r"(\d+(\.\d+)?)", 1).cast("double"), 1  # Extract and round to 1 decimal
            )
        )
    )

    bronze_df = bronze_df.drop("Rating").withColumnRenamed("Rating_Cleaned", "Rating")

    # Transform Price and MRP columns

    # Step 1: Clean both columns (remove currency symbols and commas, cast to float)
    bronze_df = bronze_df.withColumn(
        "Price_After_Discount_Cleaned",
        regexp_replace(col("Price_After_Discount"), r"[^\d.]", "").cast("float")
    ).withColumn(
        "MRP_Cleaned",
        regexp_replace(col("MRP"), r"[^\d.]", "").cast("float")
    )

    # Step 2: Impute missing values
    bronze_df = bronze_df.withColumn(
        "Price_Final",
        when(col("Price_After_Discount_Cleaned").isNotNull(), col("Price_After_Discount_Cleaned"))
        .otherwise(col("MRP_Cleaned"))
    ).withColumn(
        "MRP_Final",
        when(col("MRP_Cleaned").isNotNull(), col("MRP_Cleaned"))
        .otherwise(col("Price_After_Discount_Cleaned"))
    )

    # Step 3: Drop rows where both were originally null
    bronze_df = bronze_df.filter(
        col("Price_Final").isNotNull() & col("MRP_Final").isNotNull()
    )

    # Step 4: Drop original columns if not needed
    bronze_df = bronze_df.drop("Price_After_Discount", "MRP", "Price_After_Discount_Cleaned", "MRP_Cleaned")

    # Step 5: Rename cleaned columns to original names if desired
    bronze_df = bronze_df.withColumnRenamed("Price_Final", "Price_After_Discount") \
                        .withColumnRenamed("MRP_Final", "MRP")

    # Handling Star Rating Percentages 

    # List of star rating percentage columns
    star_cols = [
        "5_Star_Percentage",
        "4_Star_Percentage",
        "3_Star_Percentage",
        "2_Star_Percentage",
        "1_Star_Percentage"
    ]

    # Apply conditional logic to each star column
    for star_col in star_cols:
        bronze_df = bronze_df.withColumn(
            star_col,
            when(
                (col("Number_of_Reviews") == 0) & col(star_col).isNull(), 0
            ).otherwise(regexp_replace(col(star_col), "%", "").cast("float"))
        )

    # Define a placeholder URL for missing images
    placeholder_url = "https://github.com/Avcon900/Ecom_Products_Data_analytics/blob/master/placeholder_images/placeholder.png?raw=true"

    bronze_df = bronze_df.withColumn(
        "Image_URL",
        when(col("Image_URL").isNull(), placeholder_url)
        .otherwise(col("Image_URL"))
    )

    bronze_df = bronze_df.drop("Product_ID")

    # Truncate "Title" to "Product_Name" to a maximum of 5 words
    from pyspark.sql.functions import split, size, expr

    bronze_df = bronze_df.withColumn(
        "Product_Name",
        expr("""
            CASE 
                WHEN size(split(Title, ' ')) <= 5 THEN Title
                ELSE concat_ws(' ', slice(split(Title, ' '), 1, 5))
            END
        """)
    )

    # One-time creation of base directories
    os.makedirs("data_lake/silver/products", exist_ok=True)

    from pyspark.sql.functions import col, lit, max as spark_max
    import datetime # For default high-watermark and example update

    # Write the cleaned DataFrame to Delta Lake in the silver layer
    bronze_df.write.format("delta").mode("append").partitionBy("scrape_date").save("data_lake/silver/products/")

    new_max_date_obj = bronze_df.agg(spark_max("scrape_date")).collect()[0][0] # This is a datetime.date object or None

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

    spark.stop()  # Stop the Spark session when done