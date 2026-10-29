# src/etl_pipeline.py

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

# Set up logger for this module
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create a Spark session for our ETL job"""
    return SparkSession.builder \
        .appName("Grocery_Daily_ETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def extract_sales_data(spark, input_path):
    """Read sales CSVs with all their real-world messiness"""

    logger.info(f"Reading sales data from {input_path}")

    expected_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("region", StringType(), True)
    ])

    df = spark.read.csv(
        input_path,
        header=True,
        schema=expected_schema,
        mode="PERMISSIVE"
    )

    total_records = df.count()
    logger.info(f"Found {total_records} total records")

    return df

def extract_all_data(spark):
    """Combine data from multiple sources"""

    # Each system exports differently
    online_orders = extract_sales_data(spark, "data/raw/online_orders.csv")
    store_orders = extract_sales_data(spark, "data/raw/store_orders.csv")
    mobile_orders = extract_sales_data(spark, "data/raw/mobile_orders.csv")

    # Union them all together
    all_orders = online_orders.unionByName(store_orders).unionByName(mobile_orders)

    logger.info(f"Combined dataset has {all_orders.count()} orders")
    return all_orders

##The logic here: if it already starts with "CUST_", keep it.
# If it's just numbers (rlike("^[0-9]+$") checks for that), add the "CUST_" prefix. Everything else stays as-is for now. 
# This gives us a consistent format to work with downstream.

def clean_customer_id(df):
    """Standardize customer IDs (some are numbers, some are CUST_123 format)"""

    df_cleaned = df.withColumn(
        "customer_id_cleaned",
        when(col("customer_id").startswith("CUST_"), col("customer_id"))
        .when(col("customer_id").rlike("^[0-9]+$"), concat(lit("CUST_"), col("customer_id")))
        .otherwise(col("customer_id"))
    )

    return df_cleaned.drop("customer_id").withColumnRenamed("customer_id_cleaned", "customer_id")

# src/etl_pipeline.py (continuing in same file)

def clean_price_column(df):
    """Fix the price column"""

    # Remove dollar signs, commas, etc. (keep digits, decimals, and negatives)
    df_cleaned = df.withColumn(
        "price_cleaned",
        regexp_replace(col("price"), r"[^0-9.\-]", "")
    )

    # Convert to decimal, default to 0 if it fails
    df_final = df_cleaned.withColumn(
        "price_decimal",
        when(col("price_cleaned").isNotNull(),
             col("price_cleaned").cast(DoubleType()))
        .otherwise(0.0)
    )

    # Flag suspicious values for review --The regexp_replace function strips out everything that isn't a digit or decimal point.
    #  Then we convert to a proper decimal type. The quality flag column helps us track suspicious values without throwing them 
    # out.
    df_flagged = df_final.withColumn(
        "price_quality_flag",
        when(col("price_decimal") == 0.0, "CHECK_ZERO_PRICE")
        .when(col("price_decimal") > 1000, "CHECK_HIGH_PRICE")
        .when(col("price_decimal") < 0, "CHECK_NEGATIVE_PRICE")
        .otherwise("OK")
    )

    bad_price_count = df_flagged.filter(col("price_quality_flag") != "OK").count()
    logger.warning(f"Found {bad_price_count} orders with suspicious prices")

    return df_flagged.drop("price", "price_cleaned")

