# Databricks notebook source
import logging
from pyspark.sql import SparkSession
import time

spark.catalog.clearCache()

# Set up the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remove the default handler from the root logger
logger.handlers.clear()

# Define the Hive table
hive_database = "default"
hive_table = "loggs"
incremental_column = "id"  # Incremental column name

# Create the log table if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_database}")
spark.sql(f"USE {hive_database}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {hive_table} (
        {incremental_column} STRING,  -- Incremental column definition
        level STRING,
        message STRING,
        timestamp STRING  
    )
    USING DELTA
""")

# Get the max value of the incremental column from the table
max_id = spark.sql(f"SELECT MAX({incremental_column}) FROM {hive_table}").collect()[0][0]

# Check if max_id is not None before calling split()
if max_id is not None:
    max_id = max_id.split('_')[-1]
else:
    max_id = 0  # Set a default value if max_id is None
#max_id = max_id.split('_')[-1]
# If max_id is None (table is empty), set initial_id to 1; otherwise, increment max_id by 1
initial_id = 1 if max_id == 0 else int(max_id) + 1

# Create a custom logging handler to insert log records into the Hive table
class HiveLogHandler(logging.Handler):
    def __init__(self, spark):
        super().__init__()
        self.spark = spark
        self.current_id = initial_id  # Initial value for the incremental column

    def emit(self, record):
        log_data = {
            incremental_column: f"{hive_database}_{hive_table}_{self.current_id}",
            "level": record.levelname,
            "message": str(record.msg),  # Use record.msg instead of record.message
            "timestamp": str(record.created)   
        }
        df = spark.createDataFrame([log_data])
        df.write.mode("append").insertInto(hive_table)
        self.current_id += 1  # Increment the incremental column value

# Configure the logger to use the custom Hive log handler
hive_handler = HiveLogHandler(spark)
formatter = logging.Formatter('%(levelname)s - %(message)s - %(name)s - %(asctime)s')
hive_handler.setFormatter(formatter)
logger.addHandler(hive_handler)

# Start the timer
start_time = time.time()

try:
    # Some code that may raise an exception
    result = 1 / 0
except Exception as e:
    # Log the exception with a different logging level
    logger.info('Exception')
    logger.exception('Exception occurred')
#logger.exception('Exception occurred')

# Calculate the duration
end_time = time.time()
duration = end_time - start_time

# Log the duration
logger.info(f"The task took {duration} seconds to complete.")

# Log a message
logger.info("ETL process started")

# Pull data from ADLS
logger.info("Pulling data from ADLS...")

# Perform ETL transformations
logger.info("Performing ETL transformations...")

# Push data to Databricks
logger.info("Pushing data to Databricks...")

# Log completion
logger.info("ETL process completed")

# Stop the SparkSession
# spark.stop()


# COMMAND ----------

logger.info("ETL process completed")

# COMMAND ----------

A = 2+3
