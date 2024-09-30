from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, regexp_replace, lower, lit, unix_timestamp,split, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType


spark = SparkSession.builder \
    .appName("PySpark Snowflake PostgreSQL Connection") \
    .config("spark.jars", "postgresql-42.7.3.jar") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.0") \
    .config("spark.executor.extraClassPath", "/home/cipher/pyspark_files/snowflake-jdbc-3.13.14.jar:/home/cipher/pyspark_files/spark-snowflake_2.12-2.10.0-spark_3.0.jar")\
    .getOrCreate()


# Defining PostgreSQL connection properties
postgres_url = "jdbc:postgresql://localhost:5432/postgres"
postgres_properties = {
    "user": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}


# Load data from PostgreSQL into a DataFrame
df = spark.read.jdbc(url=postgres_url, table="public.de_jobs", properties=postgres_properties)

# Convert created_date to Timestamp
df = df.withColumn("created_date", col("created_date").cast(TimestampType()))

# Handle missing values in salary_max
# Option 1: Impute missing values using the salary_min value
df = df.withColumn("salary_max", when(col("salary_max").isNull(), col("salary_min")).otherwise(col("salary_max")))


# Categorize job titles into Senior/Junior
df = df.withColumn("title_category", 
                   when(col("job_title").rlike("(?i)senior|lead"), "Senior")
                   .when(col("job_title").rlike("(?i)junior|entry"), "Junior")
                   .otherwise("Mid-level"))

# Filter jobs with salary_min above a threshold (e.g., 50,000)
df = df.filter(col("salary_min") >= 50000)

# Drop the redirect_url column
df = df.drop("redirect_url")


# Defining Snowflake connection properties
SFOptions = {
  "sfURL" : "<account_url>",
  "sfUser" : "<user>",
  "sfPassword" : "<password>",
  "sfDatabase" : "<database>",
  "sfSchema" : "<schema>",
  "sfWarehouse" : "<warehouse>",
  "sfRole" : "<account_role>"
}

df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**SFOptions) \
    .option("dbtable", "<destination_table>") \
    .mode("overwrite") \
    .save()

print("Data written to Snowflake successfully!")

# Stop the Spark session
spark.stop()

