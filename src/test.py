from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Read File and List Columns") \
    .getOrCreate()

# File path (update as needed)
file_path = "s3://kan-emr-works/mn-bills/2024-08/input/contact_dim_124_delta_2024-01-01_000.csv"

try:
    # Read the file into a DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # List all columns
    columns = df.columns

    # Count the total number of rows
    total_count = df.count()

    # Print schema information
    print("Schema:")
    df.printSchema()

    # Print column names and their data types
    print("\nColumn Data Types:")
    for column in columns:
        print(f"{column}: {df.schema[column].dataType}")

    # Show a sample of the data
    print("\nSample Data:")
    df.show(5)  # Show the first 5 rows

    # Print the list of columns
    print("\nColumns:")
    for column in columns:
        print(column)

    # Print the total count of rows
    print(f"\nTotal count of rows: {total_count}")

    # Display basic statistics for numeric columns
    print("\nBasic Statistics for Numeric Columns:")
    numeric_cols = [col for col in columns if df.schema[col].dataType.typeName() in ('integer', 'double', 'float', 'long')]
    if numeric_cols:
        stats_df = df.describe(numeric_cols)
        stats_df.show()

except Exception as e:
    print(f"An error occurred: {e}")

# Stop the SparkSession
spark.stop()
