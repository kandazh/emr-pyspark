# main.py

from pyspark.sql import SparkSession
from utils import calculate_statistics, transform_data

def main():
    spark = SparkSession.builder \
        .appName('PySpark Job') \
        .getOrCreate()

    # Load data
    data = spark.read.csv('s3://kan-emr-works/mn-bills/2024-08/input/contact_dim_124_delta_2024-01-01_000.csv', header=True, inferSchema=True)

    # Transform data using a utility function
    transformed_data = transform_data(data)

    # Calculate statistics using a utility function
    stats = calculate_statistics(transformed_data)

    # Print statistics
    print(stats)

if __name__ == "__main__":
    main()
