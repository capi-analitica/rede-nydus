from pyspark.sql import SparkSession

def main():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("Data Processing Job") \
        .getOrCreate()

    # Load data
    df = spark.read.csv("path/to/input/data.csv", header=True, inferSchema=True)

    # Data transformation example
    transformed_df = df.filter(df['column_name'] > 100)  # Example transformation

    # Write the transformed data to output
    transformed_df.write.csv("path/to/output/data.csv", header=True)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()