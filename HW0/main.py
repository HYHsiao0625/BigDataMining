from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, count, col, mean, stddev

# Create a SparkSession
spark = SparkSession.builder.appName("HouseholdPowerConsumption").getOrCreate()

# Load the dataset from HDFS with complete URI
hdfs_path = "hdfs://192.168.40.130:9000/user/sparkmaster/household_power_consumption.txt" 
df = spark.read.csv(hdfs_path, sep=";", header=True, inferSchema=True)

df.show()

# Select the columns of interest
columns = ["Global_active_power", "Global_reactive_power", "Voltage", "Global_intensity"]

# Cast columns to numeric type (float)
for column in columns:
    df = df.withColumn(column, col(column).cast("float"))

# Subtask 1: Min, Max, and Count
for column in columns:
    min_val = df.agg(min(col(column))).collect()[0][0]
    max_val = df.agg(max(col(column))).collect()[0][0]
    count_val = df.agg(count(col(column))).collect()[0][0]
    print(f"{column}: Min = {min_val}, Max = {max_val}, Count = {count_val}")

# Subtask 2: Mean and Standard Deviation
for column in columns:
    mean_val = df.agg(mean(col(column))).collect()[0][0]
    std_val = df.agg(stddev(col(column))).collect()[0][0]
    print(f"{column}: Mean = {mean_val}, Standard Deviation = {std_val}")

# Subtask 3: Min-Max Normalization
for column in columns:
    min_val = df.agg(min(col(column))).collect()[0][0]
    max_val = df.agg(max(col(column))).collect()[0][0]
    df = df.withColumn(f"normalized_{column}", (col(column) - min_val) / (max_val - min_val))

# Select the normalized columns
normalized_df = df.select([col("normalized_" + column) for column in columns]) 

normalized_df.show()
# Save the normalized data to HDFS
output_hdfs_path = "hdfs://192.168.40.130:9000/user/sparkmaster/output/"
normalized_df.write.csv(output_hdfs_path, header=True, mode="overwrite")