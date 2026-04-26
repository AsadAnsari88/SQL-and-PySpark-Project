from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# Initialize the session
spark = SparkSession.builder \
    .appName("BigDataProject") \
    .getOrCreate()

# Create the raw data
data = [
    (1, "Laptop", "Electronics", 50000, 1),
    (2, "Mouse", "Electronics", 1000, 2),
    (3, "Table", "Furniture", 5000, 1),
    (4, "Phone", "Electronics", 20000, 1),
    (5, "Chair", "Furniture", 3000, 4)
]

# Define the schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Product", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Price", IntegerType(), True),
    StructField("Quantity", IntegerType(), True)
])

# Create the DataFrame
df = spark.createDataFrame(data, schema)

# Step A: Show raw data
print("Raw Sales Data:")
print(df.toPandas().to_string(index=False))

# Step B: Calculate Revenue per item
df_revenue = df.withColumn("Revenue", col("Price") * col("Quantity"))

# Step C: Group by Category for total revenue
result = df_revenue.groupBy("Category").sum("Revenue")

# Step D: Final output
print("\nFinal Category-wise Revenue Analysis:")
print(result.toPandas().to_string(index=False))

# Step E: Clean up
spark.stop()
