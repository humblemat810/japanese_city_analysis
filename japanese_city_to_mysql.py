
import urllib.request
import os

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("jp address download").getOrCreate()


schema = {
    'zipcode': 'int64',    
    'prefecture_jp': 'string',
    'citymachi_jp': 'string',
    'machi_jp': 'string',
    'prefecture_en': 'string',
    'citymachi_en': 'string',
    'machi_en': 'string'
}
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the Spark schema
spark_schema = StructType([
    StructField('zipcode', IntegerType()),
    StructField('prefecture_jp', StringType()),
    StructField('citymachi_jp', StringType()),
    StructField('machi_jp', StringType()),
    StructField('prefecture_en', StringType()),
    StructField('citymachi_en', StringType()),
    StructField('machi_en', StringType())
])
# Read the CSV file with the specified column names and schema

# df = pd.read_csv("./data/KEN_ALL_ROME.csv",
#                  encoding="cp932", 
#                  names=schema.keys(), 
#                  dtype=schema)

df = spark.read.csv("./data/KEN_ALL_ROME.csv",
                 encoding="cp932", 
                 schema=spark_schema)

# Print the first few rows of the DataFrame
df.show(5)


# Stop the Spark session
spark.stop()