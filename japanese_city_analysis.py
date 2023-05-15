
import urllib.request
import os

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("jp address download").getOrCreate()

url = 'https://www.post.japanpost.jp/zipcode/dl/roman/ken_all_rome.zip'
filename = 'ken_all_rome.zip'
data_folder = "./data/"
if not os.path.exists(data_folder):
    os.makedirs(data_folder)
full_download_path = os.path.join(data_folder, filename)
if os.path.exists(full_download_path):
    pass
else:
    urllib.request.urlretrieve(url, full_download_path)


import zipfile
import os

zip_filename = full_download_path
extract_path = data_folder

with zipfile.ZipFile(zip_filename, 'r') as zip_file:
    for file in zip_file.namelist():
        extracted_path = os.path.join(extract_path, file)
        if os.path.exists(extracted_path):
            with open(extracted_path, 'rb') as extracted_file:
                existing_contents = extracted_file.read()
            with zip_file.open(file, 'r') as new_file:
                new_contents = new_file.read()
            if existing_contents != new_contents:
                with open(extracted_path, 'wb') as outfile:
                    outfile.write(new_contents)
        else:
            zip_file.extract(file, extract_path)
#%%
# import pandas as pd

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