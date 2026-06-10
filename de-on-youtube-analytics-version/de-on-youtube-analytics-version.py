import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read each region separately with correct encoding
regions = {
    "ca": "iso-8859-1",
    "de": "iso-8859-1", 
    "fr": "iso-8859-1",
    "gb": "iso-8859-1",
    "in": "iso-8859-1",
    "jp": "utf-8",        # Japanese - UTF-8
    "kr": "utf-8",        # Korean - UTF-8
    "mx": "iso-8859-1",   # Spanish - Latin
    "ru": "utf-8",        # Russian - UTF-8
    "us": "utf-8"
}

combined_df = None

for region, encoding in regions.items():
    try:
        path = f"s3://de-on-youtube-raw-useast1-580661341769-dev/youtube/raw_statistics/region={region}/"
        
        df = spark.read.option("header", "true") \
                       .option("encoding", encoding) \
                       .option("quote", '"') \
                       .option("escape", '"') \
                       .option("multiLine", "true") \
                       .option("mode", "PERMISSIVE") \
                       .csv(path)
        
        # Add region column
        df = df.withColumn("region", F.lit(region))
        
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)
            
        print(f"Successfully loaded {region}")
        
    except Exception as e:
        print(f"Skipping {region} due to error: {str(e)}")
        continue

# Drop empty rows
combined_df = combined_df.dropna(how='all')

# Convert to DynamicFrame
cleaned = DynamicFrame.fromDF(combined_df, glueContext, "cleaned")

# Write to S3 as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=cleaned,
    connection_type="s3",
    connection_options={
        "path": "s3://de-on-youtube-raw-useast1-580661341769-dev/youtube/raw_statistics_parquet/",
        "partitionKeys": ["region"]
    },
    format="parquet"
)

job.commit()