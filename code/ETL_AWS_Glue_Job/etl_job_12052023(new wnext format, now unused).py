###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, collect_set, array_join,struct

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job




##### FROM FILES
tedx_dataset_path = "s3://unibg-tecn-cloud-sfmr01/tedx_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
    
#tedx_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ TAGS DATASET
tags_dataset_path = "s3://unibg-tecn-cloud-sfmr01/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)



# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
#    .drop("idx") \

#tedx_dataset_agg.printSchema()

##READ WATCH NEXT DATASET
watch_dataset_path = "s3://unibg-tecn-cloud-sfmr01/watch_next_dataset.csv"
watch_dataset = spark.read.option("header","true").csv(watch_dataset_path)

#CREATE THE AGGREGATE MODEL, AGAIN, THEN ADD THE WATCH NEXT TO TEDX-TAGS DATASET
temp_aggregate= tedx_dataset_agg.select(col("idx").alias("wnext_id"),col("url").alias("wnext_url"))
watch_dataset_agg = watch_dataset.filter(~ watch_dataset['url'].rlike('watch-later'))
watch_dataset_agg = watch_dataset_agg.join(temp_aggregate, watch_dataset_agg.url==temp_aggregate.wnext_url,"left")\
    .groupBy(col("idx").alias("idx_ref")) \
    .agg(collect_set(struct("url","wnext_id")).alias("wnext"))
#watch_dataset_agg.printSchema()
tedx_dataset_agg2 = tedx_dataset_agg.join(watch_dataset_agg, tedx_dataset_agg.idx == watch_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

    
    

#tedx_dataset_agg2.printSchema()



mongo_uri = "mongodb+srv://pass:word@cluster0.s4pcsx4.mongodb.net"
print(mongo_uri)

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2023_FullData",
    "collection": "tedx_data_full",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg2, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
