import os
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, BooleanType, LongType

kafka_servers = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
kafka_topic = 'trades.normalized'

s3_bucket_name = 'binance-data-lucas-2026'
s3_bucket_region = 'sa-east-1'

normalized_schema = StructType([
    StructField('event', StringType(), True),
    StructField('symbol', StringType(), True),
    StructField('trade_id', StringType(), True),
    StructField('price', DecimalType(18, 8), True),
    StructField('qty', DecimalType(18, 8), True),
    StructField('trade_time', LongType(), True),
    StructField('is_maker', BooleanType(), True)
]) 

save_to_s3 = os.getenv('SAVE_TO_S3', 'True').lower() == 'true'