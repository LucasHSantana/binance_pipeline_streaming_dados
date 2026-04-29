import os
import sys
import signal
import logging
from dotenv import load_dotenv

import boto3
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.errors import StreamingQueryException

import settings

load_dotenv()

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

if settings.save_to_s3:
    REQUIRED_ENV_VARS = ['AWS_ACCESS_KEY', 'AWS_SECRET_KEY']

    for var in REQUIRED_ENV_VARS:
        if not os.getenv(var):
                raise EnvironmentError(F'The environment variable {var} is not defined.')
    

def stop_gracefully(signum, frame):
    print('Received a stop signal. Closing queries...')

    try:
        if 'query_s3' in globals() and query.isActive:
            query.stop()

        spark.stop()
        print('Spark closed successfully.')
    except Exception as e:
        print(f'Error on closing spark: {e}')
    finally:
        sys.exit()


def ensure_bucket_exists(bucket_name: str, region: str):
    s3_client = boto3.client(
        's3',
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key = os.getenv('AWS_SECRET_KEY'),
        region_name = region
    )

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f'Bucket {bucket_name} already exists.')
    except ClientError as e:
        error_code = e.response['Error']['Code']

        if error_code == '403':
            print(f'Permission error (403): Verify if your aws keys has access to s3 bucket {bucket_name}')
        if error_code == '404':
            print(f'Bucker {bucket_name} not found. Creating...')
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
            print(f'Bucket {bucket_name} created.')
        else:
            raise e
        

def monitor_batch(df: DataFrame, batch_id: int):
    logger.info(f'---------- BATCH {batch_id} STARTED ----------')
    count = df.count()
    logger.info(f'Registers processed: {count}')

    if count > 0:
        if settings.save_to_s3:
            df.write \
                .mode('append') \
                .parquet(f's3a://{settings.s3_bucket_name}/data/normalized/')
        
        df.show(5, truncate=False)


if settings.save_to_s3:
    packages = [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
        'org.apache.hadoop:hadoop-aws:3.3.4',
        'com.amazonaws:aws-java-sdk-bundle:1.12.262'
    ]

    spark = SparkSession.builder \
        .appName("BinanceStreaming") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", ','.join(packages)) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_KEY')) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
else:
    packages = [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
    ]

    spark = SparkSession.builder \
        .appName("BinanceStreaming") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", ','.join(packages)) \
        .getOrCreate()
    


raw_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', settings.kafka_servers) \
    .option('subscribe', settings.kafka_topic) \
    .option('startingOffsets', 'latest') \
    .load()

df = raw_df.select(from_json(col('value').cast('string'), settings.normalized_schema).alias('data'))
df = df.select('data.*')

df = df.withColumn(
    'trade_time_readable',
    (from_unixtime(col('trade_time') / 1000)).cast('timestamp')
)

df = df.dropna()
# Ensures Spark shut down gracefully to prevent checkpoint corruption
signal.signal(signal.SIGTERM, stop_gracefully)

try:
    if settings.save_to_s3:
        ensure_bucket_exists(settings.s3_bucket_name, settings.s3_bucket_region)
        checkpoint_path = f's3a://{settings.s3_bucket_name}/checkpoints/'
    else:
        checkpoint_path = '/tmp/checkpoints/'

    query = df.writeStream \
        .foreachBatch(monitor_batch) \
        .option('checkpointLocation', checkpoint_path) \
        .start()
    
    query.awaitTermination()
except StreamingQueryException as e:
    print(f'Error excecuting query: {e}')
except Exception as e:
    print(f'Unexpected error: {e}')
finally:
    spark.stop()