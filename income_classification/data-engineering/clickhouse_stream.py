import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import clickhouse_connect  # Import the ClickHouse client module

# ClickHouse settings
CLICKHOUSE_HOST = 'clickhouse'  
CLICKHOUSE_PORT = 9000  
CLICKHOUSE_USER = '101:101' 
CLICKHOUSE_PASSWORD = 'clickhouse'  

def create_keyspace(session):
    session.execute("""
        CREATE DATABASE IF NOT EXISTS spark_streams
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.bank_income (
        id UUID PRIMARY KEY,
        age INTEGER,
        job STRING,
        target_income INTEGER
    )
    """)

    print("Table created successfully!")

def insert_data(session, **kwargs):
    print("Inserting data...")

    user_id = kwargs.get('id')
    age = kwargs.get('age')
    job = kwargs.get('job')
    target_income = kwargs.get('target_income')

    try:
        session.execute("""
            INSERT INTO spark_streams.bank_income (id, age, job, target_income)
            VALUES (%s, %s, %s, %s)
        """, (user_id, age, job, target_income))
        logging.info(f"Data inserted for {id}")
    except Exception as e:
        logging.error(f'Could not insert data due to {e}')

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 'com.datastax.spark:clickhouse-spark-runtime-3.3_2.12:0.7.2')\
            .config('spark.clickhouse.url', f'jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}')\
            .config('spark.clickhouse.username', CLICKHOUSE_USER)\
            .config('spark.clickhouse.password', CLICKHOUSE_PASSWORD)\
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("job", StringType(), False),
        StructField("target_income", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

    
if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka with Spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        # Connect to ClickHouse
        clickhouse_client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                                                         username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)

        if clickhouse_client is not None:
            create_keyspace(clickhouse_client)
            create_table(clickhouse_client)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.clickhouse")
                .option('checkpointLocation', '/tmp/checkpoint')
                .option('url', f'jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}')
                .option('database', 'spark_streams')
                .option('table', 'bank_income')
                .start())

            streaming_query.awaitTermination()
