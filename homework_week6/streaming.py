from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit

from settings import RIDE_SCHEMA, CONSUME_TOPIC_RIDES_CSV_GREEN, CONSUME_TOPIC_RIDES_CSV_FHV, RESULT_TOPIC


def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()
    return df_stream


def parse_ride_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df['value'], ', ')

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query  # pyspark.sql.streaming.StreamingQuery


def sink_memory(df, query_name, query_template):
    query_df = df \
        .writeStream \
        .queryName(query_name) \
        .format("memory") \
        .start()
    query_str = query_template.format(table_name=query_name)
    query_results = spark.sql(query_str)
    return query_results, query_df


def sink_kafka(df, topic):
    write_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .outputMode('complete') \
        .option("topic", topic) \
        .option("checkpointLocation", "checkpoint") \
        .start()
    return write_query


def prepare_df_to_kafka_sink(df, value_columns, key_column=None):
    df = df.withColumn("value", F.concat_ws(', ', *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", df.key.cast('string'))
    else:
        df = df.withColumn("key", lit('1'))
        df = df.withColumn("key", df.key.cast('string'))
    return df.select(['key', 'value'])


def op_groupby(df, column_names):
    df_aggregation = df.groupBy(column_names).count()
    return df_aggregation

def chuck(var_consume_topic):
    # read_streaming data
    df_consume_stream = read_from_kafka(consume_topic=var_consume_topic) #CONSUME_TOPIC_RIDES_CSV_GREEN)
    #print(df_consume_stream.printSchema())

    # parse streaming data
    df_rides = parse_ride_from_kafka_message(df_consume_stream, RIDE_SCHEMA)
    #print(df_rides.printSchema())
    #sink_console(df_rides, output_mode='append')

    # write the output to the kafka topic
    df_trip_not_sure = prepare_df_to_kafka_sink(df=df_rides, value_columns=['PULocationID'])
    sink_console(df_trip_not_sure, output_mode='append')
    #fails:
    kafka_sink_query = sink_kafka(df=df_trip_not_sure, topic=RESULT_TOPIC)


if __name__ == "__main__":

    spark = SparkSession.builder.appName('streaming-examples').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    chuck(CONSUME_TOPIC_RIDES_CSV_GREEN)
    chuck(CONSUME_TOPIC_RIDES_CSV_FHV)



    df_consume_stream = read_from_kafka(consume_topic=RESULT_TOPIC) #CONSUME_TOPIC_RIDES_CSV_GREEN)
    # parse streaming data
    df_rides = parse_ride_from_kafka_message(df_consume_stream, RIDE_SCHEMA)
    df_trip_count_by_PULocationID = op_groupby(df_rides, ['PULocationID'])

    # write the output out to the console
    sink_console(df_trip_count_by_PULocationID)

    spark.streams.awaitAnyTermination()
