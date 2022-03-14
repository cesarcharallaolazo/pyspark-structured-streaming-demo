import argparse
import json

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import window
from pyspark.sql.types import StructType

from utils.constant import *
from utils import settings


def parse_cli_args():
    """
    Parse cli arguments
    returns a dictionary of arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--root_path', action='store', dest=root_path, type=str,
                        help='Store', default=None)

    parser.add_argument('--checkpoint_path', action='store', dest=checkpoint_path, type=str,
                        help='Store', default=None)

    # parser.add_argument('--app_env', action='store', dest=app_env, type=str,
    #                     help='Store', default=None)
    #
    # parser.add_argument('--version', action='store', dest=version, type=str,
    #                     help='Store', default=None)
    #
    # parser.add_argument('--org', action='store', dest=org, type=str,
    #                     help='Store', default=None)

    known_args, unknown_args = parser.parse_known_args()
    known_args_dict = vars(known_args)
    return known_args_dict


if __name__ == '__main__':
    args = parse_cli_args()

    # monitor
    args[monitor_raw_path] = f"{args[root_path]}/raw/"
    args[monitor_json_path] = f"{args[root_path]}/raw_json/"

    # Start Spark Environment
    # spark = SparkSession.builder.getOrCreate()
    spark = SparkSession.builder \
        .config("spark.redis.host", settings.DB_MICROSERVICE_REDIS_HOST) \
        .config("spark.redis.port", settings.DB_MICROSERVICE_REDIS_PORT) \
        .getOrCreate()

    # Checkpointing tuning strategy
    sc = SparkContext.getOrCreate()
    sc.setCheckpointDir(args[checkpoint_path])
    sc.setLogLevel("WARN")

    # ===========> STREAMING

    kafkamodelpredictionsRawStreamingDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.KAFKA_BROKERS) \
        .option("subscribe", settings.KAFKA_TOPIC_MODEL_PREDICTIONS) \
        .option("startingOffsets", "latest") \
        .load()

    kafkamodelpredictionsStreamingDF = kafkamodelpredictionsRawStreamingDF \
        .selectExpr("cast(key as string) key", "cast(value as string) value") \
        .withColumn("value", from_json("value", SCHEMA_MODEL_PREDICTIONS)) \
        .select(col('value.*')) \
        .select(col("prediction_details.specific_model").alias("specific_model"),
                col("prediction_details.value_predicted").alias("value_predicted"),
                "created_at") \
        .withColumn("created_at", to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss")) \
        .withWatermark("created_at", "3 minutes") \
        .groupBy(window("created_at", "2 minutes"), "specific_model") \
        .agg(count("value_predicted").alias("count_predictions"),
             sum("value_predicted").alias("sum_predictions"),
             avg("value_predicted").alias("avg_predictions"),
             stddev("value_predicted").alias("stddev_predictions"),
             skewness("value_predicted").alias("skewness_predictions"),
             kurtosis("value_predicted").alias("kurtosis_predictions"),
             min("value_predicted").alias("min_predictions"),
             max("value_predicted").alias("max_predictions")) \
        .select(col("window.*"), col("*")).drop("window")

    # 1. write to console
    kafkamodelpredictionsStreamingDF \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("checkpointLocation", args[checkpoint_path]) \
        .option("truncate", "false") \
        .start() \
        .awaitTermination()

    # 2. write to csv
    # kafkamodelpredictionsStreamingDF\
    #  .writeStream \
    #  .format("csv") \
    #  .outputMode("append")\
    #  .option("path", args[monitor_json_path] + "topic_data.csv") \
    #  .option("checkpointLocation", args[checkpoint_path]) \
    #  .start()\
    #  .awaitTermination()

    # 3. write to Kafka
    # kafkamodelpredictionsStreamingDF.selectExpr("cast('' as string) as key", "to_json(struct(*)) as value") \
    #     .writeStream \
    #     .format("kafka") \
    #     .outputMode("append") \
    #     .option("kafka.bootstrap.servers", settings.KAFKA_BROKERS) \
    #     .option("topic", "test.monitoring.model.predictions") \
    #     .option("checkpointLocation", args[checkpoint_path]) \
    #     .start() \
    #     .awaitTermination()

    # 4. write to Redis
    # # must be .mode("append")
    # def func(batchDF, batch_id):
    #     batchDF \
    #         .write \
    #         .format("org.apache.spark.sql.redis") \
    #         .option("table", "output") \
    #         .mode("append") \
    #         .save()
    #
    #
    # kafkamodelpredictionsStreamingDF \
    #     .writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(func) \
    #     .option("checkpointLocation", args[checkpoint_path]) \
    #     .start() \
    #     .awaitTermination()
