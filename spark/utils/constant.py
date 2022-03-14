from pyspark.sql.types import *
from pyspark.sql.functions import *

root_path = "root_path"
checkpoint_path = "checkpoint_path"
org = "org"
app_env = "app_env"
version = "version"

# monitor
monitor_raw_path = "monitor_raw_path"
monitor_json_path = "monitor_json_path"

SCHEMA_MODEL_PREDICTIONS = StructType(
    [
        StructField("entity",
                    StructType(
                        [
                            StructField("id", StringType(), True),
                            StructField("name", StringType(), True)
                        ]), True),
        StructField("model",
                    StructType(
                        [
                            StructField("id", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("country", StringType(), True),
                            StructField("version", StringType(), True)
                        ]), True),
        StructField("entity_id",
                    StructType(
                        [
                            StructField("user_id", StringType(), True),
                        ]), True),
        StructField("prediction_details",
                    StructType(
                        [
                            StructField("specific_model", StringType(), True),
                            StructField("value_predicted", FloatType(), True)
                        ]), True),
        StructField("created_at", StringType(), True)
    ]
)
