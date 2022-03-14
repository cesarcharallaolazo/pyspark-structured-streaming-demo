build:
	cp ./spark/run_spark_structured_streaming.py ./target
	cd ./spark && zip -x run_spark_structured_streaming.py -r ../target/spark.zip .
