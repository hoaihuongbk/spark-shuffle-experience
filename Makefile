SHELL := /bin/bash

setup:
	@echo "Initializing the spark environment: build docker and spin up master/workers and history server"
	@docker compose up -d --build

download-data:
	@echo "Downloading the data from the internet, NYC taxi data"
	@for i in $$(seq -f "%02g" 1 12); do \
		wget -O apps/data/yellow_tripdata_2023-$$i.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-$$i.parquet; \
	done

fix-schema:
	@echo "Fixing the dataset by correcting the parquet files data type"
	@docker exec -it spark-master spark-submit \
		--master spark://spark-master:7077 \
		/app/fix_dataset.py;

clean:
	@echo "Clean up the spark environment"
	@docker-compose down

run:
	@echo "Submit the spark job with plugin $(plugin)"
	@if [ "$(plugin)" = "gluten" ]; then \
		docker exec -it spark-master spark-submit \
			--master spark://spark-master:7077 \
			--conf "spark.plugins=org.apache.gluten.GlutenPlugin" \
			--conf "spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager" \
			--conf "spark.memory.offHeap.enabled=true" \
			--conf "spark.memory.offHeap.size=1g" \
			/app/job.py $(test_type) $(plugin); \
	elif [ "$(plugin)" = "comet" ]; then \
	  	docker exec -it spark-master spark-submit \
	  		--master spark://spark-master:7077 \
			--conf "spark.plugins=org.apache.spark.CometPlugin" \
			--conf "spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager" \
			--conf "spark.comet.explainFallback.enabled=true" \
			--conf "spark.memory.offHeap.enabled=true" \
			--conf "spark.memory.offHeap.size=1g" \
			/app/job.py $(test_type) $(plugin); \
	else \
		docker exec -it spark-master spark-submit \
			--master spark://spark-master:7077 \
			/app/job.py $(test_type) $(plugin); \
	fi

