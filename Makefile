SHELL := /bin/bash

setup:
	@echo "Initializing the spark environment: build docker and spin up master/workers and history server"
	@docker compose up -d --build

prepare_dataset:
	@echo "Preparing the dataset for the spark job"
	@docker exec -it spark-master spark-submit \
		--master spark://spark-master:7077 \
		/app/prepare_dataset.py;

clean:
	@echo "Clean up the spark environment"
	@docker-compose down

run:
	@echo "Submit the spark job with plugin $(plugin)"
	@if [ "$(plugin)" = "comet" ]; then \
	  	docker exec -it spark-master spark-submit \
	  		--master spark://spark-master:7077 \
	  		--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
			--conf "spark.plugins=org.apache.spark.CometPlugin" \
			--conf "spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager" \
			--conf "spark.comet.exec.shuffle.enabled=true" \
			--conf "spark.comet.exec.replaceSortMergeJoin=true" \
			--conf "spark.comet.explainFallback.enabled=true" \
			--conf "spark.sql.extendedExplainProviders=org.apache.comet.ExtendedExplainInfo" \
			--conf "spark.memory.offHeap.enabled=true" \
			--conf "spark.memory.offHeap.size=1g" \
			/app/job.py $(test_type) $(plugin); \
	else \
		docker exec -it spark-master spark-submit \
			--master spark://spark-master:7077 \
			--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
			--conf "spark.memory.offHeap.enabled=true" \
            --conf "spark.memory.offHeap.size=1g" \
			/app/job.py $(test_type) $(plugin); \
	fi

