SHELL := /bin/bash

setup:
	@echo "Initializing the spark environment: build docker and spin up master/workers and history server"
	@docker compose up -d --build

#prepare_dataset:
#	@echo "Preparing the dataset with table format=$(table_format) for the spark job"
#	@docker exec -it spark-master spark-submit \
#		--master spark://spark-master:7077 \
#		/app/prepare_dataset.py --table-format=$(table_format);

clean:
	@echo "Clean up the spark environment"
	@docker-compose down
#
#run:
#	@echo "Submit the spark job with plugin $(plugin)"
#	@if [ "$(plugin)" = "comet" ]; then \
#	  	docker exec -it spark-master spark-submit \
#	  		--master spark://spark-master:7077 \
#	  		--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
#			--conf "spark.plugins=org.apache.spark.CometPlugin" \
#			--conf "spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager" \
#			--conf "spark.comet.exec.shuffle.enabled=true" \
#			--conf "spark.comet.exec.replaceSortMergeJoin=true" \
#			--conf "spark.comet.explainFallback.enabled=true" \
#			--conf "spark.sql.extendedExplainProviders=org.apache.comet.ExtendedExplainInfo" \
#			--conf "spark.memory.offHeap.enabled=true" \
#			--conf "spark.memory.offHeap.size=1g" \
#			/app/job.py --test-type=$(test_type) --plugin=$(plugin) --table-format=$(table_format); \
#	else \
#		docker exec -it spark-master spark-submit \
#			--master spark://spark-master:7077 \
#			--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
#			--conf "spark.memory.offHeap.enabled=true" \
#            --conf "spark.memory.offHeap.size=1g" \
#			/app/job.py --test-type=$(test_type) --plugin=$(plugin) --table-format=$(table_format); \
#	fi

generate_dataset:
	@echo "Generate the dataset with table format=$(table_format) for the spark job"
	@docker exec -it spark-master spark-submit \
		--master spark://spark-master:7077 \
		/app/generate_dataset.py --table-format=$(table_format);

test_join:
	@echo "Test the $(test_join) join with table format=$(table_format), plugin=$(plugin)"
	@if [ "$(plugin)" = "comet" ]; then \
	  	docker exec -it spark-master spark-submit \
	  		--master spark://spark-master:7077 \
			--conf "spark.plugins=org.apache.spark.CometPlugin" \
			--conf "spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager" \
			--conf "spark.comet.exec.shuffle.enabled=true" \
			--conf "spark.comet.exec.replaceSortMergeJoin=true" \
			--conf "spark.comet.explainFallback.enabled=true" \
			--conf "spark.sql.extendedExplainProviders=org.apache.comet.ExtendedExplainInfo" \
			--conf "spark.memory.offHeap.enabled=true" \
			--conf "spark.memory.offHeap.size=1g" \
			/app/test_join.py --table-format=$(table_format) --test-join=$(test_join); \
	else \
		docker exec -it spark-master spark-submit \
			--master spark://spark-master:7077 \
			--conf "spark.memory.offHeap.enabled=true" \
			--conf "spark.memory.offHeap.size=1g" \
			/app/test_join.py --table-format=$(table_format) --test-join=$(test_join); \
	fi