.DEFAULT_GOAL := help
COMPOSE        = docker compose

.PHONY: help up down restart logs ps clean submit-spark

help:                         ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*##"}{printf "  \033[36m%-20s\033[0m %s\n",$$1,$$2}'

up:                           ## Start all services
	@cp -n .env.example .env 2>/dev/null || true
	$(COMPOSE) up -d --build

down:                         ## Stop and remove containers
	$(COMPOSE) down

restart:                      ## Restart all services
	$(COMPOSE) restart

logs:                         ## Tail logs (all services)
	$(COMPOSE) logs -f

ps:                           ## Show service status
	$(COMPOSE) ps

clean:                        ## Remove containers, volumes, and orphans
	$(COMPOSE) down -v --remove-orphans

submit-spark:                 ## Submit the Spark streaming job
	$(COMPOSE) exec spark-master \
	  spark-submit \
	    --master spark://spark-master:7077 \
	    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
	    /opt/bitnami/spark/jobs/spark_stream.py

kafka-topics:                 ## List Kafka topics
	$(COMPOSE) exec broker kafka-topics --bootstrap-server localhost:9092 --list

cassandra-shell:              ## Open a cqlsh session
	$(COMPOSE) exec cassandra cqlsh -u cassandra -p cassandra
