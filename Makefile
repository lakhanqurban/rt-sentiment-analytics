.PHONY: setup up down logs ps build-spark run-producer test stop

DRIVER_URL = https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar
DRIVER     = drivers/postgresql-42.5.1.jar
SPARK_SUBMIT = docker compose exec spark-master spark-submit

## Start the full stack (Zookeeper, Kafka, Postgres, Spark)
up:
	docker compose up -d

## Bring everything down
down:
	docker compose down

## Stream logs from all services
logs:
	docker compose logs -f --tail=100

stop:
	docker compose stop

## Setup: pull driver jar, install deps, download corpora (run once after `make up`)
setup: pip $(DRIVER)
	docker compose exec -e NLTK_DATA=/app/nltk_data spark-master python /app/download_corpora.py

$(DRIVER):
	curl -fL -o $(DRIVER) $(DRIVER_URL)

## Convenience parsing helpers (used by other targets)
define spark_install
	docker compose exec spark-master pip install -r /app/requirements.txt
	docker compose exec spark-worker pip install -r /app/requirements.txt
endef

## Install Python deps into the Spark nodes
pip:
	$(spark_install)

## Start the Spark streaming job (run in its own terminal)
run-spark:
	$(SPARK_SUBMIT) \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
		--driver-class-path /drivers/postgresql-42.5.1.jar \
		--conf spark.executor.extraClassPath=/drivers/postgresql-42.5.1.jar \
		--master spark://spark-master:7077 \
		/app/spark_stream.py

## Run the producer (mock mode by default; set INGEST_MODE=tweepy for live)
run-producer:
	docker compose up --build producer

## Open a psql shell into the database
psql:
	docker compose exec postgres psql -U postgres -d sentinel

## Generate a labeled test dataset and validate sentiment accuracy
test:
	docker compose exec spark-master python /app/generate_test_data.py --num 100000 --out /tmp/test_posts.jsonl
	docker compose exec spark-master python /app/validate_sentiment.py --input /tmp/test_posts.jsonl --sample 5000

## Remove everything (including Postgres data volume)
clean:
	docker compose down -v

.PHONY: all