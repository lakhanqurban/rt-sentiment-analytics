# Real-Time Sentiment Analytics on Streaming Social Media Data

## Project Overview

This project simulates a real-world streaming analytics system for processing
live social media data — such as posts from X (formerly Twitter) — and deriving
real-time sentiment signals from it. It demonstrates how to build scalable
streaming pipelines that handle high-velocity data and integrate machine
learning for analytics.

The system ingests posts through **Apache Kafka**, processes them in near real
time with **Spark Structured Streaming** — cleaning and tokenizing text, then
scoring polarity with **TextBlob** (-1.0 to 1.0) — and sinks the enriched
results to **PostgreSQL** for querying and dashboarding. A producer supports
both a synthetic **mock** data source (100,000 posts by default) and a live
**Tweepy** stream, while a labeled test harness validates sentiment accuracy
against a 70% target.

Together these pieces exercise the full analytics stack: message queuing,
distributed stream processing, text analytics, durable storage, and automated
testing.

## How it works

- **Producer** (`producer/`): Generates/publishes posts to a Kafka topic with
  batched sends, retries, and detailed logging. Supports two ingestion modes:
  **mock** (built-in synthetic posts, 100,000 by default) or **tweepy** (live
  Twitter stream).
- **Streaming consumer** (`spark-app/spark_stream.py`): Consumes the topic with
  Spark Structured Streaming, parses JSON, **cleans and tokenizes** the text,
  applies a sentiment UDF (TextBlob polarity/subjectivity), classifies the
  label, and writes enriched rows to Postgres using `foreachBatch`.
- **Storage** (`schema.sql`): PostgreSQL table (including `clean_text` and
  `tokens`) plus aggregate views for dashboards.
- **Tests** (`spark-app/generate_test_data.py`, `spark-app/validate_sentiment.py`):
  Generate a labeled dataset and validate sentiment accuracy against a 70%
  target.

## Architecture

```
producer.py (mock or tweepy) --Kafka(topic: social-posts)--> Spark Structured Streaming
                                                 |  (clean -> tokenize -> TextBlob polarity/label)
                                                 v
                                            Postgres.analysis.posts
                                                 |
                                             Dashboard / SQL queries
```

## Prerequisites

- Docker (with Docker Compose plugin)
- Python 3.9+ (only needed for the optional offline test tools)

## Setup & Execution

### Run order at a glance

```
[1] make up            start all containers
        |
[2] make setup         one-time: JDBC driver + Spark deps + NLTK corpora
        |
[3] make run-spark     start streaming job (KEEP RUNNING in terminal A)
        |
[4] make run-producer  publish 100k posts (terminal B)
        |
[5] make psql          verify results in Postgres
```

> Use `make ...` everywhere; the manual `docker compose ...` equivalents are
> listed under [Manual steps](#manual-steps-equivalent-to-the-make-targets).

### Step-by-step (Make)

1. **`make up`** — bring up Zookeeper, Kafka, Postgres, Spark master + worker.
   Wait until Kafka and Postgres report healthy:
   ```bash
   make up
   docker compose ps
   ```
   ➜ **Next:** `make setup`.

2. **`make setup`** — one-time provisioning: downloads `postgresql-42.5.1.jar`
   into `./drivers/`, `pip install`s the Spark Python deps on master + worker,
   and downloads the TextBlob/NLTK corpora into `/app/nltk_data`:
   ```bash
   make setup
   ```
   ➜ **Next:** `make run-spark`.

3. **`make run-spark`** — start the Spark streaming job. It consumes the Kafka
   topic, cleans/tokenizes text, scores sentiment, and writes to Postgres.
   This blocks, so **keep it running in terminal A**:
   ```bash
   make run-spark
   ```
   You should see: `Streaming query started. Awaiting termination.` and
   periodically `Wrote N rows to Postgres (posts).`
   ➜ **Next (new terminal):** `make run-producer`.

4. **`make run-producer`** — publish the posts (mock mode: 100,000 synthetic).
   It exits when done:
   ```bash
   make run-producer
   ```
   You should see: `Done. Sent 100000 posts ...`
   ➜ **Next:** `make psql`.

5. **`make psql`** — verify the processed results:
   ```bash
   make psql
   ```
   ```sql
   SELECT label, count(*), round(avg(polarity)::numeric, 4) FROM posts GROUP BY label;
   SELECT clean_text, tokens, label FROM posts LIMIT 10;
   SELECT * FROM post_summary;
   ```
   Expected: `100000` rows total, split across positive / neutral / negative.
   ➜ **Next:** run `make test` to validate sentiment accuracy, or stop with
   `make down`.

### Makefile targets

| Task | Command |
|---|---|
| Start stack | `make up` |
| One-time provisioning (driver + deps + corpora) | `make setup` |
| Start Spark streaming job | `make run-spark` |
| Run producer (mock) | `make run-producer` |
| Open psql shell | `make psql` |
| Validate sentiment accuracy | `make test` |
| Install Spark Python deps only | `make pip` |
| Stream logs | `make logs` |
| Stop stack | `make down` |
| Stop & remove all data (`-v`) | `make clean` |

### Manual steps (equivalent to the Make targets)

1. **Start the stack** (Zookeeper, Kafka, Postgres, Spark master + worker):

   ```bash
   docker compose up -d
   ```

   Wait for the health checks to pass:

   ```bash
   docker compose ps
   ```

2. **Install Python deps** inside the Spark containers (master/worker share
   `./spark-app`) — skip if you ran `make setup`:

   ```bash
   docker compose exec spark-master pip install -r /app/requirements.txt
   docker compose exec spark-worker pip install -r /app/requirements.txt
   ```

3. **Download the Postgres JDBC driver** into `./drivers/`. `make setup` does
   this automatically. Because `./drivers` is a bind mount into both containers,
   the jar is already available at `/drivers/postgresql-42.5.1.jar` — no `cp`
   step is needed:

   ```bash
   # only if ./drivers is empty and you did NOT run make setup:
   curl -fL -o drivers/postgresql-42.5.1.jar \
     https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar
   ```

4. **Download TextBlob's NLTK corpora** once (also done by `make setup`). The
   container runs as a non-root user, so corpora go into the shared `/app`
   bind mount at `/app/nltk_data`:

   ```bash
   docker compose exec -e NLTK_DATA=/app/nltk_data spark-master python /app/download_corpora.py
   ```

   `NLTK_DATA=/app/nltk_data` is already set on both Spark services in
   `docker-compose.yml`, so the streaming job finds the corpora at runtime.
   (The helper script is used instead of `textblob.download_corpora`, which
   hardcodes the unwritable `/nltk_data` directory.)

5. **Start the Spark streaming job** on the driver (keep it running):

   ```bash
   docker compose exec spark-master spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
     --driver-class-path /drivers/postgresql-42.5.1.jar \
     --conf spark.executor.extraClassPath=/drivers/postgresql-42.5.1.jar \
     --master spark://spark-master:7077 \
     /app/spark_stream.py
   ```

   This streams the topic into Postgres. Keep it running in one terminal.
   (Equivalently: `make run-spark`.)

6. **Run the producer** to publish the posts (see
   [Ingestion modes](#ingestion-modes-mock-vs-tweepy)):

   ```bash
   # Default: mock mode, 100,000 synthetic posts
   docker compose up --build producer
   ```

   Equivalently: `make run-producer`.

7. **Inspect results in Postgres** (`make psql`):

   ```bash
   docker compose exec postgres psql -U postgres -d sentinel
   ```

   ```sql
   SELECT count(*) FROM posts;
   SELECT label, count(*), round(avg(polarity)::numeric, 4) FROM posts GROUP BY label;
   SELECT clean_text, tokens, label FROM posts LIMIT 10;
   SELECT * FROM post_summary;
   ```

## Ingestion modes: mock vs tweepy

The producer has two data sources, selected via the `INGEST_MODE` environment
variable. `"or"` in the specification is satisfied by either path; the default
is **mock**.

### Mock mode (default)

Built-in synthetic posts with realistic fields (`user`, `timestamp`,
`like_count`, `retweet_count`) and curated positive/neutral/negative phrasing —
no external credentials required. It publishes exactly `NUM_POSTS` (default
100,000) to the Kafka topic and exits.

```bash
docker compose up --build producer
```

### Tweepy mode (live Twitter)

Streams real tweets and maps them into the same post schema before sending to
Kafka. Requires a Twitter API **bearer token**; `tweepy` is already listed in
`producer/requirements.txt`.

Enable it by uncommenting/setting these variables, then rebuild:

```yaml
# docker-compose.yml -> producer service
INGEST_MODE: tweepy
TWITTER_BEARER_TOKEN: "your-bearer-token"
TWITTER_TRACK: "python,kafka,data"   # comma-separated keywords to follow
TWITTER_MAX_POSTS: "100000"            # stop after this many tweets
```

```bash
docker compose up --build producer
```

If the token is missing or tweepy is not installed, the producer **falls back
to mock mode** and logs a warning, so the pipeline never silently empties.

> Live tweets are captured up to `TWITTER_MAX_POSTS`; for a reproducible batch
> of exactly 100,000 posts, use mock mode.

## Configuration

Configuration is handled through environment variables (see `docker-compose.yml`):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka endpoint for both producer & job |
| `KAFKA_TOPIC` | `social-posts` | Topic name |
| `INGEST_MODE` | `mock` | `mock` (synthetic) or `tweepy` (live Twitter) |
| `NUM_POSTS` | `100000` | Number of synthetic posts to publish (mock mode) |
| `BATCH_SIZE` | `1000` | Producer batch size before flush |
| `TWITTER_BEARER_TOKEN` | *(empty)* | Twitter API bearer token (tweepy mode) |
| `TWITTER_TRACK` | `python,kafka,data` | Comma-separated keywords to follow |
| `TWITTER_MAX_POSTS` | `1000` | Stop after this many live tweets |
| `TWITTER_TIMEOUT` | `300` | Live stream timeout in seconds |
| `JDBC_URL` | `jdbc:postgresql://postgres:5432/sentinel` | Postgres JDBC URL |
| `PG_USER` / `PG_PASSWORD` | `postgres` / `postgres` | DB credentials |

## Testing

Generate a labeled dataset (100,000 posts) and validate sentiment accuracy on a
sample (or `make test`):

```bash
docker compose exec spark-master python /app/generate_test_data.py --num 100000 --out /tmp/test_posts.jsonl
docker compose exec spark-master python /app/validate_sentiment.py --input /tmp/test_posts.jsonl --sample 5000
```

The validator compares the TextBlob prediction against `expected_label` and
prints the accuracy. The target is **> 70%**; the validator logs a warning if it
falls below.

### Offline (no Docker) run

These two scripts only depend on `textblob`; run them locally too:

```bash
pip install textblob
python3 tests/generate_test_data.py --num 10000 --out test_posts.jsonl
python3 tests/validate_sentiment.py --input test_posts.jsonl --sample 1000
```

## Scalability

- **Kafka**: increase `KAFKA_NUM_PARTITIONS` in `docker-compose.yml` and spread
  partitions across more consumers.
- **Spark**: add workers via `docker compose up --scale spark-worker=3` and bump
  `--executor-cores`/`--executor-memory`; set `spark.executor.instances`
  accordingly on a cluster deploy.
- **Postgres**: use an external managed database for higher write throughput.

## Innovation: MLlib sentiment model

If Textblob's accuracy is insufficient, replace the UDF with a learned model:

```python
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import RandomForestClassifier, Pipeline

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashing = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=10000)
idf = IDF(inputCol="raw_features", outputCol="features")
clf = RandomForestClassifier(labelCol="label", featuresCol="features")

pipeline = Pipeline(stages=[tokenizer, hashing, idf, clf])
```

Train on the labeled dataset, serialize with `model.save()`, and load inside the
streaming UDF/batch to produce predictions instead of TextBlob polarity.
