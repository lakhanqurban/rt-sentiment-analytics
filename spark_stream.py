import logging
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, udf
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from textblob import TextBlob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social-posts")
JDBC_URL = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/sentinel")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("text", StringType(), True),
        StructField("user", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("like_count", IntegerType(), True),
        StructField("retweet_count", IntegerType(), True),
    ]
)


def _clean(text):
    """Lowercase, strip URLs/@mentions/hashtags/punctuation, collapse whitespace."""
    try:
        text = str(text or "")
        text = re.sub(r"https?://\S+|www\.\S+", " ", text)
        text = re.sub(r"@\w+", " ", text)
        text = re.sub(r"#", " ", text)
        text = re.sub(r"[^a-zA-Z0-9\s']", " ", text)
        return re.sub(r"\s+", " ", text).strip().lower()
    except Exception as e:
        logger.warning("Clean error for text %r: %s", text, e)
        return ""


def _tokenize(cleaned):
    if not cleaned:
        return []
    return [w for w in cleaned.split() if w]


clean_udf = udf(_clean, StringType())
tokenize_udf = udf(_tokenize, ArrayType(StringType()))


def _sentiment(text):
    """Return (polarity, subjectivity) for text via TextBlob.

    Polarity ranges from -1.0 (negative) to 1.0 (positive).
    Subjectivity ranges from 0.0 (objective) to 1.0 (subjective).
    """
    try:
        if not text:
            return 0.0, 0.0
        blob = TextBlob(text)
        return round(blob.sentiment.polarity, 4), round(blob.sentiment.subjectivity, 4)
    except Exception as e:  # pragma: no cover
        logger.warning("Sentiment error for text %r: %s", text, e)
        return 0.0, 0.0


def _classify(polarity):
    if polarity > 0.1:
        return "positive"
    if polarity < -0.1:
        return "negative"
    return "neutral"


SENTIMENT_SCHEMA = StructType(
    [
        StructField("polarity", DoubleType(), False),
        StructField("subjectivity", DoubleType(), False),
    ]
)
sentiment_udf = udf(_sentiment, SENTIMENT_SCHEMA)
classify_udf = udf(_classify, StringType())


def get_spark():
    spark = SparkSession.builder.appName("StreamingSentiment").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    return spark


def main():
    spark = get_spark()
    logger.info("Starting Spark streaming consumer on topic=%s", KAFKA_TOPIC)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), SCHEMA).alias("post"))
        .select(
            col("post.id").alias("id"),
            col("post.text").alias("text"),
            col("post.user").alias("username"),
            to_timestamp(col("post.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX")
            .alias("timestamp"),
            col("post.like_count").alias("like_count"),
            col("post.retweet_count").alias("retweet_count"),
        )
    )

    enriched = (
        parsed.select(
            "*",
            clean_udf(col("text")).alias("clean_text"),
        )
        .select(
            "*",
            tokenize_udf(col("clean_text")).alias("tokens"),
            sentiment_udf(col("clean_text")).alias("sentiment"),
        )
        .select(
            "id",
            "text",
            "clean_text",
            "tokens",
            "username",
            "timestamp",
            "like_count",
            "retweet_count",
            col("sentiment.polarity").alias("polarity"),
            col("sentiment.subjectivity").alias("subjectivity"),
            classify_udf(col("sentiment.polarity")).alias("label"),
        )
    )

    jdbc_opts = {
        "url": JDBC_URL,
        "user": PG_USER,
        "password": PG_PASSWORD,
    }

    query = (
        enriched.writeStream.outputMode("append")
        .option("checkpointLocation", "/app/checkpoints")
        .trigger(processingTime="5 seconds")
        .foreachBatch(lambda batch_df, batch_id: _write_batch(batch_df, jdbc_opts))
        .start()
    )

    logger.info("Streaming query started. Awaiting termination.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stream stopped gracefully.")


def _write_batch(batch_df, opts):
    if batch_df.isEmpty():
        return
    rows = batch_df.count()
    try:
        batch_df.write.jdbc(
            url=opts["url"],
            table="posts",
            mode="append",
            properties={
                "user": opts["user"],
                "password": opts["password"],
                "driver": "org.postgresql.Driver",
            },
        )
        logger.info("Wrote %d rows to Postgres (posts).", rows)
    except Exception as e:
        logger.error("JDBC write failed on batch %d rows: %s", rows, e)
        raise


if __name__ == "__main__":
    main()