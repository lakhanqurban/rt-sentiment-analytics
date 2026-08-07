import json
import os
import random
import time
from datetime import datetime

import logging

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social-posts")
NUM_POSTS = int(os.getenv("NUM_POSTS", "100000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

# "mock" uses the built-in synthetic generator; "tweepy" streams live tweets.
INGEST_MODE = os.getenv("INGEST_MODE", "mock").lower()
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")
TWITTER_TRACK = os.getenv("TWITTER_TRACK", "python,kafka,data")
TWITTER_MAX_POSTS = int(os.getenv("TWITTER_MAX_POSTS", "1000"))
TWITTER_TIMEOUT = int(os.getenv("TWITTER_TIMEOUT", "300"))

POSITIVE = [
    "Absolutely loved the new product, it is amazing!",
    "Having a fantastic day, everything is going great.",
    "This update is fantastic and truly awesome.",
    "Great customer support, very helpful and kind.",
    "Wow, what a wonderful experience. Highly recommend!",
]
NEGATIVE = [
    "The service is terrible and I am very disappointed.",
    "This is a horrible product, waste of money.",
    "Worst experience ever, avoid at all costs.",
    "I hate the new layout, it is so confusing.",
    "The app crashes constantly, unacceptable quality.",
]
NEUTRAL = [
    "The weather today is what it is.",
    "Just reading the news, nothing special.",
    "Having coffee and thinking about the week.",
    "Not sure about this, maybe later.",
    "Checking the schedule for tomorrow.",
]
USERS = ["alice", "bob", "carol", "dave", "erin", "frank", "grace", "henry"]


def build_post(post_id: int) -> dict:
    roll = random.random()
    if roll < 0.4:
        text = random.choice(POSITIVE)
    elif roll < 0.8:
        text = random.choice(NEUTRAL)
    else:
        text = random.choice(NEGATIVE)

    return {
        "id": post_id,
        "text": text,
        "user": random.choice(USERS),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "like_count": random.randint(0, 5000),
        "retweet_count": random.randint(0, 1000),
    }


def main():
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )

    if INGEST_MODE == "tweepy":
        _run_tweepy(producer)
    else:
        _run_mock(producer)

    producer.flush()
    logger.info("Producer finished (mode=%s).", INGEST_MODE)


def _run_mock(producer):
    batch = []
    total_sent = 0
    failed = 0
    start = time.time()

    for post_id in range(NUM_POSTS):
        post = build_post(post_id)
        batch.append((KAFKA_TOPIC, post))
        if len(batch) >= BATCH_SIZE:
            sent, errors = _flush(producer, batch)
            total_sent += sent
            failed += errors
            batch = []
            if total_sent % (BATCH_SIZE * 10) == 0:
                logger.info(
                    "Produced %d posts (%d failed) in %.2fs",
                    total_sent,
                    failed,
                    time.time() - start,
                )

    if batch:
        sent, errors = _flush(producer, batch)
        total_sent += sent
        failed += errors

    elapsed = time.time() - start
    logger.info(
        "Done. Sent %d posts in %.2fs (%.1f posts/s), %d failures",
        total_sent,
        elapsed,
        total_sent / elapsed if elapsed else 0,
        failed,
    )


def _run_tweepy(producer):
    if not TWITTER_BEARER_TOKEN:
        logger.error(
            "INGEST_MODE=tweepy requires TWITTER_BEARER_TOKEN. Falling back to mock mode."
        )
        return _run_mock(producer)

    try:
        import tweepy
    except ImportError:
        logger.error("tweepy is not installed. Falling back to mock mode.")
        return _run_mock(producer)

    client = tweepy.StreamingClient(TWITTER_BEARER_TOKEN)

    class Listener(tweepy.StreamingClient):
        def __init__(self, bearer_token, producer):
            super().__init__(bearer_token)
            self.producer = producer
            self.count = 0
            self.failed = 0

        def on_tweet(self, tweet):
            post = {
                "id": tweet.id,
                "text": tweet.text,
                "user": (tweet.author_id if tweet.author_id else "unknown"),
                "timestamp": (tweet.created_at.isoformat() + "Z"
                              if getattr(tweet, "created_at", None) else
                              datetime.utcnow().isoformat() + "Z"),
                "like_count": None,
                "retweet_count": None,
            }
            try:
                self.producer.send(KAFKA_TOPIC, post)
                self.count += 1
            except Exception as e:
                self.failed += 1
                logger.error("Error sending tweet %s: %s", post["id"], e)

            if self.count and self.count % BATCH_SIZE == 0:
                logger.info("Streamed %d tweets (%d failed)", self.count, self.failed)
            if TWITTER_MAX_POSTS and self.count >= TWITTER_MAX_POSTS:
                self.disconnect()

        def on_errors(self, errors):
            logger.error("Tweepy stream errors: %s", errors)

        def on_exception(self, exception):
            logger.error("Tweepy stream exception: %s", exception)

    stream = Listener(TWITTER_BEARER_TOKEN, producer)
    try:
        stream.add_rules(tweepy.StreamRule(TWITTER_TRACK))
    except Exception as e:
        logger.error("Could not add stream rules: %s", e)

    logger.info(
        "Streaming live tweets matching '%s' (max=%d).",
        TWITTER_TRACK,
        TWITTER_MAX_POSTS,
    )
    try:
        stream.filter(tweet_fields=["created_at", "author_id"])
    except KeyboardInterrupt:
        logger.info("Tweet stream stopped by user.")
    finally:
        logger.info("Streamed %d tweets total.", stream.count)


def _flush(producer, batch):
    sent = 0
    failed = 0
    for topic, post in batch:
        try:
            producer.send(topic, post)
            sent += 1
        except Exception as e:
            failed += 1
            logger.error("Error sending post %s: %s", post.get("id"), e)
    producer.flush()
    return sent, failed


if __name__ == "__main__":
    main()