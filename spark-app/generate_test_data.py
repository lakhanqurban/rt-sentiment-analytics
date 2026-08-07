"""Generate a test dataset of labeled JSON posts for offline sentiment validation.

Usage:
    python tests/generate_test_data.py --num 100000 --out test_posts.jsonl

Each line is a JSON object with fields: id, text, user, timestamp, like_count,
retweet_count, expected_label. `expected_label` encodes the ground-truth
sentiment (positive/neutral/negative) used by validate_sentiment.py.
"""
import argparse
import json
import random
from datetime import datetime, timedelta

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

LABELED = [("positive", POSITIVE), ("neutral", NEUTRAL), ("negative", NEGATIVE)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num", type=int, default=100000)
    parser.add_argument("--out", default="test_posts.jsonl")
    args = parser.parse_args()

    start = datetime(2024, 1, 1)
    with open(args.out, "w", encoding="utf-8") as f:
        for i in range(args.num):
            label, pool = random.choice(LABELED)
            ts = start + timedelta(seconds=random.randint(0, 86400 * 365))
            post = {
                "id": i,
                "text": random.choice(pool),
                "user": random.choice(USERS),
                "timestamp": ts.isoformat() + "Z",
                "like_count": random.randint(0, 5000),
                "retweet_count": random.randint(0, 1000),
                "expected_label": label,
            }
            f.write(json.dumps(post) + "\n")

    print(f"Wrote {args.num} labeled posts to {args.out}")


if __name__ == "__main__":
    main()