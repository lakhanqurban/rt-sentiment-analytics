"""Validate sentiment classification accuracy against a labeled test set.

Reads the JSONL produced by generate_test_data.py, applies the same TextBlob
sentiment logic used in the streaming pipeline, and reports accuracy.

Usage:
    python tests/validate_sentiment.py --input test_posts.jsonl --sample 5000
"""
import argparse
import json

from textblob import TextBlob


def classify(polarity):
    if polarity > 0.1:
        return "positive"
    if polarity < -0.1:
        return "negative"
    return "neutral"


def validate(path, sample):
    total = 0
    correct = 0
    wrong = []

    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if sample and i >= sample:
                break
            post = json.loads(line)
            label = classify(TextBlob(post["text"]).sentiment.polarity)
            expected = post["expected_label"]
            total += 1
            if label == expected:
                correct += 1
            else:
                wrong.append((post["text"], expected, label))

    accuracy = correct / total if total else 0.0
    print(f"Evaluated {total} posts, accuracy = {accuracy:.2%}")

    if wrong:
        print("\nSample misclassifications:")
        for text, exp, got in wrong[:10]:
            print(f"  expected={exp!r} got={got!r} | {text!r}")

    if accuracy < 0.70:
        print("WARNING: accuracy below 70% target.")
    else:
        print("PASS: accuracy >= 70% target.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="test_posts.jsonl")
    parser.add_argument("--sample", type=int, default=5000)
    args = parser.parse_args()
    validate(args.input, args.sample)


if __name__ == "__main__":
    main()