"""Download the NLTK corpora needed by TextBlob into an explicit directory.

The Bitnami Spark image runs as a non-root user, and `textblob.download_corpora`
targets the unwritable `/nltk_data`. This script writes to `NLTK_DATA` (default
`/app/nltk_data`), which is already first in `nltk.data.path` at runtime, so
both the driver and executors can find the corpora.
"""
import logging
import os

import nltk

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CORPORA = [
    "brown",
    "punkt",
    "wordnet",
    "averaged_perceptron_tagger",
    "movie_reviews",
]


def main():
    download_dir = os.getenv("NLTK_DATA", "/app/nltk_data")
    os.makedirs(download_dir, exist_ok=True)
    for corpus in CORPORA:
        logger.info("Downloading %s to %s ...", corpus, download_dir)
        nltk.download(corpus, download_dir=download_dir)
    logger.info("All corpora downloaded to %s.", download_dir)


if __name__ == "__main__":
    main()