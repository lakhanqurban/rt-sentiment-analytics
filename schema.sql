-- Schema for the streaming sentiment pipeline.
-- Applied automatically on first Postgres container startup.

DROP TABLE IF EXISTS posts;

CREATE TABLE posts (
    id            BIGINT PRIMARY KEY,
    text          TEXT NOT NULL,
    clean_text    TEXT,
    tokens        TEXT[],
    username      VARCHAR(64),
    timestamp     TIMESTAMPTZ,
    like_count    BIGINT,
    retweet_count BIGINT,
    polarity      DOUBLE PRECISION,
    subjectivity  DOUBLE PRECISION,
    label         VARCHAR(16),
    ingested_at   TIMESTAMPTZ DEFAULT now()
);

-- Index for time-windowed analytics queries.
CREATE INDEX idx_posts_timestamp ON posts (timestamp);
CREATE INDEX idx_posts_label ON posts (label);

-- A simple aggregate view for dashboard reporting.
CREATE VIEW post_summary AS
SELECT
    label,
    COUNT(*) AS total,
    ROUND(AVG(polarity)::numeric, 4) AS avg_polarity
FROM posts
GROUP BY label;