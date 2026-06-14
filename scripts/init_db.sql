--Kairos databse initialization
-- Runs automatically on first Postgres boot

\connect kairos 
-- Schema for our data layers

CREATE SCHEMA IF NOT EXISTS kairos_raw;
CREATE SCHEMA IF NOT EXISTS kairos_staging;
CREATE SCHEMA IF NOT EXISTS kairos_marts;

-- Raw posts table (landing zone)

CREATE TABLE IF NOT EXISTS kairos_raw.videos(
    video_id VARCHAR(20) PRIMARY KEY,
    category_id VARCHAR(100) NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    title TEXT NOT NULL,
    channel_title VARCHAR(255),
    view_count BIGINT DEFAULT 0,
    like_count BIGINT DEFAULT 0,
    comment_count BIGINT DEFAULT 0,
    sentiment_score FLOAT,
    published_at TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT NOW(),
    region_code VARCHAR(5) DEFAULT 'US'
);

-- Index for common query patterns 

CREATE INDEX IF NOT EXISTS idx_posts_subreddit_created
    ON kairos_raw.videos (subreddit,created_utc DESC);

CREATE INDEX IF NOT EXISTS idx_posts_score
    ON kairos_raw.videos (score DESC);