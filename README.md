# Kairos

Detects YouTube videos that are accelerating in views before they peak — using velocity scoring computed entirely in SQL.

The question this project answers is not "which videos are popular right now." That's just a leaderboard. The question is: **which videos are moving the fastest, and is that movement accelerating?**

---

## What it does

Kairos ingests trending YouTube videos every hour across 5 categories, stores hourly snapshots of each video's view count, and computes a velocity score — views gained per hour between snapshots — using a `LAG()` window function in dbt. Videos that cross a velocity threshold are flagged as breakout candidates and served through a FastAPI layer.

```
YouTube Data API v3
        ↓
Airflow DAG factory (one DAG per category, hourly)
        ↓
Postgres (raw videos + hourly snapshots)
        ↓
dbt (staging → metrics → velocity scoring → marts)
        ↓
FastAPI (/v1/trending, /v1/breakouts, /v1/categories/{id})
```

---

## Stack

- **Orchestration** — Airflow 2.9 with a config-driven DAG factory (one YAML file spins up all 5 category pipelines)
- **Storage** — Postgres 15 for structured data, LocalStack S3 for raw JSON landing
- **Transformation** — dbt with 5 models across staging, intermediate, and mart layers
- **Sentiment** — VADER scoring on video titles at ingestion time
- **API** — FastAPI with SQLAlchemy and Pydantic response models
- **Infrastructure** — Docker Compose, multi-stage Dockerfiles, non-root containers

---

## API

```bash
# Top trending videos by velocity
GET /v1/trending?limit=10

# Active breakout alerts (velocity > 10k views/hour)
GET /v1/breakouts?limit=10

# Filter by YouTube category ID
GET /v1/categories/28?limit=10

# Health
GET /health
```

---

## dbt models

```
stg_videos              — clean, typed, filtered from raw ingestion
int_video_metrics       — engagement rate, like ratio, hours since published
int_video_signals       — LAG()-based velocity scoring on hourly snapshots
mart_trending           — top videos ranked by velocity, joined with metadata
mart_breakout_alerts    — videos exceeding the 10k views/hour threshold
```

8 dbt tests across all models.

---

## The velocity scoring model

The core of Kairos is `int_video_signals`. For each video, it compares the current snapshot's view count against the previous snapshot using `LAG()`, then divides views gained by hours elapsed to get views per hour.

```sql
LAG(view_count) OVER (
    PARTITION BY video_id
    ORDER BY snapshotted_at
) as previous_view_count
```

A video gaining 50,000 views per hour is more interesting than one sitting at 5 million total views with no momentum.

---

## Limitations

**No distinction between Shorts and standard videos.** YouTube's API returns both in the same trending endpoint. A Short with 2 million views is a fundamentally different signal than a 20-minute video with 2 million views, but the pipeline treats them identically. The fix is a `video_type` column classified by `contentDetails.duration` — deferred for a future iteration.

**Velocity scoring requires multiple ingestion cycles.** The first snapshot of any video has no previous value to compare against, so `velocity` is null until at least two snapshots exist. In production with hourly runs this resolves naturally within an hour, but cold starts show no velocity data.

**Breakout threshold is static.** The 10,000 views/hour threshold is a fixed number, not relative to category norms. A niche education video gaining 5k views/hour might be a significant breakout for that category while being invisible in this model. A dynamic threshold using rolling standard deviation per category would be more accurate.

**S3 is LocalStack in development.** Swapping to real AWS requires removing `AWS_ENDPOINT_URL` from the environment. No code changes needed.

**No deduplication on sentiment scoring.** If a video title changes between ingestions, the sentiment score reflects the most recent title only.

---

## Running locally

```bash
git clone https://github.com/Lone-Joestar/kairos
cd kairos

cp .env.example .env
# Add your YOUTUBE_API_KEY

docker compose up --build
```

Airflow UI at `http://localhost:8081` (admin/admin).  
API at `http://localhost:8000`.  
dbt models: `cd dbt/kairos && dbt run`

---

