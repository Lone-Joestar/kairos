import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Default args applied to every task in every DAG
DEFAULT_ARGS = {
    "owner": "kairos",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),

    "retry_exponential_backoff": True,
    "email_on_failure": False,
}



def load_sources() -> dict:
    """Load category config from sources.yml"""
    config_path = Path("/opt/airflow/config/sources.yml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def build_dag(category: dict) -> DAG:
    """
    Build one DAG for one YouTube category.
    Called once per category in sources.yml.
    """

    category_id = category["id"]
    category_name = category["name"]
    dag_id = f"kairos_youtube_{category_name}"

    dag = DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        description=f"Kairos pipeline for {category['label']}",
        schedule=timedelta(hours=1),
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["kairos", "youtube", category_name],
    )

    with dag:

        @task(task_id="extract")
        def extract(category_id: str, category_name: str) -> dict:
            """Fetch trending videos from YouTube API."""
            import os
            import sys
            sys.path.insert(0,'/opt/airflow/plugins')
            from hooks.youtube_hook import YouTubeHook

            api_key = os.environ["YOUTUBE_API_KEY"]
            hook = YouTubeHook(api_key=api_key)

            videos = hook.get_trending_videos(
                category_id=category_id,
                region_code="US",
                max_results=50
            )

            hook.close()

            return {
                "category_id": category_id,
                "category_name": category_name,
                "videos": videos,
                "count": len(videos)
            }

        @task(task_id="upload_s3")
        def upload_s3(extracted: dict) -> str:
            """Upload raw JSON to S3."""
            import boto3
            import uuid
            from datetime import datetime, timezone

            s3 = boto3.client(
                "s3",
                endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
            )

            now = datetime.now(timezone.utc)
            batch_id = str(uuid.uuid4())[:8]
            category_name = extracted["category_name"]

            key = (
                f"youtube/{category_name}/"
                f"{now.year}/{now.month:02d}/{now.day:02d}/"
                f"{batch_id}.json"
            )

            payload = json.dumps(extracted, default=str)

            s3.put_object(
                Bucket=os.environ["S3_BUCKET"],
                Key=key,
                Body=payload,
                ContentType="application/json"
            )

            print(f"Uploaded {len(extracted['videos'])} videos to s3://{os.environ['S3_BUCKET']}/{key}")
            return f"s3://{os.environ['S3_BUCKET']}/{key}"

        @task(task_id="load_postgres")
        def load_postgres(extracted: dict) -> int:
            """Parse videos and upsert to kairos_raw.videos."""
            import psycopg2
            from datetime import datetime, timezone

            conn = psycopg2.connect(
                host=os.environ["POSTGRES_HOST"],
                port=os.environ["POSTGRES_PORT"],
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"]
            )

            upsert_sql = """
                INSERT INTO kairos_raw.videos (
                    video_id, category_id, category_name, title,
                    channel_title, view_count, like_count, comment_count,
                    published_at, ingested_at, region_code
                ) VALUES (
                    %(video_id)s, %(category_id)s, %(category_name)s, %(title)s,
                    %(channel_title)s, %(view_count)s, %(like_count)s, %(comment_count)s,
                    %(published_at)s, NOW(), %(region_code)s
                )
                ON CONFLICT (video_id) DO UPDATE SET
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    comment_count = EXCLUDED.comment_count,
                    ingested_at = NOW();
            """
               
            snapshot_sql=   """INSERT INTO kairos_raw.video_snapshots (
                        video_id, category_id, category_name, title,
                        channel_title, view_count, like_count, comment_count,
                        published_at, snapshotted_at, region_code
                    ) VALUES (
                        %(video_id)s, %(category_id)s, %(category_name)s, %(title)s,
                        %(channel_title)s, %(view_count)s, %(like_count)s, %(comment_count)s,
                        %(published_at)s, NOW(), %(region_code)s
                    ) 
                    """

            videos = extracted["videos"]
            inserted = 0

            with conn:
                with conn.cursor() as cur:
                    for video in videos:
                        cur.execute(upsert_sql, video)
                        cur.execute(snapshot_sql,video)
                        inserted += 1

            conn.close()
            print(f"Upserted {inserted} videos to kairos_raw.videos")
            return inserted
        


        @task(task_id="score_sentiment")
        def score_sentiment(extracted: dict) -> int:
            """Score video titles with VADER sentiment."""
            import psycopg2
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

            analyzer = SentimentIntensityAnalyzer()
            conn = psycopg2.connect(
                host=os.environ["POSTGRES_HOST"],
                port=os.environ["POSTGRES_PORT"],
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"]
            )

            videos = extracted["videos"]
            scored = 0

            with conn:
                with conn.cursor() as cur:
                    for video in videos:
                        compound = analyzer.polarity_scores(
                            video["title"]
                        )["compound"]

                        cur.execute(
                            """
                            UPDATE kairos_raw.videos
                            SET sentiment_score = %s
                            WHERE video_id = %s
                            """,
                            (compound, video["video_id"])
                        )
                        scored += 1

            conn.close()
            print(f"Scored sentiment for {scored} videos")
            return scored

        def _branch_on_count(**context) -> str:
            """Branch: skip downstream tasks if no videos fetched."""
            ti = context["ti"]
            extracted = ti.xcom_pull(task_ids="extract")
            if extracted["count"] > 0:
                return "load_postgres"
            return "skip_notify"

        branch = BranchPythonOperator(
            task_id="branch_on_count",
            python_callable=_branch_on_count,
            provide_context=True,
        )

        @task(task_id="skip_notify")
        def skip_notify():
            print("No videos fetched. Skipping pipeline.")

        @task(
            task_id="notify_success",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )
        def notify_success(count: int):
            print(f"Pipeline complete. Processed {count} videos.")

        # Wire the tsks togerether
        extracted = extract(category_id, category_name)
        s3_path = upload_s3(extracted)
        pg_count = load_postgres(extracted)
        sentiment_count = score_sentiment(extracted)

        extracted >> branch
        branch >> [pg_count, skip_notify()]
        pg_count >> sentiment_count >> notify_success(pg_count)

    return dag