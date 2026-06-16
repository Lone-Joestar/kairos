with staged as (
    select * from {{ref('stg_videos') }}
),
metrics as (
        select 
            video_id,
            title,
            category_id,
            category_name,
            channel_title,
            view_count,
            like_count,
            comment_count,
            sentiment_score,
            region_code,
            published_at,
            ingested_at,
            -- engagement metrics
            round((like_count + comment_count)::numeric /nullif(view_count,0),
            6) as engagement_rate,
            round(like_count::numeric /nullif(view_count,0),6 
            ) as like_ratio,
            round((comment_count::numeric /nullif(view_count,0)),6)
            as comment_ratio,

            --time metrics
            extract(epoch from (ingested_at - published_at))/3600 as hours_since_published,
            extract(epoch from (now()- ingested_at)) / 3600 as hours_since_ingested
        from staged
)

select * from metrics
