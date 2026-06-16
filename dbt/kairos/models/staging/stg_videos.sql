with source as (
    select * from {{source('kairos_raw','videos')}}
)

,
staged as(
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
        ingested_at
    from source
    where video_id is not null
)

select * from staged 
