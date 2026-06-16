with mart_layer as (select 
    stg_videos.title,
    stg_videos.view_count,
    stg_videos.sentiment_score,
    stg_videos.category_id,
    stg_videos.category_name,
    stg_videos.channel_title,
    int_video_signals.views_gained,
    int_video_signals.velocity 

    from {{ref('stg_videos')}} as stg_videos
    inner join {{ref('int_video_signals')}} as int_video_signals
    on stg_videos.video_id=int_video_signals.video_id)

    select * from mart_layer
    WHERE velocity is not null
    ORDER by velocity DESC





