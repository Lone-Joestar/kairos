with snapshots as (
    select * from {{source('kairos_raw','video_snapshots')}}
),
velo_layer as
(select video_id,snapshotted_at,
view_count,
LAG(view_count) OVER(
    PARTITION BY video_id
    ORDER BY snapshotted_at
) as previous_view_count,
LAG(snapshotted_at) OVER (PARTITION BY video_id ORDER BY snapshotted_at) as previous_snapshotted_at
from snapshots),
signals as (
    select 
video_id,snapshotted_at,view_count,
previous_view_count,
view_count- previous_view_count as  views_gained,
extract(epoch from (snapshotted_at-previous_snapshotted_at)/3600) as hours_elapsed,
case    
    when previous_view_count is null then null
    else round(
        (view_count - previous_view_count )::numeric /
        nullif(extract(epoch from (snapshotted_at - previous_snapshotted_at))/3600,0),2
    )
    end as velocity 
from velo_layer
)

select * from signals
