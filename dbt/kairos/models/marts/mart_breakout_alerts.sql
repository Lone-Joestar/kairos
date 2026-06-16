with sourced as (
    SELECT * FROM {{ref('mart_trending')}}
),

breakout as (
    SELECT * ,
    'HIGH VELOCITY BREAKOUT ' as alert_reason
    from sourced
    WHERE velocity >=10000 
    
)

SELECT * from breakout
ORDER BY velocity DESC