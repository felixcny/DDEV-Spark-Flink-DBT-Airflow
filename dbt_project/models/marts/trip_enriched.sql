select
    t.*,
    w.temperature,
    w.humidity,
    w.wind_speed,
    w.condition,
    w.weather_category
from {{ source('raw', 'fact_taxi_trips') }} t
left join {{ source('raw', 'weather') }} w
    on date_trunc('hour', t.tpep_pickup_datetime) = date_trunc('hour', w.timestamp)