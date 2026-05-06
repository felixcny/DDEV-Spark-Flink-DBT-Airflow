with weather_by_hour as (
    select
        observation_hour,
        avg(temperature) as temperature,
        avg(humidity) as humidity,
        avg(wind_speed) as wind_speed,
        max(condition) as condition,
        max(weather_category) as weather_category
    from {{ source('raw', 'weather') }}
    group by observation_hour
)

select
    t.*,
    w.temperature,
    w.humidity,
    w.wind_speed,
    w.condition,
    w.weather_category
from {{ source('raw', 'fact_taxi_trips') }} t
left join weather_by_hour w
    on t.heure_prisencharge = w.observation_hour
