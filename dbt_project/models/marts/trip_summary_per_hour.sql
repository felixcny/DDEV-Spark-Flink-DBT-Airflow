select
    date_trunc('hour', tpep_pickup_datetime) as pickup_hour,
    weather_category,
    count(*) as total_trips,
    avg(duree_trajet) as avg_trip_duration,
    avg(pourcentage_pourboire) as avg_tip_percentage
from {{ ref('trip_enriched') }}
group by 1, 2