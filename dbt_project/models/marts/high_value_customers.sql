select
    passenger_count,
    count(*) as total_trips,
    sum(total_amount) as total_spent,
    avg(pourcentage_pourboire) as avg_tip_percentage
from {{ ref('trip_enriched') }}
where passenger_count is not null
group by passenger_count
having count(*) > 10
   and sum(total_amount) > 300
   and avg(pourcentage_pourboire) > 15