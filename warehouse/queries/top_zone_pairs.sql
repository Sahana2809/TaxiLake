-- Top 3 drop-off zones for each pickup zone, by trip count.
-- Demonstrates: classic "top N per group" with ROW_NUMBER().

WITH zone_pairs AS (
    SELECT
        pz.zone AS pickup_zone,
        dz.zone AS dropoff_zone,
        COUNT(*) AS trips
    FROM fact_trips f
    JOIN dim_zone pz ON f.pickup_zone_key = pz.zone_key
    JOIN dim_zone dz ON f.dropoff_zone_key = dz.zone_key
    GROUP BY pz.zone, dz.zone
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY pickup_zone ORDER BY trips DESC) AS rn
    FROM zone_pairs
)
SELECT pickup_zone, dropoff_zone, trips
FROM ranked
WHERE rn <= 3
ORDER BY pickup_zone, rn;
