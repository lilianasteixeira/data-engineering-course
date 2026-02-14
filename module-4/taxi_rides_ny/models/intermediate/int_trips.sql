-- Enrich and deduplicate trip data
-- Demonstrates enrichment and surrogate key generation
-- Note: Data quality analysis available in analyses/trips_data_quality.sql

with unioned as (
    select * from {{ ref('int_trips_unioned') }}
),

payment_types as (
    select * from {{ ref('payment_type_lookup') }}
),

cleaned_and_enriched as (
    select
        -- Generate unique trip identifier (surrogate key pattern)
        {{ dbt_utils.generate_surrogate_key(['u.vendor_id', 'u.pickup_datetime', 'u.pickup_location_id', 'u.service_type']) }} as trip_id,

        -- Identifiers
        u.vendor_id,
        u.service_type,
        u.rate_code_id,

        -- Location IDs
        u.pickup_location_id,
        u.dropoff_location_id,

        -- Timestamps
        u.pickup_datetime,
        u.dropoff_datetime,

        -- Trip details
        u.store_and_fwd_flag,
        u.passenger_count,
        u.trip_distance,
        u.trip_type,

        -- Payment breakdown
        u.fare_amount,
        u.extra,
        u.mta_tax,
        u.tip_amount,
        u.tolls_amount,
        u.ehail_fee,
        u.improvement_surcharge,
        u.total_amount,

        -- Enrich with payment type description
        coalesce(u.payment_type, 0) as payment_type,
        coalesce(pt.description, 'Unknown') as payment_type_description

    from unioned u
    left join payment_types pt
        on coalesce(u.payment_type, 0) = pt.payment_type
)

-- Aggregate rows per dedupe key to produce a single deterministic row per trip
select
    md5(cast(coalesce(cast(vendor_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_datetime as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_location_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(service_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as trip_id,

    vendor_id,
    service_type,
    min(rate_code_id) as rate_code_id,

    pickup_location_id,
    min(dropoff_location_id) as dropoff_location_id,

    pickup_datetime,
    min(dropoff_datetime) as dropoff_datetime,

    min(store_and_fwd_flag) as store_and_fwd_flag,
    min(passenger_count) as passenger_count,
    min(trip_distance) as trip_distance,
    min(trip_type) as trip_type,

    min(fare_amount) as fare_amount,
    min(extra) as extra,
    min(mta_tax) as mta_tax,
    min(tip_amount) as tip_amount,
    min(tolls_amount) as tolls_amount,
    min(ehail_fee) as ehail_fee,
    min(improvement_surcharge) as improvement_surcharge,
    min(total_amount) as total_amount,

    min(coalesce(payment_type,0)) as payment_type,
    min(coalesce(payment_type_description,'Unknown')) as payment_type_description
from cleaned_and_enriched
group by vendor_id, service_type, pickup_location_id, pickup_datetime
