{{
    config(materialized='table')
}}

with ranked as (
    select
        *,
        row_number() over (
            partition by oms_location_id
            order by last_updated_date desc, ingested_at desc
        ) as rn
    from {{ ref('bronze_eudragmdp') }}
)

select
    oms_location_id,
    oms_organisation_id,
    site_name,
    address_1,
    address_2,
    address_3,
    address_4,
    city,
    postcode,
    country,
    duns_number,
    site_nca_reference,
    last_updated_date,
    cast(null as text) as qualifyze_site_id,
    'unmatched'        as match_confidence
from ranked
where rn = 1
