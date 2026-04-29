{{
    config(materialized='table')
}}

with ranked as (
    select
        *,
        row_number() over (
            partition by eudragmdp_doc_ref
            order by last_updated_date desc, ingested_at desc
        ) as rn
    from {{ ref('bronze_eudragmdp') }}
)

select
    source,
    eudragmdp_doc_ref          as source_document_id,
    document_type,
    oms_location_id,
    site_name,
    country,
    inspection_end_date        as inspection_date,
    issue_date,
    last_updated_date,
    cast(null as text)         as severity,
    cast(null as text)         as outcome
from ranked
where rn = 1
