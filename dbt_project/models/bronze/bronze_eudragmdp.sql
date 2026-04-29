{{
    config(
        materialized='incremental',
        unique_key='eudragmdp_doc_ref',
        incremental_strategy='append'
    )
}}

with batch as (
    select *
    from {{ source('raw', 'raw_eudragmdp') }}
    where ingestion_batch_id = '{{ var("batch_id") }}'
      and source             = '{{ var("source") }}'
),

valid_rows as (
    select *
    from batch
    where oms_location_id              is not null
      and eudragmdp_doc_ref            is not null
      and site_name                    is not null
      and country                      is not null
      and document_type                is not null
      and inspection_end_date          is not null
      and date(inspection_end_date)    is not null   -- rejects 'not-a-date' etc.
      and issue_date                   is not null
      and date(issue_date)             is not null
)

select
    certificate_number,
    eudragmdp_doc_ref,
    document_type,
    mia_number,
    oms_organisation_id,
    oms_location_id,
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
    date(inspection_end_date) as inspection_end_date,
    date(issue_date)          as issue_date,
    date(last_updated_date)   as last_updated_date,
    source,
    ingestion_batch_id,
    datetime('now')           as ingested_at
from valid_rows
