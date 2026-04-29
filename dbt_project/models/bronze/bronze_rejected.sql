{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

-- Provider-agnostic rejected rows. Schema is fixed:
--     source              -- which upstream feed
--     ingestion_batch_id  -- which run
--     raw_row             -- JSON capture of the original row, full fidelity
--     rejection_reason    -- one or more flag tokens (space-separated)
--     rejected_at         -- when bronze rejected it
--
-- Each source contributes its own SELECT branch below. Adding a new provider
-- means adding a new from source('raw', 'raw_<name>') block that
-- builds the same JSON envelope.

with eudragmdp_batch as (
    select *
    from {{ source('raw', 'raw_eudragmdp') }}
    where ingestion_batch_id = '{{ var("batch_id") }}'
      and source             = '{{ var("source") }}'
),

eudragmdp_invalid as (
    select *
    from eudragmdp_batch
    where oms_location_id              is null
       or eudragmdp_doc_ref            is null
       or site_name                    is null
       or country                      is null
       or document_type                is null
       or inspection_end_date          is null
       or date(inspection_end_date)    is null
       or issue_date                   is null
       or date(issue_date)             is null
),

eudragmdp_rejected as (
    select
        source,
        ingestion_batch_id,
        json_object(
            'certificate_number',   certificate_number,
            'eudragmdp_doc_ref',    eudragmdp_doc_ref,
            'document_type',        document_type,
            'mia_number',           mia_number,
            'oms_organisation_id',  oms_organisation_id,
            'oms_location_id',      oms_location_id,
            'site_name',            site_name,
            'address_1',            address_1,
            'address_2',            address_2,
            'address_3',            address_3,
            'address_4',            address_4,
            'city',                 city,
            'postcode',             postcode,
            'country',              country,
            'duns_number',          duns_number,
            'site_nca_reference',   site_nca_reference,
            'inspection_end_date',  inspection_end_date,
            'issue_date',           issue_date,
            'last_updated_date',    last_updated_date
        ) as raw_row,
        trim(
            case when oms_location_id      is null then 'oms_location_id ' else '' end ||
            case when eudragmdp_doc_ref    is null then 'eudragmdp_doc_ref ' else '' end ||
            case when site_name            is null then 'site_name ' else '' end ||
            case when country              is null then 'country ' else '' end ||
            case when document_type        is null then 'document_type ' else '' end ||
            case when inspection_end_date is null
                    then 'inspection_end_date_null '
                 when date(inspection_end_date) is null
                    then 'inspection_end_date_invalid '
                 else '' end ||
            case when issue_date is null
                    then 'issue_date_null '
                 when date(issue_date) is null
                    then 'issue_date_invalid '
                 else '' end
        ) as rejection_reason,
        datetime('now') as rejected_at
    from eudragmdp_invalid
)

select * from eudragmdp_rejected
-- union all
-- select * from fda_rejected     -- future provider
-- union all
-- select * from mhra_rejected
