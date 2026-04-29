{{
    config(materialized='table')
}}

-- Per-site compliance feed.
--
-- Driven from gold_master_site so every Qualifyze site appears in the view
-- regardless of whether it currently has a matched EudraGMDP record.
-- Documents are filtered to accepted types via INNER JOIN on the seed.

select
    gms.qualifyze_site_id,
    gms.canonical_name,
    gms.country,
    scd.source,
    scd.source_document_id,
    scd.document_type,
    scd.inspection_date,
    scd.issue_date,
    scd.severity,
    scd.outcome
from {{ ref('gold_master_site') }} gms
left join {{ ref('silver_eudragmdp_sites') }} ses
    on ses.qualifyze_site_id = gms.qualifyze_site_id
left join {{ ref('silver_compliance_documents') }} scd
    on scd.oms_location_id = ses.oms_location_id
left join {{ ref('ref_accepted_document_types') }} r
    on scd.document_type = r.document_type
