{{
    config(
        materialized='incremental',
        unique_key='source_document_id',
        incremental_strategy='append'
    )
}}

with new_ncrs as (
    select
        source_document_id,
        document_type,
        oms_location_id,
        site_name,
        country,
        inspection_date,
        issue_date,
        last_updated_date
    from {{ ref('silver_compliance_documents') }}
    where document_type = 'GMPNC'
)

select
    n.source_document_id,
    n.document_type,
    n.oms_location_id,
    n.site_name,
    n.country,
    n.inspection_date,
    n.issue_date,
    n.last_updated_date,
    datetime('now') as detected_at,
    'pending'       as notification_status,
    cast(null as text) as notified_at
from new_ncrs n

{% if is_incremental() %}
where n.source_document_id not in (
    select source_document_id from {{ this }}
)
{% endif %}
