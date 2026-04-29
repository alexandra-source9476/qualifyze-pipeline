-- Singular test: detect full-row duplicates inside the *current* batch only.
--
-- Bronze is append-only by design (audit trail). That means a duplicate
-- introduced in one batch lives there forever. A `unique` test on the table
-- as a whole would therefore stay red on every future run, even after the
-- upstream issue has been fixed.
--
-- This test scopes the check to var('batch_id'), so each ingestion is judged
-- on its own data quality and the failure self-resolves once the upstream
-- feed stops emitting duplicates.
--
-- The "natural key" used here is the set of fields required to be non-null
-- in bronze. Two rows are considered duplicates only if every one of these
-- columns matches.
--
-- Usage: invoked automatically by `dbt test`. Returns 0 rows on success
-- (dbt convention: a singular test passes when the SELECT returns nothing).

with current_batch as (
    select
        oms_location_id,
        eudragmdp_doc_ref,
        site_name,
        country,
        document_type,
        inspection_end_date,
        issue_date
    from {{ ref('bronze_eudragmdp') }}
    where ingestion_batch_id = '{{ var("batch_id", "manual-run") }}'
)

select
    oms_location_id,
    eudragmdp_doc_ref,
    site_name,
    country,
    document_type,
    inspection_end_date,
    issue_date,
    count(*) as duplicate_count
from current_batch
group by 1, 2, 3, 4, 5, 6, 7
having count(*) > 1
