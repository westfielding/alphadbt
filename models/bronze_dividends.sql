-- models/bronze/bronze_dividends.sql
-- ---------------------------------------------------------------------------
-- Layer   : Bronze
-- Table   : dividend_db.bronze_dividends
-- Strategy: Incremental / append-only Delta
-- Purpose : Preserve every API response row exactly as received.
--           No type casting. Pipeline metadata columns are forwarded.
-- ---------------------------------------------------------------------------

{{
    config(
        materialized        = 'incremental',
        incremental_strategy = 'append',
        unique_key          = ['symbol', 'date', '_batch_id'],
        file_format         = 'delta',
        partition_by        = ['_ingested_date'],
        on_schema_change    = 'sync_all_columns',
        tags                = ['bronze', 'dividend']
    )
}}

select
    -- ── Identifiers ──────────────────────────────────────────────────────────
    symbol,

    -- ── Date fields (all raw STRING — cast deferred to Silver) ───────────────
    date,
    record_date,
    payment_date,
    declaration_date,

    -- ── Financial fields (raw STRING) ────────────────────────────────────────
    adj_dividend,
    dividend,
    yield,
    frequency,

    -- ── Pipeline metadata ────────────────────────────────────────────────────
    _ingested_at,
    cast(_ingested_at as date)      as _ingested_date,   -- partition column
    _source,
    _batch_id

from {{ source('raw_dividend_api', 'raw_dividends') }}

{% if is_incremental() %}
    -- Only process rows from batches not yet landed in this table
    where _batch_id not in (
        select distinct _batch_id
        from {{ this }}
    )
{% endif %}
