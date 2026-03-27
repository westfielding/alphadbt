-- models/silver/silver_dividends.sql
-- ---------------------------------------------------------------------------
-- Layer   : Silver
-- Table   : dividend_db.silver_dividends
-- Strategy: Incremental / merge on (symbol, ex_date)
-- Purpose : Cast all types, rename to snake_case, derive enrichment columns,
--           flag data quality issues. One canonical row per (symbol, ex_date).
-- ---------------------------------------------------------------------------

{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'merge',
        unique_key           = ['symbol', 'ex_date'],
        file_format          = 'delta',
        partition_by         = ['ex_year'],
        on_schema_change     = 'sync_all_columns',
        tags                 = ['silver', 'dividend']
    )
}}

with bronze as (

    select *
    from {{ ref('bronze_dividends') }}

    {% if is_incremental() %}
        -- Re-process anything ingested after the latest silver record
        where _ingested_at > (
            select coalesce(max(_ingested_at), '1900-01-01')
            from {{ this }}
        )
    {% endif %}

),

cast_and_rename as (

    select
        -- ── Identifiers ──────────────────────────────────────────────────────
        upper(trim(symbol))                         as symbol,

        -- ── Dates (cast from STRING) ─────────────────────────────────────────
        to_date(date,            'yyyy-MM-dd')      as ex_date,
        to_date(recordDate,      'yyyy-MM-dd')      as record_date,
        to_date(paymentDate,     'yyyy-MM-dd')      as payment_date,
        to_date(declarationDate, 'yyyy-MM-dd')      as declaration_date,

        -- ── Financial figures (cast to DECIMAL) ───────────────────────────────
        cast(adjDividend as decimal(12, 6))         as dividend_adj,
        cast(dividend    as decimal(12, 6))         as dividend_raw,
        -- Yield stored as a ratio in the API (0.004295…); convert to pct
        round(cast(yield as decimal(18, 10)) * 100, 6) as yield_pct,

        -- ── Frequency (normalise casing) ─────────────────────────────────────
        initcap(trim(frequency))                    as frequency,

        -- ── Pipeline provenance ──────────────────────────────────────────────
        _ingested_at,
        _source,
        _batch_id

    from bronze

),

enrich as (

    select
        *,

        -- ── Partition helper ─────────────────────────────────────────────────
        year(ex_date)                               as ex_year,

        -- ── Derived timing metrics ───────────────────────────────────────────
        datediff(payment_date,     ex_date)         as days_to_payment,
        datediff(ex_date, declaration_date)         as declaration_to_ex_lag,
        datediff(record_date,      ex_date)         as ex_to_record_lag,

        -- ── Frequency → expected days between payments ───────────────────────
        case frequency
            when 'Quarterly'    then 91
            when 'Monthly'      then 30
            when 'Annual'       then 365
            when 'Semi-annual'  then 182
            else null
        end                                         as frequency_days,

        -- ── Data quality flag ────────────────────────────────────────────────
        -- True = something looks off; surface to consumers to filter or review
        case
            when dividend_adj != dividend_raw       then true  -- corp action adj
            when yield_pct = 0                      then true  -- zero yield anomaly
            when dividend_adj <= 0                  then true  -- non-positive div
            when payment_date < ex_date             then true  -- date ordering issue
            else false
        end                                         as dq_flag,

        -- ── Audit ────────────────────────────────────────────────────────────
        current_timestamp()                         as _silver_loaded_at

    from cast_and_rename

),

deduplicated as (

    -- Keep the most recently ingested record per (symbol, ex_date)
    select *
    from (
        select
            *,
            row_number() over (
                partition by symbol, ex_date
                order by _ingested_at desc
            ) as _rn
        from enrich
    )
    where _rn = 1

)

select
    symbol,
    ex_date,
    record_date,
    payment_date,
    declaration_date,
    ex_year,
    dividend_adj,
    dividend_raw,
    yield_pct,
    frequency,
    frequency_days,
    days_to_payment,
    declaration_to_ex_lag,
    ex_to_record_lag,
    dq_flag,
    _ingested_at,
    _source,
    _batch_id,
    _silver_loaded_at

from deduplicated
