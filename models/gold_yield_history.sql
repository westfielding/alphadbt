-- models/gold/gold_yield_history.sql
-- ---------------------------------------------------------------------------
-- Layer   : Gold
-- Table   : dividend_db.gold_yield_history
-- Purpose : Rolling yield metrics per symbol over time.
--           Useful for yield trend charts and income-return analysis
--           on EquityDeck single-stock pages.
-- ---------------------------------------------------------------------------

{{
    config(
        materialized     = 'table',
        file_format      = 'delta',
        on_schema_change = 'sync_all_columns',
        tags             = ['gold', 'dividend', 'yield']
    )
}}

with silver as (

    select *
    from {{ ref('silver_dividends') }}
    where dq_flag = false   -- exclude quality-flagged rows from analytics

),

windowed as (

    select
        symbol,
        ex_date,
        ex_year,
        payment_date,
        frequency,
        dividend_adj,
        yield_pct,

        -- ── Trailing-twelve-month yield (sum of dividends / latest price proxy) ─
        -- TTM = sum of dividend_adj for the 4 most recent quarters (or 12 months)
        sum(dividend_adj) over (
            partition by symbol
            order by ex_date
            rows between 3 preceding and current row
        )                                           as ttm_dividend_adj,

        -- ── Rolling average yield ─────────────────────────────────────────────
        round(avg(yield_pct) over (
            partition by symbol
            order by ex_date
            rows between 3 preceding and current row
        ), 6)                                       as yield_ma_4q,

        round(avg(yield_pct) over (
            partition by symbol
            order by ex_date
            rows between 7 preceding and current row
        ), 6)                                       as yield_ma_8q,

        -- ── Min / max yield over trailing 8 quarters (range context) ──────────
        round(min(yield_pct) over (
            partition by symbol
            order by ex_date
            rows between 7 preceding and current row
        ), 6)                                       as yield_min_8q,

        round(max(yield_pct) over (
            partition by symbol
            order by ex_date
            rows between 7 preceding and current row
        ), 6)                                       as yield_max_8q,

        -- ── Row count for guard on sparse series ──────────────────────────────
        count(*) over (
            partition by symbol
            order by ex_date
            rows between 7 preceding and current row
        )                                           as trailing_8q_count

    from silver

)

select
    symbol,
    ex_date,
    ex_year,
    payment_date,
    frequency,
    dividend_adj,
    yield_pct,
    ttm_dividend_adj,
    yield_ma_4q,
    -- Only surface 8q averages when enough history exists
    case when trailing_8q_count >= 4 then yield_ma_8q  else null end  as yield_ma_8q,
    case when trailing_8q_count >= 4 then yield_min_8q else null end  as yield_min_8q,
    case when trailing_8q_count >= 4 then yield_max_8q else null end  as yield_max_8q,
    current_timestamp()                                                as _gold_loaded_at

from windowed
order by symbol, ex_date
