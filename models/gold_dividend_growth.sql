-- models/gold/gold_dividend_growth.sql
-- ---------------------------------------------------------------------------
-- Layer   : Gold
-- Table   : dividend_db.gold_dividend_growth
-- Purpose : Period-over-period dividend growth rates.
--           Drives compounder screening, aristocrat tracking, and
--           dividend growth comparison on EquityDeck.
-- ---------------------------------------------------------------------------

{{
    config(
        materialized     = 'table',
        file_format      = 'delta',
        on_schema_change = 'sync_all_columns',
        tags             = ['gold', 'dividend', 'growth']
    )
}}

with silver as (

    select *
    from {{ ref('silver_dividends') }}
    where dq_flag = false

),

with_lag as (

    select
        symbol,
        ex_date,
        ex_year,
        frequency,
        dividend_adj,

        -- ── Prior period (QoQ) ────────────────────────────────────────────────
        lag(dividend_adj, 1) over (
            partition by symbol
            order by ex_date
        )                                           as prev_dividend_adj,

        -- ── Same period prior year (YoY) — lag by 4 for quarterly, 1 for annual
        -- Use frequency_days to determine correct lag offset
        lag(dividend_adj, case
            when frequency = 'Quarterly'   then 4
            when frequency = 'Monthly'     then 12
            when frequency = 'Semi-annual' then 2
            when frequency = 'Annual'      then 1
            else 4
        end) over (
            partition by symbol
            order by ex_date
        )                                           as yoy_prev_dividend_adj,

        -- ── 5-year prior for CAGR (20 quarters back for quarterly payers) ─────
        lag(dividend_adj, case
            when frequency = 'Quarterly'   then 20
            when frequency = 'Monthly'     then 60
            when frequency = 'Semi-annual' then 10
            when frequency = 'Annual'      then 5
            else 20
        end) over (
            partition by symbol
            order by ex_date
        )                                           as five_yr_prev_dividend_adj,

        lag(ex_date, case
            when frequency = 'Quarterly'   then 20
            when frequency = 'Monthly'     then 60
            when frequency = 'Semi-annual' then 10
            when frequency = 'Annual'      then 5
            else 20
        end) over (
            partition by symbol
            order by ex_date
        )                                           as five_yr_prev_ex_date,

        -- ── Consecutive raises: running count of QoQ increases ────────────────
        sum(case
            when dividend_adj > lag(dividend_adj, 1) over (
                partition by symbol order by ex_date
            ) then 1 else 0
        end) over (
            partition by symbol
            order by ex_date
            rows between unbounded preceding and current row
        )                                           as cumulative_increases

    from silver

),

growth_rates as (

    select
        symbol,
        ex_date,
        ex_year,
        frequency,
        dividend_adj,
        prev_dividend_adj,
        yoy_prev_dividend_adj,
        five_yr_prev_dividend_adj,
        five_yr_prev_ex_date,
        cumulative_increases,

        -- ── QoQ growth % ─────────────────────────────────────────────────────
        case
            when prev_dividend_adj is not null
             and prev_dividend_adj > 0
            then round(
                (dividend_adj - prev_dividend_adj) / prev_dividend_adj * 100
            , 4)
        end                                         as qoq_growth_pct,

        -- ── YoY growth % ─────────────────────────────────────────────────────
        case
            when yoy_prev_dividend_adj is not null
             and yoy_prev_dividend_adj > 0
            then round(
                (dividend_adj - yoy_prev_dividend_adj) / yoy_prev_dividend_adj * 100
            , 4)
        end                                         as yoy_growth_pct,

        -- ── 5-year CAGR — only populated when 5 years of history exists ───────
        case
            when five_yr_prev_dividend_adj is not null
             and five_yr_prev_dividend_adj > 0
             and five_yr_prev_ex_date is not null
            then round(
                (
                    power(
                        cast(dividend_adj as double)
                        / cast(five_yr_prev_dividend_adj as double),
                        1.0 / (datediff(ex_date, five_yr_prev_ex_date) / 365.25)
                    ) - 1
                ) * 100
            , 4)
        end                                         as cagr_5y_pct

    from with_lag

)

select
    symbol,
    ex_date,
    ex_year,
    frequency,
    dividend_adj,
    prev_dividend_adj,
    yoy_prev_dividend_adj,
    five_yr_prev_dividend_adj,
    qoq_growth_pct,
    yoy_growth_pct,
    cagr_5y_pct,
    cumulative_increases,

    -- ── Convenience flag: has this symbol grown its dividend every year for 5y?
    case
        when cagr_5y_pct > 0 and cumulative_increases >= 20 then true
        else false
    end                                             as is_consistent_grower,

    current_timestamp()                             as _gold_loaded_at

from growth_rates
order by symbol, ex_date
