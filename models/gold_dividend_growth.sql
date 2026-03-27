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

-- Spark SQL requires LAG() offset to be a constant foldable expression.
-- Pre-compute all needed offsets, then select the right one with CASE.
lagged as (

    select
        symbol,
        ex_date,
        ex_year,
        frequency,
        dividend_adj,

        -- QoQ
        lag(dividend_adj, 1)  over (partition by symbol order by ex_date) as lag_div_1,

        -- YoY candidates
        lag(dividend_adj, 2)  over (partition by symbol order by ex_date) as lag_div_2,
        lag(dividend_adj, 4)  over (partition by symbol order by ex_date) as lag_div_4,
        lag(dividend_adj, 12) over (partition by symbol order by ex_date) as lag_div_12,

        -- 5-year candidates (dividend)
        lag(dividend_adj, 5)  over (partition by symbol order by ex_date) as lag_div_5,
        lag(dividend_adj, 10) over (partition by symbol order by ex_date) as lag_div_10,
        lag(dividend_adj, 20) over (partition by symbol order by ex_date) as lag_div_20,
        lag(dividend_adj, 60) over (partition by symbol order by ex_date) as lag_div_60,

        -- 5-year candidates (ex_date)
        lag(ex_date, 5)  over (partition by symbol order by ex_date) as lag_date_5,
        lag(ex_date, 10) over (partition by symbol order by ex_date) as lag_date_10,
        lag(ex_date, 20) over (partition by symbol order by ex_date) as lag_date_20,
        lag(ex_date, 60) over (partition by symbol order by ex_date) as lag_date_60

    from silver

),

with_lag as (

    select
        symbol,
        ex_date,
        ex_year,
        frequency,
        dividend_adj,

        -- ── Prior period (QoQ) ────────────────────────────────────────────────
        lag_div_1                                   as prev_dividend_adj,

        -- ── Same period prior year (YoY) ─────────────────────────────────────
        case
            when frequency = 'Quarterly'   then lag_div_4
            when frequency = 'Monthly'     then lag_div_12
            when frequency = 'Semi-annual' then lag_div_2
            when frequency = 'Annual'      then lag_div_1
            else lag_div_4
        end                                         as yoy_prev_dividend_adj,

        -- ── 5-year prior for CAGR ─────────────────────────────────────────────
        case
            when frequency = 'Quarterly'   then lag_div_20
            when frequency = 'Monthly'     then lag_div_60
            when frequency = 'Semi-annual' then lag_div_10
            when frequency = 'Annual'      then lag_div_5
            else lag_div_20
        end                                         as five_yr_prev_dividend_adj,

        case
            when frequency = 'Quarterly'   then lag_date_20
            when frequency = 'Monthly'     then lag_date_60
            when frequency = 'Semi-annual' then lag_date_10
            when frequency = 'Annual'      then lag_date_5
            else lag_date_20
        end                                         as five_yr_prev_ex_date,

        -- ── Consecutive raises: running count of QoQ increases ────────────────
        sum(case
            when dividend_adj > lag_div_1 then 1 else 0
        end) over (
            partition by symbol
            order by ex_date
            rows between unbounded preceding and current row
        )                                           as cumulative_increases

    from lagged

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
