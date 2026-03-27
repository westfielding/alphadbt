-- models/gold/gold_payout_calendar.sql
-- ---------------------------------------------------------------------------
-- Layer   : Gold
-- Table   : dividend_db.gold_payout_calendar
-- Purpose : Forward-looking dividend payment calendar.
--           Projects the next expected payment date from frequency interval.
--           Confirmed = declaration_date populated; Projected = extrapolated.
--           Drives income calendar UI and portfolio cashflow forecasting.
-- ---------------------------------------------------------------------------

{{
    config(
        materialized     = 'table',
        file_format      = 'delta',
        on_schema_change = 'sync_all_columns',
        tags             = ['gold', 'dividend', 'calendar']
    )
}}

with silver as (

    select *
    from {{ ref('silver_dividends') }}
    where dq_flag = false

),

latest_per_symbol as (

    -- Most recent confirmed payment per symbol (for projecting next date)
    select
        symbol,
        ex_date         as latest_ex_date,
        payment_date    as latest_payment_date,
        dividend_adj    as latest_dividend_adj,
        frequency,
        frequency_days
    from (
        select
            *,
            row_number() over (
                partition by symbol
                order by ex_date desc
            ) as rn
        from silver
        where payment_date is not null
    )
    where rn = 1

),

projected as (

    select
        l.symbol,
        l.frequency,
        l.frequency_days,
        l.latest_ex_date,
        l.latest_payment_date,
        l.latest_dividend_adj,

        -- Project next ex-date and payment date by adding frequency interval
        date_add(l.latest_ex_date,     l.frequency_days) as next_expected_ex_date,
        date_add(l.latest_payment_date, l.frequency_days) as next_expected_payment_date,

        -- Days until projected next payment from today
        datediff(
            date_add(l.latest_payment_date, l.frequency_days),
            current_date()
        )                                                   as days_until_next_payment

    from latest_per_symbol l

),

all_payments as (

    -- Historical confirmed payments
    select
        s.symbol,
        s.ex_date,
        s.payment_date,
        s.declaration_date,
        s.dividend_adj          as expected_amount,
        s.frequency,
        s.days_to_payment,
        true                    as is_confirmed,
        false                   as is_projected,
        null                    as days_until_payment

    from silver s

    union all

    -- Projected next payment (one row per symbol)
    select
        p.symbol,
        p.next_expected_ex_date         as ex_date,
        p.next_expected_payment_date    as payment_date,
        null                            as declaration_date,
        p.latest_dividend_adj           as expected_amount,  -- assumes flat dividend
        p.frequency,
        p.frequency_days                as days_to_payment,
        false                           as is_confirmed,
        true                            as is_projected,
        p.days_until_next_payment       as days_until_payment

    from projected p
    -- Only include projection if next expected date is in the future
    where p.days_until_next_payment >= 0

)

select
    symbol,
    ex_date,
    payment_date,
    declaration_date,
    expected_amount,
    frequency,
    days_to_payment,
    is_confirmed,
    is_projected,
    days_until_payment,
    current_timestamp()     as _gold_loaded_at

from all_payments
order by symbol, ex_date
