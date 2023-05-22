insert into ST23051601__DWH.dm_global_metrics(date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)

with cnt_transactions as (
    select distinct
        t.transaction_dt::date                                                               as date_update,
            t.currency_code                                                                      as currency_from,
        count(t.operation_id)
            over (PARTITION BY t.transaction_dt::date, t.currency_code)                        as cnt_transactions
    from ST23051601__STAGING.transactions t
    where t.status = 'done' and CAST('{{ ds }}' AS date) = t.transaction_dt::date
),
amount_transaction as (
    select t.transaction_dt::date                date_update,
            t.currency_code                       currency_from,
           sum(t.amount)                         amount_total,
           count(distinct t.account_number_from) cnt_accounts_make_transactions
    from ST23051601__STAGING.transactions t
    where t.status = 'done' and
          CAST('{{ ds }}' AS date) = t.transaction_dt::date and
          t.amount > 0
    group by t.transaction_dt::date, t.currency_code
),
curency as (
    select distinct
        t.currency_code                       as currency_code,
        first_value(c.currency_with_div) over (order by c.date_update desc) as currency_with_div
    from ST23051601__STAGING.transactions t
        join ST23051601__STAGING.currencies c on
            c.currency_code = t.currency_code and
            c.date_update::date <= t.transaction_dt::date and
            c.currency_code_with='420'
    where t.status = 'done'
)

select at.date_update,
       at.currency_from,
       at.amount_total * cur.currency_with_div as amount_total,
       cnt.cnt_transactions as cnt_transactions,
       cnt.cnt_transactions / at.cnt_accounts_make_transactions as avg_transactions_per_account,
       at.cnt_accounts_make_transactions as cnt_accounts_make_transactions
from amount_transaction at
    join cnt_transactions cnt on cnt.date_update = at.date_update and cnt.currency_from = at.currency_from
    join curency cur on cur.currency_code = at.currency_from;