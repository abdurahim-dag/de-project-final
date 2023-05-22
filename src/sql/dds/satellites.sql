INSERT INTO ST23051601__DWH.s_currency_div(hk_h_currency, hk_h_currency_with, div, date_update, load_dt, load_src)
select
    hc.hk_currency_pk  as hk_h_currency,
    hcw.hk_currency_pk as hk_h_currency_with,
    c.currency_with_div,
    c.date_update,
    CAST('{{ ts }}' AS timestamp) as load_dt,
    'kafka-pg' as load_src
from ST23051601__STAGING.currencies c
         join ST23051601__DWH.h_сurrencies as hc on c.currency_code = hc.currency_code
         join ST23051601__DWH.h_сurrencies as hcw on c.currency_code_with = hcw.currency_code;

INSERT INTO ST23051601__DWH.s_transaction_status(hk_h_transaction, status, status_dt, load_dt, load_src)
select
    ht.hk_transaction_pk as hk_h_transaction,
    t.status as status,
    t.transaction_dt as status_dt,
    CAST('{{ ts }}' AS timestamp) as load_dt,
    'kafka-pg' as load_src
from ST23051601__STAGING.transactions t
         join ST23051601__DWH.h_transactions as ht on ht.operation_id = t.operation_id;