INSERT INTO ST23051601__DWH.l_transaction_account(hk_l_transaction_account_pk, hk_transaction_id, hk_account_from_id, hk_account_to_id, load_dt, load_src)
select distinct
    hash(h.hk_transaction_pk) as hk_l_transaction_account_pk,
    h.hk_transaction_pk as hk_transaction_id,
    af.hk_account_pk as hk_account_from_id,
    at.hk_account_pk as hk_account_to_id,
    CAST('{{ ts }}' AS timestamp) as load_dt,
    'kafka-pg' as load_src
from ST23051601__STAGING.transactions t
         join ST23051601__DWH.h_transactions as h on t.operation_id = h.operation_id
         join ST23051601__DWH.h_accounts as af on t.account_number_from = af.account_number
         join ST23051601__DWH.h_accounts as at on t.account_number_to = at.account_number
where hash(h.hk_transaction_pk) not in (select hk_l_transaction_account_pk from ST23051601__DWH.l_transaction_account);


INSERT INTO ST23051601__DWH.l_transaction_country(hk_l_transaction_country_pk, hk_transaction_id, hk_country_id, load_dt, load_src)
select distinct
    hash(h.hk_transaction_pk) as hk_l_transaction_country_pk,
    h.hk_transaction_pk as hk_transaction_id,
    c.hk_country_pk as hk_country_id,
    CAST('{{ ts }}' AS timestamp) as load_dt,
    'kafka-pg' as load_src
from ST23051601__STAGING.transactions t
         join ST23051601__DWH.h_transactions as h on t.operation_id = h.operation_id
         join ST23051601__DWH.h_countries as c on t.country = c.country_name
where hash(h.hk_transaction_pk) not in (select hk_l_transaction_country_pk from ST23051601__DWH.l_transaction_country);

INSERT INTO ST23051601__DWH.l_transaction_currency(hk_l_transaction_currency_pk, hk_transaction_id, hk_currency_id, load_dt, load_src)
select distinct
    hash(h.hk_transaction_pk) as hk_l_transaction_country_pk,
    h.hk_transaction_pk as hk_transaction_id,
    c.hk_currency_pk as hk_currency_id,
    CAST('{{ ts }}' AS timestamp) as load_dt,
    'kafka-pg' as load_src
from ST23051601__STAGING.transactions t
         join ST23051601__DWH.h_transactions as h on t.operation_id = h.operation_id
         join ST23051601__DWH.h_сurrencies as c on t.currency_code = c.currency_code
where hash(h.hk_transaction_pk) not in (select hk_l_transaction_currency_pk from ST23051601__DWH.l_transaction_currency);
