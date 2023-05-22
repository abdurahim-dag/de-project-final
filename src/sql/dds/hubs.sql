INSERT INTO ST23051601__DWH.h_accounts(hk_account_pk, account_number, load_dt, load_src)
select distinct
    hash(account_number_from) as hk_account_pk,
    account_number_from as account_number,
    CAST('{{ ts }}' AS timestamp) as load_dt,
    'kafka-pg' as load_src
from ST23051601__STAGING.transactions
where hash(account_number_from) not in (select hk_account_pk from ST23051601__DWH.h_accounts);

INSERT INTO ST23051601__DWH.h_accounts(hk_account_pk, account_number, load_dt, load_src)
select distinct
    hash(account_number_to) as hk_account_pk,
    account_number_to as account_number,
    CAST('{{ ts }}' AS timestamp) as load_dt,
    'kafka-pg' as load_src
from ST23051601__STAGING.transactions
where hash(account_number_to) not in (select hk_account_pk from ST23051601__DWH.h_accounts);

INSERT INTO ST23051601__DWH.h_countries(hk_country_pk, country_name, load_dt, load_src)
select distinct
    hash(country) as hk_country_pk,
    country as country_name,
    CAST('{{ ts }}' AS timestamp) as load_dt,
    'kafka-pg' as load_src
from ST23051601__STAGING.transactions
where hash(country) not in (select hk_country_pk from ST23051601__DWH.h_countries);

INSERT INTO ST23051601__DWH.h_transactions(hk_transaction_pk, operation_id, amount, transaction_type, transaction_dt, load_dt, load_src)
select distinct
    hash(operation_id) as hk_transaction_pk,
    operation_id,
    amount,
    transaction_type,
    transaction_dt,
    CAST('{{ ts }}' AS timestamp) as load_dt,
    'kafka-pg' as load_src
from ST23051601__STAGING.transactions
where hash(operation_id) not in (select hk_transaction_pk from ST23051601__DWH.h_transactions);

INSERT INTO ST23051601__DWH.h_сurrencies(hk_currency_pk, currency_code, load_dt, load_src)
select distinct
    hash(currency_code) as hk_currency_pk,
    currency_code,
    CAST('{{ ts }}' AS timestamp) as load_dt,
    'kafka-pg' as load_src
from ST23051601__STAGING.currencies
where hash(currency_code) not in (select hk_currency_pk from ST23051601__DWH.h_сurrencies);
