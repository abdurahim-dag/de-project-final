create table if not exists ST23051601__DWH.l_transaction_country(
    hk_l_transaction_country_pk bigint primary key,
    hk_transaction_id bigint not null CONSTRAINT fk_l_transaction_country_transaction REFERENCES ST23051601__DWH.h_transactions(hk_transaction_pk),
    hk_country_id bigint not null CONSTRAINT fk_l_transaction_country_country REFERENCES ST23051601__DWH.h_countries(hk_country_pk),
    load_dt datetime  not null,
    load_src varchar(20)  not null
)
order by load_dt
SEGMENTED BY hk_l_transaction_country_pk all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 1, 1);

create table if not exists ST23051601__DWH.l_transaction_account(
    hk_l_transaction_account_pk bigint primary key,
    hk_transaction_id bigint not null CONSTRAINT fk_l_transaction_country_transaction REFERENCES ST23051601__DWH.h_transactions(hk_transaction_pk),
    hk_account_from_id bigint not null CONSTRAINT fk_l_transaction_account_account_from REFERENCES ST23051601__DWH.h_accounts(hk_account_pk),
    hk_account_to_id bigint not null CONSTRAINT fk_l_transaction_account_account_to REFERENCES ST23051601__DWH.h_accounts(hk_account_pk),
    load_dt datetime  not null,
    load_src varchar(20)  not null
)
order by load_dt
SEGMENTED BY hk_l_transaction_account_pk all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 1, 1);

create table if not exists ST23051601__DWH.l_transaction_currency(
    hk_l_transaction_currency_pk bigint primary key,
    hk_transaction_id bigint not null CONSTRAINT fk_l_transaction_currency_transaction REFERENCES ST23051601__DWH.h_transactions(hk_transaction_pk),
    hk_currency_id bigint not null CONSTRAINT fk_l_transaction_currency_currency REFERENCES ST23051601__DWH.h_сurrencies(hk_currency_pk),
    load_dt datetime  not null,
    load_src varchar(20)  not null
)
order by load_dt
SEGMENTED BY hk_l_transaction_currency_pk all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 1, 1);
