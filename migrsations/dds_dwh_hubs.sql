create table if not exists ST23051601__DWH.h_transactions
(
    hk_transaction_pk bigint primary key,
    operation_id uuid  not null,
    amount INTEGER NOT NULL,
    transaction_type VARCHAR(300) NOT NULL,
    transaction_dt datetime(0)  not null,
    load_dt datetime  not null,
    load_src varchar(20)  not null
)
order by load_dt
SEGMENTED BY hk_transaction_pk all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 1, 1);

create table if not exists ST23051601__DWH.h_countries
(
    hk_country_pk bigint primary key,
    country_name varchar(200)  not null,
    load_dt datetime  not null,
    load_src varchar(20)  not null
)
order by load_dt
SEGMENTED BY hk_country_pk all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 1, 1);

create table if not exists ST23051601__DWH.h_accounts
(
    hk_account_pk bigint primary key,
    account_number integer  not null,
    load_dt datetime  not null,
    load_src varchar(20)  not null
)
order by load_dt
SEGMENTED BY hk_account_pk all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 1, 1);

create table if not exists ST23051601__DWH.h_сurrencies
(
    hk_currency_pk bigint primary key,
    currency_code varchar(3)  not null,
    load_dt datetime  not null,
    load_src varchar(20)  not null
)
order by load_dt
SEGMENTED BY hk_currency_pk all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 1, 1);