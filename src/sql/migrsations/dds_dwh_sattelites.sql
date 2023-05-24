create table if not exists ST23051601__DWH.s_transaction_status(
    hk_h_transaction bigint not null CONSTRAINT fk_s_transaction_status_transaction REFERENCES ST23051601__DWH.h_transactions(hk_transaction_pk),
    status VARCHAR(300) NOT NULL,
    status_dt timestamp(0) not null,
    load_dt datetime  not null,
    load_src varchar(20)  not null
)
order by status_dt
SEGMENTED BY hk_h_transaction all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists ST23051601__DWH.s_currency_div(
     hk_h_currency bigint not null CONSTRAINT fk_s_currency_div_currency REFERENCES ST23051601__DWH.h_сurrencies(hk_currency_pk),
     hk_h_currency_with bigint not null CONSTRAINT fk_s_currency_div_with_currency REFERENCES ST23051601__DWH.h_сurrencies(hk_currency_pk),
     div NUMERIC NOT NULL,
     date_update timestamp(0) not null,
     load_dt datetime  not null,
     load_src varchar(20)  not null
)
order by date_update
SEGMENTED BY hk_h_currency all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
