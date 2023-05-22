create table if not exists ST23051601__DWH.dm_global_metrics
(
    global_metrics_pk IDENTITY(1,1),
    date_update date not null,
    currency_from varchar(3),
    amount_total integer not null,
    cnt_transactions integer not null,
    avg_transactions_per_account numeric not null,
    cnt_accounts_make_transactions integer not null
)
order by date_update
SEGMENTED BY global_metrics_pk all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);
