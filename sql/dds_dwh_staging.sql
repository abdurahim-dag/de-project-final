create table if not exists ST23051601__STAGING.transactions (
    id identity primary key ENABLED,
    operation_id uuid not null,
    account_number_from INTEGER NOT NULL,
    account_number_to INTEGER NOT NULL,
    currency_code INTEGER NOT NULL,
    country VARCHAR(300) NOT NULL,
    status VARCHAR(300) NOT NULL,
    transaction_type VARCHAR(300) NOT NULL,
    amount INTEGER NOT NULL,
    transaction_dt TIMESTAMP(0) NOT NULL
)
-- В стейдж слое будет поиск по id и дате
order by id, transaction_dt
-- сегментируем записи по нодам
segmented by hash(id) all nodes
-- Партицируем данные по дате
partition by transaction_dt::date
-- объеденим партиции в активные группы по 1 месяцу и 1 году
GROUP BY calendar_hierarchy_day(transaction_dt::date, 1, 1);

create table if not exists ST23051601__STAGING.currencies(
     id identity primary key ENABLED,
     currency_code_with VARCHAR(3) NOT NULL,
     currency_code VARCHAR(3) NOT NULL,
     currency_code_div NUMERIC NOT NULL,
     date_update TIMESTAMP(0) NOT NULL
)
-- В стейдж слое будет поиск по id и дате
order by id, date_update
-- сегментируем записи по нодам
segmented by hash(id) all nodes
-- Партицируем данные по дате
partition by date_update::date
-- объеденим партиции в активные группы по 1 месяцу и 1 году
GROUP BY calendar_hierarchy_day(date_update::date, 1, 1);