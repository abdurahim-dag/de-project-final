create table if not exists public.transactions
(
    id        integer primary key generated always as identity,
    object_id uuid not null ,
    payload   jsonb not null
);
create table if not exists public.currencies
(
    id        integer primary key generated always as identity,
    object_id uuid not null ,
    payload   jsonb not null
);
CREATE TABLE if not exists public.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);