create table messages
(
    id integer generated always as identity primary key,
    object_id   uuid         not null,
    object_type varchar(200) not null,
    sent_dttm   timestamp    not null,
    payload     jsonb        not null
);

CREATE TABLE if not exists public.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);