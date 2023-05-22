add-connection:
	docker exec de-sprint-6 bash -c "airflow connections add \"vertica_dwh\" --conn-json '{\
    \"conn_type\": \"vertica\",\
    \"login\": \"st23051601\",\
    \"password\": \"1sZlXezzv3QqBZb\",\
    \"host\": \"51.250.75.20\",\
    \"port\": \"5433\",\
    \"schema\": \"dwh\",\
}'"

add-connection:
	docker exec de-sprint-6 bash -c "airflow connections add \"postgres\" --conn-json '{\
    \"conn_type\": \"postgres\",\
    \"login\": \"sysdba\",\
    \"password\": \"Ragim1984\",\
    \"host\": \"rc1b-ls6h2hxbbkx42arp.mdb.yandexcloud.net\",\
    \"port\": \"6432\",\
    \"schema\": \"de\",\
}'"
