import os
os.path.abspath('')
# Топик источник.
TOPIC_NAME = 'message-service-input'

# Библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

postgresql_settings = {
    'url': 'jdbc:postgresql://rc1b-ls6h2hxbbkx42arp.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'user': 'sysdba',
    'password': 'Ragim1984',

}

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"consumer_producer\" password=\"Ragim1984\";',
    'kafka.bootstrap.servers': 'rc1a-t8qa66m62evas38u.mdb.yandexcloud.net:9091',
    "kafka.ssl.truststore.location": r'C:\Users\Admin\PycharmProjects\de-project-final\src\py\truststore.jks',
    "kafka.ssl.truststore.password": r'your_password'
}
