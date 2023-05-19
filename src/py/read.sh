kafkacat -b rc1a-t8qa66m62evas38u.mdb.yandexcloud.net:9091 -t transaction-service-input -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username="consumer_producer" -X sasl.password="Ragim1984" -X ssl.ca.location=/mnt/c/Users/Admin/PycharmProjects/de-project-final/CA.pem -C -o beginning
curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/project/register_kafka -H 'Content-Type: application/json; charset=utf-8' --data-binary @- << EOF
{
    "student": "abdurahimd",
    "kafka_connect":{
        "host": "rc1a-t8qa66m62evas38u.mdb.yandexcloud.net",
        "port": 9091,
        "topic": "message-service-input",
        "producer_name": "consumer_producer",
        "producer_password": "Ragim1984"
    }
}
EOF

curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/project/delete_kafka -H 'Content-Type: application/json; charset=utf-8' --data-binary @- << EOF
{
    "student": "abdurahimd"
}
EOF