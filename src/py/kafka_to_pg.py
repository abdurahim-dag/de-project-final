"""Модуль чтения данных из Kafka посредством PySpark"""
from time import sleep

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, TimestampType
from pyspark.sql import functions as F
from schemas import message_columns, message_schema

from settings import TOPIC_NAME
from settings import kafka_security_options
from settings import postgresql_settings
from settings import spark_jars_packages

# Таргет таблица.
postgresql_settings['dbtable'] = 'public.messages'


def  spark_init(name: str) -> SparkSession:
    """Создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL.

    :param name: Наименование спрак сессии.
    :return: Спарк сессия.
    :rtype: SparkSession
    """
    return (SparkSession.builder.master('local[*]')
            .appName(name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate())


def transaction_stream(spark: SparkSession, options: dict) -> DataFrame:
    """Читаем из топика Kafka сообщения.

    :param spark: Рабочая спарк сессия.
    :param options: Параметры подключения к PG
    :return:
    """
    return (spark.readStream
            .format('kafka')
            .options(**options)
            .option('subscribe', TOPIC_NAME)
            #.option("startingOffsets", "earliest")
            .load()
            )


def message_read_stream(df: DataFrame, schema: StructType) -> DataFrame:
    """Чтение из потока."""
    return (df
            .withColumn('value_str', F.col('value').cast(StringType()))
            .withColumn('message', F.from_json(F.col('value_str'), schema))
            .selectExpr('message.*')
            .withColumn("payload", F.to_json(F.col("payload")))
            .dropDuplicates(['object_id', 'sent_dttm'])
            .withWatermark('sent_dttm', '1 minutes')
            )


def foreach_batch_function(df: DataFrame, epoch_id):
    """Метод для записи данных в PostgreSQL."""

    # записываем df в PostgreSQL с полем feedback
    df \
        .select(message_columns) \
        .dropna() \
        .write.format("jdbc") \
        .mode('append') \
        .options(**postgresql_settings) \
        .option("stringtype", "unspecified") \
        .save()


if __name__ == "__main__":
    spark = spark_init('messages stream consumer')

    df = transaction_stream(spark, kafka_security_options)
    prepared = message_read_stream(df, message_schema)

    query = prepared \
        .writeStream \
        .option("checkpointLocation", "cp_messages-0") \
        .trigger(processingTime="15 seconds") \
        .foreachBatch(foreach_batch_function) \
        .start()

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(60)

    query.awaitTermination()

