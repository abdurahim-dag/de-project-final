from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType, MapType

# определяем схему входного сообщения для json
message_schema = StructType([
    StructField("object_id", StringType(), nullable=False),
    StructField("object_type", StringType(), nullable=False),
    StructField("sent_dttm", TimestampType(), nullable=False),
    #StructField("payload", StructType(), nullable=False)
    StructField("payload", MapType(StringType(), StringType()), nullable=False)

])

message_columns = [
    'object_id',
    'object_type',
    'sent_dttm',
    'payload',
]

# transaction_columns = [
#     'message',
# ]
