from pyspark.sql.types import MapType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType


# определяем схему входного сообщения для json
message_schema = StructType([
    StructField("object_id", StringType(), nullable=False),
    StructField("object_type", StringType(), nullable=False),
    StructField("sent_dttm", TimestampType(), nullable=False),
    StructField("payload", MapType(StringType(), StringType()), nullable=False)

])

# Список столбцов в таблице PG.
message_columns = [
    'object_id',
    'object_type',
    'sent_dttm',
    'payload',
]
