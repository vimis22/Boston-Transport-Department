from pyspark.sql.functions import expr
from pyspark.sql.avro.functions import from_avro


# Helper function to decode Avro payload, skipping the 5-byte Confluent header
def decode_avro_payload(col_name: str, schema: str):
    """Decode Avro payload, skipping the 5-byte Confluent header (Magic Byte + Schema ID)."""
    return from_avro(expr(f"substring({col_name}, 6, length({col_name})-5)"), schema)
