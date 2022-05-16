from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


def get_schema():
    value_schema_str = """
{
   "namespace": "my.test",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
    }
"""

    key_schema_str = """
{
   "namespace": "my.test",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""
    return value_schema_str, key_schema_str


value_schema_str, key_schema_str = get_schema()
value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)

value = {"name": "Value"}
key = {"name": "Key"}


class SchemaHandler:
    def __init__(self, value, key):
        self._value = value
        self._key = key


def main():
    avroProducer = AvroProducer({
        'bootstrap.servers': 'localhost',
        'on_delivery': delivery_report,
        'schema.registry.url': 'http://schema_registry_host:port'
    }, default_key_schema=key_schema, default_value_schema=value_schema)

    avroProducer.produce(topic='my_topic', value=value, key=key)
    avroProducer.flush()
