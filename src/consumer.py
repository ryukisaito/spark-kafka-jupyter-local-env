from confluent_kafka import Consumer

conf = {'bootstrap.servers': "localhost",
        'group.id': "foo",
        'auto.offset.reset': 'smallest'}

c = Consumer(conf)
consumer = Consumer(conf)
