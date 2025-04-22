from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'upbit-consumer',
}

c = Consumer(conf)
c.subscribe(['temp'])
try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        result = msg.value().decode('utf-8')
        print(result)
except KeyboardInterrupt:
    c.close()