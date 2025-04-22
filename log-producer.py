import random
from faker import Faker
from datetime import datetime
from confluent_kafka import Producer
import time

producer = Producer({'bootstrap.servers': 'localhost:9092'})
topic = 'logs'

fake = Faker()

def generate_log_line(timestamp):
   
    ip = fake.ipv4()
    method = random.choice(['GET', 'POST'])
    if random.random() < 0.5:
        path = f'/product/{random.randint(1000, 9000)}'
    else:
        path = random.choice(['/index', '/login', '/contact'])
    protocol = 'HTTP/1.1'
    status_code = random.choice([200, 301, 400, 404, 500])
    response_size = random.randint(200, 5000)

    return f'{ip} {timestamp} {method} {path} {protocol} {status_code} {response_size}'

def send_one_log():
    now = datetime.now().strftime('%Y-%m-%d:%H:%M:%S')

    log_line = generate_log_line(now)

    print(log_line)
    producer.poll(0)
    producer.produce(topic, log_line.encode('utf-8'))
    producer.flush()


while True:
    send_one_log()
    time.sleep(1)