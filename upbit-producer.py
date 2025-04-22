from confluent_kafka import Producer
import time
import requests

upbit_URL = 'https://api.upbit.com/v1/ticker?markets=KRW-BTC'

conf = {
    'bootstrap.servers': 'localhost:9092',
}

p = Producer(conf)

while True:
    res = requests.get(upbit_URL)
    bit_data = res.json()[0]
    result = f'{bit_data["market"]}, {bit_data["trade_date"]}, {bit_data["trade_time"]}, {bit_data["trade_price"]}'
    p.poll(0)
    p.produce('temp', result)
    p.flush()
    time.sleep(5)
