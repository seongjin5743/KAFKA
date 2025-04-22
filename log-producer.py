import random  # 랜덤 값을 생성하기 위한 random 모듈 임포트
from faker import Faker  # 가짜 데이터를 생성하기 위한 Faker 라이브러리 임포트
from datetime import datetime  # 현재 시간 생성을 위한 datetime 모듈 임포트
from confluent_kafka import Producer  # Kafka Producer 라이브러리 임포트
import time  # 시간 지연을 위한 time 모듈 임포트

# Kafka Producer 객체 생성
producer = Producer({'bootstrap.servers': 'localhost:9092'})  # Kafka 브로커 주소 설정
topic = 'logs'  # 전송할 Kafka 토픽 이름

# Faker 객체 생성
fake = Faker()

# 로그 라인 생성 함수
def generate_log_line(timestamp):
    # 랜덤 IP 주소 생성
    ip = fake.ipv4()
    # HTTP 메서드 랜덤 선택
    method = random.choice(['GET', 'POST'])
    # 랜덤 경로 생성
    if random.random() < 0.5:
        path = f'/product/{random.randint(1000, 9000)}'  # 상품 경로
    else:
        path = random.choice(['/index', '/login', '/contact'])  # 고정된 경로 중 선택
    # HTTP 프로토콜 버전
    protocol = 'HTTP/1.1'
    # HTTP 상태 코드 랜덤 선택
    status_code = random.choice([200, 301, 400, 404, 500])
    # 응답 크기 랜덤 생성
    response_size = random.randint(200, 5000)

    # 로그 라인 포맷팅
    return f'{ip} {timestamp} {method} {path} {protocol} {status_code} {response_size}'

# 로그 한 줄을 Kafka에 전송하는 함수
def send_one_log():
    # 현재 시간 생성
    now = datetime.now().strftime('%Y-%m-%d:%H:%M:%S')

    # 로그 라인 생성
    log_line = generate_log_line(now)

    # 생성된 로그 출력
    print(log_line)
    # Kafka 내부 큐를 비우기 위한 호출
    producer.poll(0)
    # Kafka 토픽에 로그 전송
    producer.produce(topic, log_line.encode('utf-8'))
    # 메시지가 브로커로 전송될 때까지 대기
    producer.flush()

# 무한 루프를 통해 로그를 지속적으로 생성 및 전송
while True:
    send_one_log()  # 로그 한 줄 전송
    time.sleep(1)  # 1초 대기 후 다음 로그 전송