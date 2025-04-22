from confluent_kafka import Consumer  # Kafka Consumer 라이브러리 임포트

# Kafka Consumer 설정
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka 브로커 주소
    'group.id': 'upbit-consumer',  # Consumer 그룹 ID
}

# Kafka Consumer 객체 생성
c = Consumer(conf)

# 'temp' 토픽 구독
c.subscribe(['temp'])

try:
    while True:
        # Kafka 메시지 폴링 (1초 대기)
        msg = c.poll(1.0)
        if msg is None:  # 메시지가 없으면 다음 반복으로 이동
            continue
        if msg.error():  # 메시지에 에러가 있으면 에러 출력 후 다음 반복으로 이동
            print(f"Error: {msg.error()}")
            continue
        # 메시지 값 디코딩 및 출력
        result = msg.value().decode('utf-8')
        print(result)
except KeyboardInterrupt:  # 키보드 인터럽트 발생 시 Consumer 종료
    c.close()