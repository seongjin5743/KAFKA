from confluent_kafka import Producer  # Kafka Producer 라이브러리 임포트
import time  # 시간 지연을 위한 time 모듈 임포트
import requests  # HTTP 요청을 위한 requests 라이브러리 임포트

# 업비트 API URL (비트코인 현재가 정보)
upbit_URL = 'https://api.upbit.com/v1/ticker?markets=KRW-BTC'

# Kafka 프로듀서 설정
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka 브로커 주소
}

# Kafka 프로듀서 객체 생성
p = Producer(conf)

# 무한 루프를 통해 데이터를 지속적으로 Kafka에 전송
while True:
    # 업비트 API로부터 비트코인 데이터 요청
    res = requests.get(upbit_URL)
    bit_data = res.json()[0]  # 응답 데이터를 JSON으로 변환 후 첫 번째 항목 가져오기

    # 필요한 데이터 추출 및 문자열 포맷팅
    result = f'{bit_data["market"]}, {bit_data["trade_date"]}, {bit_data["trade_time"]}, {bit_data["trade_price"]}'
    
    p.poll(0)  # Kafka 내부 큐를 비우기 위한 호출
    p.produce('temp', result)  # 'temp' 토픽에 데이터 전송
    p.flush()  # 메시지가 브로커로 전송될 때까지 대기
    time.sleep(5)  # 5초 대기 후 다음 데이터 전송