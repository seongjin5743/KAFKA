- start

```bash
# zookeeper 실행
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
# kafka 실행
bin/kafka-server-start.sh -daemon config/server.properties
```

- create topic

```bash
bin/kafka-topics.sh --create --topic temp --bootstrap-server localhost:9092
```

- producer (publisher) 실행

```bash
bin/kafka-console-producer.sh --bootstrap-server loc
alhost:9092 --topic temp
```

- consumer (subscriber) 실행
    - `--from-beginning` : topic의 가장 첫번째 메세지 부터 수신

```bash
bin/kafka-console-consumer.sh --bootstrap-server loc
alhost:9092 --topic temp
```

- kafka - spark 연결 설정

```bash
spark.jars.packages => `org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4`
```