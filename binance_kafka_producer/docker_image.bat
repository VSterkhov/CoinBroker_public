docker build -t sterkhov/binance_kafka_producer .
docker login -u "sterkhov" docker.io
docker push sterkhov/binance_kafka_producer