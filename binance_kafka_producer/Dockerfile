FROM openjdk:8-jre-alpine
COPY ./build/libs/binance_kafka_producer-all.jar /usr/app/
WORKDIR /usr/app
ENTRYPOINT ["java", "-jar", "binance_kafka_producer-all.jar"]