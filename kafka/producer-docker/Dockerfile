FROM maven AS maven_build

COPY ./producer /usr/producer
RUN mvn -f /usr/producer/pom.xml clean package

FROM openjdk:8
COPY --from=maven_build /usr/producer/target /usr/producer
ENTRYPOINT ["java","-cp","/usr/producer/kafka-topic-producer-0.0.1-SNAPSHOT.jar", "com.example.kafka_topic_producer.App"]  
