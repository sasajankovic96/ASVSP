package com.example.kafka_streams_examples.util;

public interface KafkaConstants {
    public static final String KAFKA_BROKERS = System.getenv("KAFKA_BROKERS");

    public static final String CLIENT_ID = "client1";

    public static final String TOPIC_NAME = System.getenv("TOPIC_NAME");

    public static final String SEPARATOR = "#";
}
