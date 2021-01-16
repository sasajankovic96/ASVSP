package com.example.kafka_streams_examples;

import com.example.kafka_streams_examples.util.KafkaConstants;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class TotalNumberOfAccidents {
    public static final String FROM_TOPIC_NAME = System.getenv("TOPIC_NAME");
    public static final String TO_TOPIC = "car-accidents-proxy-topic";
    public static final String KAFKA_BROKERS = System.getenv("KAFKA_BROKERS");

    public static void main(String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> carAccidents = builder.stream(FROM_TOPIC_NAME);

        carAccidents.to(TO_TOPIC);

        final KafkaStreams streams =
                new KafkaStreams(builder.build(), getStreamsConfiguration("carAccidents"));
        System.out.println(streams.toString());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Properties getStreamsConfiguration(String exampleName) {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name. The name must be unique in the
        // Kafka cluster against which the application is run.
        streamsConfiguration.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                KafkaConstants.TOPIC_NAME + "-streams-example-" + exampleName);
        streamsConfiguration.put(
                StreamsConfig.CLIENT_ID_CONFIG,
                KafkaConstants.TOPIC_NAME + "-streams-example-client-" + exampleName);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        streamsConfiguration.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        return streamsConfiguration;
    }
}
