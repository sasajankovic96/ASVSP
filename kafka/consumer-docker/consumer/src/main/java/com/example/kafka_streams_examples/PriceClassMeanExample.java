package com.example.kafka_streams_examples;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import com.example.kafka_streams_examples.serdes.PairSerdes;
import com.example.kafka_streams_examples.util.KafkaConstants;
import com.example.kafka_streams_examples.util.KafkaStreamsUtil;
import com.example.kafka_streams_examples.util.Pair;

public class PriceClassMeanExample {
    public static void main(String[] args) {
        final Properties streamsConfiguration = KafkaStreamsUtil.getStreamsConfiguration("price-class-mean");

        // In the subsequent lines we define the processing topology of the Streams
        // application.
        final StreamsBuilder builder = new StreamsBuilder();

        // Read the input Kafka topic into a KStream instance.
        final KStream<Long, String> flights = builder.stream(KafkaConstants.TOPIC_NAME);

        flights
                .map(new KeyValueMapper<Long, String, KeyValue<String, Long>>() {
                    @Override
                    public KeyValue<String, Long> apply(Long key, String value) {
                        String[] words = value.split(KafkaConstants.SEPARATOR);
                        return new KeyValue<>(words[8], Long.parseLong(words[9]));
                    }
                })

				// KGroupedStream is an abstraction of a grouped record stream of KeyValue pairs.
                // It is an intermediate representation of a KStream in order to apply an aggregation operation on the original KStream records.
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .aggregate(
                		// The Initializer interface for creating an initial value in aggregations.
                        new Initializer<Pair>() {
                            @Override
                            public Pair apply() {
                                return new Pair();
                            }
                        },
						// The Aggregator interface for aggregating values of the given key.
                        new Aggregator<String, Long, Pair>() {
                            @Override
                            public Pair apply(final String key, final Long value, final Pair aggregate) {
                                ++aggregate.cnt;
                                aggregate.sum += value;
                                return aggregate;
                            }
                        },
						// Used to describe how a StateStore should be materialized.
                        Materialized.with(Serdes.String(), new PairSerdes())
                )
                .mapValues(new ValueMapper<Pair, Double>() {
                    @Override
                    public Double apply(Pair value) {
                        return value.sum / (double) value.cnt;
                    }
                })
                .toStream()
                .to(KafkaConstants.TOPIC_NAME + "-price-mean", Produced.with(Serdes.String(), Serdes.Double()));

        @SuppressWarnings("resource") final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
