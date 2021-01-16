package com.example.kafka_topic_producer;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class App {
    public static final String TOPIC_NAME = System.getenv("TOPIC_NAME");
    public static final String CLIENT_ID = "Producer 1";
    public static final String KAFKA_BROKERS = System.getenv("KAFKA_BROKERS");

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Long, String> producer = new KafkaProducer(properties);

        try (InputStream inputStream = new App().getClass().getResourceAsStream("/data.csv"); ) {
            readOneByOneAndProduce(new InputStreamReader(inputStream), producer);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void readOneByOneAndProduce(Reader reader, Producer<Long, String> producer) {
        try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build()) {
            String[] line;
            Long counter = 0l;
            while ((line = csvReader.readNext()) != null) {
                ProducerRecord<Long, String> record =
                        new ProducerRecord<>(
                                TOPIC_NAME,
                                ++counter,
                                Arrays.stream(line).collect(Collectors.joining("#")));
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.println(
                            "Record sent with key "
                                    + counter
                                    + " to partition "
                                    + metadata.partition()
                                    + " with offset "
                                    + metadata.offset());
                    Thread.sleep(500);
                } catch (ExecutionException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                } catch (InterruptedException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                }
            }
            reader.close();
        } catch (IOException e) {
            System.out.println("I/O error occured");
        }
    }
}
