package com.wickedwitch.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKey {

    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger LOGGER = LoggerFactory.getLogger(ProducerWithKey.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 20; i++) {

            String topic = "first-topic";
            String value = "####" + i;
            String key = "Key_" + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            LOGGER.info("Key: " + key);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        LOGGER.info("Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        LOGGER.error("Error while producing: " + e);
                    }
                }
            }).get();
        }

    }

}
