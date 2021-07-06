package com.wickedwitch.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignSeek {

    public static void main(String[] args) {

        Logger LOGGER = LoggerFactory.getLogger(ConsumerAssignSeek.class);

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first-topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //assign and seek are mostly used to replay data or fetch specific message

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 1);
        long offsetToReadFrom = 4L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesRead = 0;
        boolean keepReading = true;

        while(keepReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                numberOfMessagesRead++;
                LOGGER.info("Key: " + record.key() + " | Value: " + record.value());
                LOGGER.info("Partition: " + record.partition() + " | Offset: " + record.offset());
                if(numberOfMessagesRead >= numberOfMessagesToRead){
                    keepReading = false;
                    break;
                }
            }
        }
        LOGGER.info("Exiting to application ");
    }
}
