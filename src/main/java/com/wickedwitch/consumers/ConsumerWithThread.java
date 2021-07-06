package com.wickedwitch.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {

    Logger LOGGER = LoggerFactory.getLogger(ConsumerWithThread.class);

    public static void main(String[] args) {
        new ConsumerWithThread().run();
    }

    public ConsumerWithThread() {

    }

    public void run() {
        Logger LOGGER = LoggerFactory.getLogger(ConsumerWithThread.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-next-04-application";
        String topic = "first-topic";

        //create latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create consumer runnable
        LOGGER.info("Creating the consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(latch, bootstrapServer, groupId, topic);

        //start thread
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Application got interrupted", e);
        } finally {
            LOGGER.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {


        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch,
                                String bootstrapServer,
                                String groupId,
                                String topic) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<>(properties);

            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.info("Key: " + record.key() + " | Value: " + record.value());
                        LOGGER.info("Partition: " + record.partition() + " | Offset: " + record.offset());
                    }
                }
            } catch (WakeupException ex) {
                LOGGER.info("Received shutdown signal");
            } finally {
                consumer.close();
                // tell main code we are done with consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            consumer.wakeup();

        }
    }
}
