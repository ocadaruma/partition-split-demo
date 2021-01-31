package com.mayreh.kafka.demo;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerApp extends Thread implements AutoCloseable {
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final Consumer<String, String> consumer;

    public ConsumerApp(Config config) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "30000");
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Collections.singleton(config.topic()));
    }

    @Override
    public void run() {
        while (!terminated.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("[ID=%s] received.", record.value()));
            }

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws InterruptedException {
        terminated.set(true);
        join();
        consumer.close();
    }
}
