package com.mayreh.kafka.demo;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApp extends Thread implements AutoCloseable {
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final AtomicLong sequence = new AtomicLong(0L);
    private final Producer<String, String> producer;
    private final Random random;
    private final Config config;

    public ProducerApp(Config config) {
        // set seed to generate exactly same sequence
        random = new Random(42L);

        this.config = config;
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "demo-producer");
        props.setProperty(ProducerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    @Override
    public void run() {
        while (!terminated.get()) {
            long id = sequence.getAndIncrement();

            ProducerRecord<String, String> record = new ProducerRecord<>(
                    config.topic(),
                    null,
                    System.currentTimeMillis(),
                    String.valueOf(random.nextInt()),
                    String.valueOf(id));

            try {
                RecordMetadata recordMeta = producer.send(record).get();
                System.out.println(String.format("[ID=%d] sent. tp=%s-%d[offset=%d]",
                                                 id, recordMeta.topic(), recordMeta.partition(), recordMeta.offset()));
                Thread.sleep(1000L);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws InterruptedException {
        terminated.set(true);
        join();
        producer.close();
    }
}
