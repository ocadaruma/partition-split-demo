package com.mayreh.kafka.demo;

public class DemoMain {
    public static void main(String[] args) throws Exception {
        Config config = new Config(System.getenv("DEMO_BOOTSTRAP_SERVERS"),
                                   System.getenv("DEMO_TOPIC"));

        ConsumerApp consumer = new ConsumerApp(config);
        ProducerApp producer = new ProducerApp(config);

        consumer.start();
        Thread.sleep(5000L);
        producer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                producer.close();
                consumer.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }
}
