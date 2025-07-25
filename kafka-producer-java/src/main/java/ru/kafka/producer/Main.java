package ru.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import ru.kafka.producer.config.KafkaConfig;
import ru.kafka.producer.producer.DroneDeliveryStatusProducer;
import ru.kafka.producer.producer.DroneMetricsProducer;
import ru.kafka.producer.producer.DronePositionProducer;

public class Main {
    public static void main(String[] args) {
        KafkaConfig.createTopicsIfMissing();
        KafkaProducer<String, Object> producer = new KafkaProducer<>(KafkaConfig.getProducerProps());

        DroneDeliveryStatusProducer statusProducer = new DroneDeliveryStatusProducer(producer);
        DroneMetricsProducer metricsProducer = new DroneMetricsProducer(producer);
        DronePositionProducer positionProducer = new DronePositionProducer(producer);


        Thread deliveryThread = new Thread(statusProducer, "delivery-producer-thread");
        Thread metricThread = new Thread(metricsProducer, "drone-producer-thread");
        Thread positionThread = new Thread(positionProducer, "position-producer-thread");

        metricThread.start();
        positionThread.start();
        deliveryThread.start();

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        producer.close();
    }
}
