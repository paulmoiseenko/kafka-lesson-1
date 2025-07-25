package ru.kafka.consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import ru.kafka.consumer.config.KafkaConfig;
import ru.kafka.consumer.domain.DeliveryStatusDTO;
import ru.kafka.consumer.listener.abstracts.AbstractBaseListener;
import ru.kafka.consumer.service.ReportService;
import ru.kafka.consumer.service.StorageService;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class DeliveryStatusListener extends AbstractBaseListener implements Runnable {

    private final StorageService storageService;
    private final ReportService reportService;

    public DeliveryStatusListener(StorageService storageService, ReportService reportService) {
        this.storageService = storageService;
        this.reportService = reportService;
    }

    @Override
    public void run() {
        Properties properties = KafkaConfig.getConsumerProps();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("drone_delivery_statuses"));
            consumeLoop(consumer);
        }
    }

    @Override
    public void processMessage(Map<String, Object> kafkaMessage) {
        log.info("Receive message: {}", kafkaMessage);

        String address = String.valueOf(kafkaMessage.get("address"));
        String timeToDeliveryStr = (String) kafkaMessage.get("timeToDelivery");

        LocalDateTime timeToDelivery = null;
        if (timeToDeliveryStr != null) {
            Instant instant = Instant.parse(timeToDeliveryStr);
            timeToDelivery = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        }

        Object droneObj = kafkaMessage.get("drone");
        if (!(droneObj instanceof Map<?, ?> droneMap)) {
            return;
        }

        String uuid = String.valueOf(droneMap.get("uuid"));
        String name = String.valueOf(droneMap.get("name"));

        DeliveryStatusDTO status = new DeliveryStatusDTO(address, timeToDelivery);
        storageService.putToDeliveryStatus(uuid, status);
        reportService.generateReport(uuid, name);
    }
}
