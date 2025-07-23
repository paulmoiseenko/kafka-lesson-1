package ru.kafka.consumer.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kafka.consumer.domain.DeliveryStatusDTO;
import ru.kafka.consumer.service.ReportService;
import ru.kafka.consumer.service.StorageService;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class DeliveryStatusListener {

    private final StorageService storageService;
    private final ReportService reportService;

    @KafkaListener(topics = "drone_delivery_statuses")
    public void listenPositionDrone(Map<Object, Object> kafkaMessage) {
        log.info("Получено сообщение: {}", kafkaMessage);

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
        DeliveryStatusDTO status = new DeliveryStatusDTO(address, timeToDelivery);

        storageService.putToDeliveryStatus(uuid, status);
        reportService.generateReport(uuid, String.valueOf(droneMap.get("name")));
    }
}
