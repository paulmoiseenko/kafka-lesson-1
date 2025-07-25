package ru.kafka.consumer.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kafka.consumer.domain.PositionDTO;
import ru.kafka.consumer.service.StorageService;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.TimeZone;

@Component
@Slf4j
@RequiredArgsConstructor
public class PositionListener {

    private final StorageService storageService;

    @KafkaListener(topics = "drone_positions")
    public void listenPositionDrone(Map<Object, Object> kafkaMessage, ConsumerRecord<String, Map<Object, Object>> record) {
        log.info("Получено сообщение: {}", kafkaMessage);

        LocalDateTime time = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(record.timestamp()),
                TimeZone.getDefault().toZoneId()
        );

        String latitude = String.valueOf(kafkaMessage.get("latitude"));
        String longitude = String.valueOf(kafkaMessage.get("longitude"));

        Object droneObj = kafkaMessage.get("drone");
        if (!(droneObj instanceof Map<?, ?> droneMap)) {
            return;
        }
        String uuid = String.valueOf(droneMap.get("uuid"));

        PositionDTO position = new PositionDTO(latitude, longitude, time);
        storageService.putToPositionStatus(uuid, position);
    }
}
