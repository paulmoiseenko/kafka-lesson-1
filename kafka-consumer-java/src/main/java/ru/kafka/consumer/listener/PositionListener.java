package ru.kafka.consumer.listener;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import ru.kafka.consumer.config.KafkaConfig;
import ru.kafka.consumer.domain.PositionDTO;
import ru.kafka.consumer.listener.abstracts.AbstractBaseListener;
import ru.kafka.consumer.service.StorageService;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

@Slf4j
public class PositionListener extends AbstractBaseListener implements Runnable {

    private final StorageService storageService;

    public PositionListener(StorageService storageService) {
        this.storageService = storageService;
    }

    @Override
    public void run() {
        Properties properties = KafkaConfig.getConsumerProps();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("drone_positions"));
            consumeLoop(consumer);
        }
    }

    @Override
    public void handleRecordSafely(ConsumerRecord<String, String> record) {
        try {
            Map<String, Object> kafkaMessage = objectMapper.readValue(record.value(), new TypeReference<>() {
            });
            log.info("Receive message: {}", kafkaMessage);

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

        } catch (Exception e) {
            log.error("Ошибка при обработке сообщения: {}", record.value(), e);
        }
    }
}
