package ru.kafka.consumer.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kafka.consumer.domain.MetricDTO;
import ru.kafka.consumer.service.StorageService;
import ru.kafka.consumer.service.TemperatureMonitorService;

import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class MetricListener {

    private final StorageService storageService;
    private final TemperatureMonitorService monitorService;

    @KafkaListener(topics = "drone_metrics")
    public void listenPositionDrone(Map<Object, Object> kafkaMessage) {
        log.info("Получено сообщение: {}", kafkaMessage);

        String temperature = String.valueOf(kafkaMessage.get("temperature"));
        int battery = (int) kafkaMessage.get("battery");

        Object droneObj = kafkaMessage.get("drone");
        if (!(droneObj instanceof Map<?, ?> droneMap)) {
            return;
        }
        String uuid = String.valueOf(droneMap.get("uuid"));
        MetricDTO metric = new MetricDTO(temperature, battery);

        storageService.putToMetricState(uuid, metric);
        monitorService.handleMetrics(uuid, metric);
    }
}
