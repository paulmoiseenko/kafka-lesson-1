package ru.kafka.consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import ru.kafka.consumer.config.KafkaConfig;
import ru.kafka.consumer.domain.MetricDTO;
import ru.kafka.consumer.listener.abstracts.AbstractBaseListener;
import ru.kafka.consumer.service.StorageService;
import ru.kafka.consumer.service.TemperatureMonitorService;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class MetricListener extends AbstractBaseListener implements Runnable {

    private final StorageService storageService;
    private final TemperatureMonitorService monitorService;

    public MetricListener(StorageService storageService, TemperatureMonitorService monitorService) {
        this.storageService = storageService;
        this.monitorService = monitorService;
    }

    @Override
    public void run() {
        Properties properties = KafkaConfig.getConsumerProps();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("drone_metrics"));
            consumeLoop(consumer);
        }
    }

    @Override
    public void processMessage(Map<String, Object> kafkaMessage) {
        log.info("Receive message: {}", kafkaMessage);

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
