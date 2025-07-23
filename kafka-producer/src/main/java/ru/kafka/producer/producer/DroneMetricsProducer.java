package ru.kafka.producer.producer;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.kafka.producer.domain.dto.DroneDTO;
import ru.kafka.producer.domain.payload.DroneMetricPayload;
import ru.kafka.producer.generator.DroneGenerator;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class DroneMetricsProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final DroneGenerator droneGenerator;
    private final Faker faker = Faker.instance();

    private static final String DRONE_METRICS_TOPIC = "drone_metrics";


    @Scheduled(fixedRate = 3000)
    private void generateMessages() {
        List<DroneDTO> drones = droneGenerator.getDrones();
        drones.forEach(drone -> {
            DroneMetricPayload payload = new DroneMetricPayload(drone, faker.weather().temperatureCelsius(), faker.number().numberBetween(1, 100));
            log.debug("Send drone_metrics, payload {}", payload);
            kafkaTemplate.send(DRONE_METRICS_TOPIC, payload);
        });
    }
}
