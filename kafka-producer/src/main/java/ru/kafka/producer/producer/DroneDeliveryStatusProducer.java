package ru.kafka.producer.producer;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.kafka.producer.domain.dto.DroneDTO;
import ru.kafka.producer.domain.payload.DroneDeliveryStatusPayload;
import ru.kafka.producer.generator.DroneGenerator;

import java.time.Instant;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class DroneDeliveryStatusProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final DroneGenerator droneGenerator;
    private final Faker faker = Faker.instance();

    private static final String DRONE_DELIVERY_STATUS_TOPIC = "drone_delivery_statuses";

    @Scheduled(fixedRate = 60000)
    private void generateMessages() {
        List<DroneDTO> drones = droneGenerator.getDrones();
        drones.forEach(drone -> {
            DroneDeliveryStatusPayload payload = new DroneDeliveryStatusPayload(drone, faker.address().fullAddress(), Instant.now().plusSeconds(60).toString());
            log.debug("Send drone_delivery_statuses, payload {}", payload);
            kafkaTemplate.send(DRONE_DELIVERY_STATUS_TOPIC, drone.getName(), payload);
        });
    }
}
