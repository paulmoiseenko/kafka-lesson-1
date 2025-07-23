package ru.kafka.producer.producer;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.kafka.producer.domain.dto.DroneDTO;
import ru.kafka.producer.domain.payload.DronePositionPayload;
import ru.kafka.producer.generator.DroneGenerator;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class DronePositionProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final DroneGenerator droneGenerator;
    private final Faker faker = Faker.instance();

    private static final String DRONE_POSITIONS_TOPIC = "drone_positions";

    @Scheduled(fixedRate = 800)
    private void generateMessages() {
        List<DroneDTO> drones = droneGenerator.getDrones();
        drones.forEach(drone -> {
            DronePositionPayload payload = new DronePositionPayload(drone, faker.address().latitude(), faker.address().longitude());
            log.debug("Send drone_positions, payload {}", payload);
            kafkaTemplate.send(DRONE_POSITIONS_TOPIC, drone.getName(), payload);
        });
    }
}
