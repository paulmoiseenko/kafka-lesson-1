package ru.kafka.producer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.kafka.producer.domain.dto.DroneDTO;
import ru.kafka.producer.domain.payload.DronePositionPayload;
import ru.kafka.producer.generator.DroneGenerator;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DronePositionProducer implements Runnable {

    private static final String DRONE_POSITIONS_TOPIC = "drone_positions";

    private final Faker faker = Faker.instance();
    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaProducer<String, Object> producer;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public DronePositionProducer(KafkaProducer<String, Object> producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                generateMessages();
            } catch (Exception e) {
                log.error("Error in generateMessages", e);
            }
        }, 0, 3, TimeUnit.SECONDS);
    }

    private void generateMessages() {
        List<DroneDTO> drones = DroneGenerator.getDrones();
        drones.forEach(drone -> {
            DronePositionPayload payload = new DronePositionPayload(drone, faker.address().latitude(), faker.address().longitude());
            log.info("Send drone_positions, payload {}", payload);
            try {
                producer.send(new ProducerRecord<>(DRONE_POSITIONS_TOPIC, drone.getName(), mapper.writeValueAsString(payload)));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
