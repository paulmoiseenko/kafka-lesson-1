package ru.kafka.producer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.kafka.producer.domain.dto.DroneDTO;
import ru.kafka.producer.domain.payload.DroneDeliveryStatusPayload;
import ru.kafka.producer.generator.DroneGenerator;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DroneDeliveryStatusProducer implements Runnable {

    private static final String DRONE_DELIVERY_STATUS_TOPIC = "drone_delivery_statuses";

    private final Faker faker = Faker.instance();
    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaProducer<String, Object> producer;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public DroneDeliveryStatusProducer(KafkaProducer<String, Object> producer) {
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
            DroneDeliveryStatusPayload payload = new DroneDeliveryStatusPayload(drone, faker.address().fullAddress(), Instant.now().plusSeconds(60).toString());
            log.info("Send drone_delivery_statuses, payload {}", payload);
            try {
                producer.send(new ProducerRecord<>(DRONE_DELIVERY_STATUS_TOPIC, mapper.writeValueAsString(payload)));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
        producer.flush();
    }
}
