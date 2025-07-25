package ru.kafka.producer.generator;

import com.github.javafaker.Faker;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.kafka.producer.domain.dto.DroneDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class DroneGenerator {

    private final Faker faker = Faker.instance();
    @Getter
    private final List<DroneDTO> drones = new ArrayList<>();

    @PostConstruct
    private void init() {
        int count = (int) (Math.random() * (8 - 3) + 3);
        for (int i = 0; i < count; i++) {
            drones.add(new DroneDTO(UUID.randomUUID().toString(), faker.funnyName().name()));
        }
    }
}
