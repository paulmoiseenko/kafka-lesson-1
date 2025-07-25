package ru.kafka.producer.generator;

import com.github.javafaker.Faker;
import lombok.Getter;
import ru.kafka.producer.domain.dto.DroneDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DroneGenerator {

    @Getter
    public static final List<DroneDTO> drones = new ArrayList<>();

    static {
        int count = (int) (Math.random() * (8 - 3) + 3);
        for (int i = 0; i < count; i++) {
            drones.add(new DroneDTO(UUID.randomUUID().toString(), Faker.instance().funnyName().name()));
        }
    }
}
