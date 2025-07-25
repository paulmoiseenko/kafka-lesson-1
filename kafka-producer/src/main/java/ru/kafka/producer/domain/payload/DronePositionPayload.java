package ru.kafka.producer.domain.payload;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.kafka.producer.domain.dto.DroneDTO;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DronePositionPayload {
    private DroneDTO drone;
    private String latitude;
    private String longitude;
}
