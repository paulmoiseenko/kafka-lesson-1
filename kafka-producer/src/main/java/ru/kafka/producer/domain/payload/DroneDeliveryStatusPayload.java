package ru.kafka.producer.domain.payload;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.kafka.producer.domain.dto.DroneDTO;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DroneDeliveryStatusPayload {
    private DroneDTO drone;
    private String address;
    private String timeToDelivery;
}
