package ru.kafka.consumer.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PositionDTO {
    private String latitude;
    private String longitude;
    private LocalDateTime time;
}
