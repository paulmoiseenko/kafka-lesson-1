package ru.kafka.consumer.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReportDTO {
    private String drone;
    private String address;
    private LocalDateTime deliveryTime;
    private String latitude;
    private String longitude;
    private String temperature;
}
