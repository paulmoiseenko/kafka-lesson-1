package ru.kafka.consumer.service;


import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import ru.kafka.consumer.domain.DeliveryStatusDTO;
import ru.kafka.consumer.domain.MetricDTO;
import ru.kafka.consumer.domain.PositionDTO;
import ru.kafka.consumer.domain.ReportDTO;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import static java.util.Objects.nonNull;
import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

@Service
@RequiredArgsConstructor
public class ReportService {

    private final StorageService storageService;

    @Async
    public void generateReport(String uuid, String name) {
        PositionDTO position = storageService.getActualPositionStatus(uuid);
        if (nonNull(position)) {
            DeliveryStatusDTO delivery = storageService.getActualDeliveryStatus(uuid);
            MetricDTO metric = storageService.getActualMetricState(uuid);
            ReportDTO report = new ReportDTO(name, delivery.getAddress(), delivery.getTimeToDelivery(),
                    position.getLatitude(), position.getLongitude(), metric.getTemperature());
            appendReportToCsvFile(report);
        }
    }

    private String toCsvLine(ReportDTO report) {
        return String.join(";",
                report.getDrone(),
                report.getAddress(),
                report.getDeliveryTime().toString(),
                report.getLatitude(),
                report.getLongitude(),
                String.valueOf(report.getTemperature())
        );
    }

    private void appendReportToCsvFile(ReportDTO report) {
        File file = new File("report.csv");
        boolean isNewFile = !file.exists();
        try (FileWriter writer = new FileWriter(file, true)) {
            if (isNewFile) {
                writer.write("Drone;Address;DeliveryTime;Latitude;Longitude;Temperature" + System.lineSeparator());
            }
            writer.write(toCsvLine(report) + System.lineSeparator());
        } catch (IOException e) {
            log.error(Arrays.toString(e.getStackTrace()));
        }
    }
}
