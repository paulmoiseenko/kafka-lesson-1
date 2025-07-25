package ru.kafka.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.kafka.consumer.domain.MetricDTO;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class TemperatureMonitorService {

    private static final double TEMPERATURE_THRESHOLD = 15.0;
    private static final long ONE_MINUTE_MILLIS = 60_000;

    private final Map<String, List<TempRecord>> droneTemperatureHistory = new HashMap<>();

    private final Map<String, Long> lastAlertTime = new HashMap<>();

    public void handleMetrics(String uuid, MetricDTO metricDTO) {
        double temp;
        try {
            temp = Double.parseDouble(metricDTO.getTemperature().replace("°C", "").trim());
        } catch (NumberFormatException e) {
            log.error("Ошибка парсинга температуры: {}", metricDTO.getTemperature());
            return;
        }

        long now = System.currentTimeMillis();

        droneTemperatureHistory
                .computeIfAbsent(uuid, k -> new ArrayList<>())
                .add(new TempRecord(now, temp));

        List<TempRecord> records = droneTemperatureHistory.get(uuid);
        records.removeIf(r -> r.timestamp < now - ONE_MINUTE_MILLIS);

        double average = records.stream().mapToDouble(r -> r.temperature).average().orElse(0.0);

        if (average > TEMPERATURE_THRESHOLD) {
            long lastAlert = lastAlertTime.getOrDefault(uuid, 0L);
            if (now - lastAlert >= ONE_MINUTE_MILLIS) {
                String message = String.format("[%s] ALERT: Средняя температура дрона %s за минуту = %.2f°C",
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault()),
                        uuid, average);
                log.warn(message);
                writeAlertToFile(message);
                lastAlertTime.put(uuid, now);
            }
        }
    }

    private void writeAlertToFile(String message) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("alerts.log", true))) {
            writer.write(message);
            writer.newLine();
        } catch (IOException e) {
            log.error("Ошибка записи в файл alerts.log: {}", e.getMessage());
        }
    }

    private static class TempRecord {
        long timestamp;
        double temperature;

        TempRecord(long timestamp, double temperature) {
            this.timestamp = timestamp;
            this.temperature = temperature;
        }
    }
}
