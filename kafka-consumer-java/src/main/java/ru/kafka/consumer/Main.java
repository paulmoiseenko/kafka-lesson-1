package ru.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import ru.kafka.consumer.listener.DeliveryStatusListener;
import ru.kafka.consumer.listener.MetricListener;
import ru.kafka.consumer.listener.PositionListener;
import ru.kafka.consumer.service.ReportService;
import ru.kafka.consumer.service.StorageService;
import ru.kafka.consumer.service.TemperatureMonitorService;

@Slf4j
public class Main {
    public static void main(String[] args) {
        StorageService storageService = new StorageService();
        ReportService reportService = new ReportService(storageService);
        TemperatureMonitorService monitorService = new TemperatureMonitorService();

        DeliveryStatusListener deliveryStatusListener = new DeliveryStatusListener(storageService, reportService);
        MetricListener metricListener = new MetricListener(storageService, monitorService);
        PositionListener positionListener = new PositionListener(storageService);


        Thread deliveryThread = new Thread(deliveryStatusListener, "delivery-listener-thread");
        Thread metricThread = new Thread(metricListener, "drone-metrics-thread");
        Thread positionThread = new Thread(positionListener, "position-listener-thread");

        metricThread.start();
        positionThread.start();
        deliveryThread.start();

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
