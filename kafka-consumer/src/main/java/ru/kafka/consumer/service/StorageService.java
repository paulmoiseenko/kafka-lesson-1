package ru.kafka.consumer.service;

import org.springframework.stereotype.Service;
import ru.kafka.consumer.domain.DeliveryStatusDTO;
import ru.kafka.consumer.domain.MetricDTO;
import ru.kafka.consumer.domain.PositionDTO;

import java.util.HashMap;

@Service
public class StorageService {

    private final HashMap<String, DeliveryStatusDTO> deliveryStatus = new HashMap<>();
    private final HashMap<String, PositionDTO> positionStatus = new HashMap<>();
    private final HashMap<String, MetricDTO> metricState = new HashMap<>();

    public DeliveryStatusDTO getActualDeliveryStatus(String uuid) {
        return deliveryStatus.get(uuid);
    }

    public PositionDTO getActualPositionStatus(String uuid) {
        return positionStatus.get(uuid);
    }

    public MetricDTO getActualMetricState(String uuid) {
        return metricState.get(uuid);
    }

    public void putToDeliveryStatus(String uuid, DeliveryStatusDTO status) {
        deliveryStatus.put(uuid, status);
    }

    public void putToPositionStatus(String uuid, PositionDTO position) {
        positionStatus.put(uuid, position);
    }

    public void putToMetricState(String uuid, MetricDTO metric) {
        metricState.put(uuid, metric);
    }

}
