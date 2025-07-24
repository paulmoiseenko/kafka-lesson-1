package ru.kafka.consumer.listener.abstracts;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

@Slf4j
public abstract class AbstractBaseListener {

    protected final ObjectMapper objectMapper = new ObjectMapper();

    public void consumeLoop(KafkaConsumer<String, String> consumer) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(this::handleRecordSafely);
            } catch (Exception e) {
                log.error("Ошибка в poll/consume: {}", e.getMessage());
                log.error(Arrays.toString(e.getStackTrace()));
            }
        }
    }

    public void handleRecordSafely(ConsumerRecord<String, String> record) {
        try {
            Map<String, Object> message = objectMapper.readValue(record.value(), new TypeReference<>() {
            });
            processMessage(message);
        } catch (Exception e) {
            log.error("Ошибка при обработке сообщения: {}", record.value());
            log.error(Arrays.toString(e.getStackTrace()));
        }
    }

    public void processMessage(Map<String, Object> kafkaMessage) {
    }
}