package ru.kafka.producer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

@Slf4j
public class KafkaConfig {

    public static Properties getProducerProps() {
        Properties props = new Properties();
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        log.info("Kafka bootstrap servers: {}", bootstrapServers);

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        return props;
    }

    public static void createTopicsIfMissing() {
        Properties properties = getProducerProps();
        String topicsEnv = System.getenv().getOrDefault("KAFKA_TOPICS", "drone_metrics,drone_positions,drone_delivery_statuses");
        List<String> topics = Arrays.stream(topicsEnv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();

        if (topics.isEmpty()) {
            log.warn("Переменная KAFKA_TOPICS не задана или пуста – топики не будут созданы");
            return;
        }

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();

            List<NewTopic> toCreate = topics.stream()
                    .filter(topic -> !existingTopics.contains(topic))
                    .map(t -> new NewTopic(t, 3, (short) 2))
                    .toList();

            if (!toCreate.isEmpty()) {
                adminClient.createTopics(toCreate);
                log.info("Созданы топики: {}", toCreate.stream().map(NewTopic::name).toList());
            }
        } catch (Exception e) {
            log.error("Ошибка при создании топиков: {}", e.getMessage(), e);
        }
    }
}
