package org.example.kafka.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class AdminService {
    public final KafkaAdmin kafkaAdmin;

    public void createNewTopic(String topicName, int partitions, int replicationFactor) {
        log.info("Creating or modify topic {}", topicName);
        kafkaAdmin.createOrModifyTopics(
                TopicBuilder.name(topicName)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build());
    }

    public String getTopicInfo(String topicName) {
        try {
            return kafkaAdmin.describeTopics(topicName).toString();
        } catch (KafkaException e) {
            return e.getMessage();
        }
    }
}
