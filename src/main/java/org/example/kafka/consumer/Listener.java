package org.example.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Listener {
    @KafkaListener(id = "listen1", topics = "SPRING-DEMO")
    public void listenSpringDemoTopic(String message) {
       log.info("Listening spring demo topic: {}", message);
    }

    @KafkaListener(id = "listen2", topics = "DEFAULT-TOPIC")
    public void listenDefaultTopic(String message) {
        log.info("Listening default topic: {}", message);
    }
}
