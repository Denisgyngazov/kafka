package org.example.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class Sender {
    private final KafkaTemplate<Integer, String> kafkaTemplate;

    public void send(int key, String message) {
        kafkaTemplate.send("SPRING-DEMO", key, message);
    }

    public void send(String message) {
        final CompletableFuture<SendResult<Integer, String>> future = kafkaTemplate.sendDefault(message);
        // можно добавить .get(10, SECONDS) - синхронная отправка ждем результат 10 секунд
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                handleError(ex);
            } else {
                handleSuccess(result);
            }
        });
    }

    private void handleSuccess(SendResult<Integer, String> result) {
        RecordMetadata metadata = result.getRecordMetadata();
        log.info("Сообщение успешно отправлено. Тема: {}, Раздел: {}, Смещение: {}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
        );
    }

    private void handleError(Throwable ex) {
        log.info("Ошибка отправки сообщения в Kafka: {}", ex.getMessage());
    }
}
