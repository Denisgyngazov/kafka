package org.example.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Configuration
@ComponentScan(basePackages = "org.example.kafka.producer")
@Slf4j
public class ProducerConfig {
    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        final DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(senderProperties());
        // Можно просто вернуть DefaultKafkaProducerFactory
        producerFactory.addListener(new ProducerFactory.Listener<Integer, String>() {
            @Override
            public void producerAdded(String id, Producer<Integer, String> producer) {
                log.info("Producer {} added", id);
                ProducerFactory.Listener.super.producerAdded(id, producer);
            }

            @Override
            public void producerRemoved(String id, Producer<Integer, String> producer) {
                log.info("Producer {} removed", id);
                ProducerFactory.Listener.super.producerRemoved(id, producer);
            }
        });
        return producerFactory;
    }

    @Bean
    public KafkaTemplate<Integer, String> kafkaTemplate(ProducerFactory<Integer, String> producerFactory) {
        final KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
// Единный сценарий для всех отправок через этот KafkaTemplate
//        kafkaTemplate.setProducerListener(new ProducerListener<Integer, String>() {
//            @Override
//            public void onSuccess(ProducerRecord<Integer, String> producerRecord, RecordMetadata recordMetadata) {
//                ProducerListener.super.onSuccess(producerRecord, recordMetadata);
//            }
//
//            @Override
//            public void onError(ProducerRecord<Integer, String> producerRecord, RecordMetadata recordMetadata, Exception exception) {
//                ProducerListener.super.onError(producerRecord, recordMetadata, exception);
//            }
//        });
        // Для использования метода sendDefault
        kafkaTemplate.setDefaultTopic("DEFAULT-TOPIC");
        return kafkaTemplate;
    }

    private Map<String, Object> senderProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(LINGER_MS_CONFIG, 10);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

}
