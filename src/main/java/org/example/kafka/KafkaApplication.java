package org.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.example.kafka.config.ConsumerConfig;
import org.example.kafka.config.ProducerConfig;
import org.example.kafka.producer.Sender;
import org.example.kafka.services.AdminService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@SpringBootApplication
@Slf4j
public class KafkaApplication {

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(KafkaApplication.class, args);

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
                ConsumerConfig.class,
                ProducerConfig.class
        );

        createNewTopic(
                "DEFAULT_TOPIC",
                context.getBean(AdminService.class),
                1,
                1
        );

        final Sender sender = context.getBean(Sender.class);

        sender.send("Hello Kafka");

        Thread.sleep(100000);
    }


    public static void createNewTopic(String topicName, AdminService service, int partitions, int replicationFactor) {
        service.createNewTopic(
                topicName,
                partitions,
                replicationFactor
        );
//        log.info(service.getTopicInfo(topicName));
    }
}
