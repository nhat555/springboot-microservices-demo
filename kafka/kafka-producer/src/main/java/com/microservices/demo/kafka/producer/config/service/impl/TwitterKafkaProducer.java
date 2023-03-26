package com.microservices.demo.kafka.producer.config.service.impl;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.config.service.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import javax.annotation.PreDestroy;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaProducer.class);

    private final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String topic, Long key, TwitterAvroModel message) {
        LOG.info("Sending message to topic: {}, key: {}, message: {}", topic, key, message);
        ProducerRecord<Long, TwitterAvroModel> record = new ProducerRecord<>(topic, key, message);
        CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture = kafkaTemplate.send(record);
        kafkaResultFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                LOG.error("Error sending message to topic: {}, key: {}, message: {}", topic, key, message, ex);
            } else {
                RecordMetadata metadata = result.getRecordMetadata();
                LOG.debug("Received new metadata. Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                        metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
            }
        });
    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            LOG.info("Closing Kafka Producer");
            kafkaTemplate.destroy();
        }
    }
}
