package com.fit.iuh.keycloak_event_publisher;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaEventPublisher {

    private static final Logger log = Logger.getLogger(KafkaEventPublisher.class.getName());

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final ObjectMapper mapper;

    public KafkaEventPublisher(KafkaProducer<String, String> producer, String topic, ObjectMapper mapper) {
        this.producer = producer;
        this.topic = topic;
        this.mapper = mapper;
    }

    public void publish(String key, Map<String, Object> payload) {
        try {
            String message = mapper.writeValueAsString(payload);
            producer.send(new ProducerRecord<>(topic, key, message));
        } catch (Exception e) {
            log.severe("Kafka publish error: " + e.getMessage());
        }
    }

    public void flush() {
        producer.flush();
    }
}
