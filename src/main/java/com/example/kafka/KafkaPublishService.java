package com.example.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class KafkaPublishService implements PublishService {

    private static ObjectMapper objectMapper;

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final AtomicInteger processed = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();

    @Value(value = "${kafka.topic}")
    private String topicName;

    static {
        objectMapper = new ObjectMapper();
    }

    public KafkaPublishService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(Payment payment) {
        try {
            kafkaTemplate.send(topicName, payment.getId(), objectMapper.writeValueAsString(payment)).addCallback(stringStringSendResult -> processed.incrementAndGet(), throwable -> failed.incrementAndGet());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return kafkaTemplate.metrics();
    }

    @Override
    public int processed() {
        return processed.get() + failed.get();
    }
}

