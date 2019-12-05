package com.example.kafka;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaApplicationTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaApplicationTests.class);
    private static final String TOPIC_NAME = "messages";
    private static final int MESSAGE_COUNT = 100000;

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer();

    @BeforeClass
    public static void setup() {
        System.setProperty("TOPIC", TOPIC_NAME);
        System.setProperty("ADDRESS", kafka.getBootstrapServers());
    }

    @Autowired
    private PublishService publishService;

    @Test
    public void itsWorks() throws InterruptedException {
        int count = MESSAGE_COUNT;

        while (count-- > 0) {
            publishService.publish(new Payment().setId(UUID.randomUUID().toString()));
        }

        while (publishService.processed() < MESSAGE_COUNT) {
            TimeUnit.SECONDS.sleep(1);
        }

        publishService.metrics().values().forEach(m -> LOGGER.info("{} : {}", m.metricName(), m.metricValue()));
    }
}
