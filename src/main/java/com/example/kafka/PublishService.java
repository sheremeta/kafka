package com.example.kafka;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;

public interface PublishService {

    void publish(Payment payment);

    Map<MetricName, ? extends Metric> metrics();

    int processed();
}
