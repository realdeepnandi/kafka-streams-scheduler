package com.kafka_streams_scheduler;

import com.kafka_streams_scheduler.dto.DataDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "com.kafka_streams_scheduler.deserializer.CustomDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");

        KafkaConsumer<String, DataDTO> kafkaConsumer = new KafkaConsumer<>(
                properties);
        kafkaConsumer.subscribe(Collections.singletonList("kafka.demo.output"));

        while (true) {
            ConsumerRecords<String, DataDTO> messages = kafkaConsumer
                    .poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, DataDTO> message : messages) {
                System.out.println("message - " + message);
            }
        }

    }
}
