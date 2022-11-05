package com.kafka_streams_scheduler;

import com.kafka_streams_scheduler.dto.DataDTO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class Producer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "com.kafka_streams_scheduler.serializer.CustomSerializer");

        KafkaProducer<String, DataDTO> kafkaProducer = new KafkaProducer<>(
                properties);

        DataDTO data = new DataDTO();
        data.setEntId("kafka.demo");
        data.setTime(10);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, DataDTO> kafkaRecord = new ProducerRecord<>(
                    "kafka.demo", UUID.randomUUID().toString(), data);

            kafkaProducer.send(kafkaRecord);
            Thread.sleep(1000);
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
