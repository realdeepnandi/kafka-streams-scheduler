package com.kafka_streams_scheduler;

import com.kafka_streams_scheduler.dto.DataDTO;
import com.kafka_streams_scheduler.serde.CustomSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Component
public class KafkaStreamsScheduler {

    private static final Serde<String> KEY_SERDE = Serdes.String();
    private static final Serde<DataDTO> VALUE_SERDE = new JsonSerde<>(
            DataDTO.class);

    private static final Deserializer<String> STRING_DE = new StringDeserializer();
    private static final Deserializer<DataDTO> VALUE_JSON_DE = new JsonDeserializer<>(
            DataDTO.class);

    @PostConstruct
    public static void initKafkaStreams() {
        try {

            Properties properties = new Properties();
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG,
                    "demo-kafka-streams");
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                    Serdes.String().getClass().getName());
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                    CustomSerde.class);
            StreamsBuilder builder = new StreamsBuilder();

            StoreBuilder<KeyValueStore<String, DataDTO>> storeStoreBuilder = Stores
                    .keyValueStoreBuilder(
                            Stores.persistentKeyValueStore("kafka_table"),
                            KEY_SERDE, VALUE_SERDE);
            Topology topology = builder.build();
            topology.addSource("Source", STRING_DE, VALUE_JSON_DE,
                    "kafka.demo")
                    .addProcessor("Process",
                            () -> new CustomProcessor("kafka_table"), "Source")
                    .addStateStore(storeStoreBuilder, "Process")
                    .addSink("Sink", "kafka.demo.output", "Process");
        KafkaStreams kafkaStreams = new KafkaStreams(topology,
                    properties);
                kafkaStreams.start();
            
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
