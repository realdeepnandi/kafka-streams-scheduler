package com.kafka_streams_scheduler.serde;

import com.kafka_streams_scheduler.deserializer.CustomDeserializer;
import com.kafka_streams_scheduler.dto.DataDTO;
import com.kafka_streams_scheduler.serializer.CustomSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomSerde implements Serde<DataDTO> {
    private CustomDeserializer deserializer = new CustomDeserializer();
    private CustomSerializer serializer = new CustomSerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<DataDTO> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<DataDTO> deserializer() {
        return deserializer;
    }
}
