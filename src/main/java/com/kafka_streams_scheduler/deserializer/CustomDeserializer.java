package com.kafka_streams_scheduler.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka_streams_scheduler.dto.DataDTO;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CustomDeserializer implements Deserializer<DataDTO> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map configs, boolean isKey) {
    }

    @Override
    public DataDTO deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                System.out.println("Null received at deserializing");
                return null;
            }
            System.out.println("Deserializing...");
            return objectMapper.readValue(new String(data, "UTF-8"),
                    DataDTO.class);
        } catch (Exception e) {
            throw new SerializationException(
                    "Error when deserializing byte[] to DataDto");
        }
    }

    @Override
    public void close() {
    }
}
