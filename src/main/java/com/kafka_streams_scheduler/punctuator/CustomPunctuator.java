package com.kafka_streams_scheduler.punctuator;

import com.kafka_streams_scheduler.dto.DataDTO;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class CustomPunctuator implements Punctuator {

    private final ProcessorContext processorContext;

    private final KeyValueStore<String, DataDTO> stateStore;

    public CustomPunctuator(ProcessorContext processorContext,
            KeyValueStore<String, DataDTO> stateStore) {
        this.processorContext = processorContext;
        this.stateStore = stateStore;
    }

    @Override
    public void punctuate(long timestamp) {
        while (stateStore.all().hasNext()) {
            KeyValue<String, DataDTO> stringStringKeyValue = stateStore.all()
                    .next();
            stateStore.delete(stringStringKeyValue.key);
            Record<String, DataDTO> record = new Record<>(
                    stringStringKeyValue.key, stringStringKeyValue.value,
                    timestamp);
            processorContext.forward(record);
        }

    }
}
