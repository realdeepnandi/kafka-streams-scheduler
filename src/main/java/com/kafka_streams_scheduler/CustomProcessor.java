package com.kafka_streams_scheduler;

import com.kafka_streams_scheduler.dto.DataDTO;
import com.kafka_streams_scheduler.punctuator.CustomPunctuator;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class CustomProcessor
        implements Processor<String, DataDTO, String, DataDTO> {
    private KeyValueStore<String, DataDTO> stateStore;

    private final String stateStoreName;

    public CustomProcessor(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        stateStore = (KeyValueStore<String, DataDTO>) context.getStateStore(
                stateStoreName);

        context.schedule(Duration.ofSeconds(10),
                PunctuationType.WALL_CLOCK_TIME,
                new CustomPunctuator(context, stateStore));
    }

    @Override
    public void process(Record<String, DataDTO> record) {
        stateStore.put(record.key(), record.value());
    }

    @Override
    public void close() {
    }
}
