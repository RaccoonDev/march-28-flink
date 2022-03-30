package com.devraccoon.processes;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.PlayerEventType;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.UUID;
import java.util.stream.StreamSupport;

public class DetectOfflineEvent extends ProcessWindowFunction<PlayerEvent, Integer, UUID, TimeWindow> {

    @Override
    public void process(
            UUID uuid,
            ProcessWindowFunction<PlayerEvent, Integer, UUID, TimeWindow>.Context context,
            Iterable<PlayerEvent> iterable,
            Collector<Integer> collector) {
        if (StreamSupport.stream(iterable.spliterator(), false)
                .anyMatch(e -> e.getEventType() == PlayerEventType.OFFLINE)) {
            collector.collect(1);
        }
    }

}
