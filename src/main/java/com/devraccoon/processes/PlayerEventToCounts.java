package com.devraccoon.processes;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.PlayerOfflineEvent;
import com.devraccoon.models.PlayerOnlineEvent;
import com.devraccoon.models.PlayerRegisteredEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PlayerEventToCounts extends KeyedProcessFunction<Integer, PlayerEvent, Tuple2<Long, Long>> {

    private long registrations;
    private long online;

    private transient Counter counterRegistrations;
    private transient Counter counterOnline;

    @Override
    public void open(Configuration parameters) {
        this.counterRegistrations = getRuntimeContext().getMetricGroup().counter("registrations_total");
        this.counterOnline = getRuntimeContext().getMetricGroup().counter("online_total");
    }

    @Override
    public void processElement(
            PlayerEvent playerEvent,
            KeyedProcessFunction<Integer, PlayerEvent, Tuple2<Long, Long>>.Context context,
            Collector<Tuple2<Long, Long>> collector) {

        if(playerEvent instanceof PlayerRegisteredEvent) {
            registrations++;
            counterRegistrations.inc();
        } else if(playerEvent instanceof PlayerOnlineEvent) {
            online++;
            counterOnline.inc();
        } else if (playerEvent instanceof PlayerOfflineEvent) {
            online--;
            counterOnline.dec();
        }

        collector.collect(Tuple2.of(registrations, online));
    }
}
