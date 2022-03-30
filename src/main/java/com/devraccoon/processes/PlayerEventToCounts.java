package com.devraccoon.processes;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.PlayerOfflineEvent;
import com.devraccoon.models.PlayerOnlineEvent;
import com.devraccoon.models.PlayerRegisteredEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

    private transient MapState<Long, Tuple2<Long, Long>> stateOutput;

    @Override
    public void open(Configuration parameters) {
        this.counterRegistrations = getRuntimeContext().getMetricGroup().counter("registrations_total");
        this.counterOnline = getRuntimeContext().getMetricGroup().counter("online_total");

        this.stateOutput = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>(
                        "delayedOutputs",
                        BasicTypeInfo.LONG_TYPE_INFO,
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})
                        ));
    }

    @Override
    public void processElement(
            PlayerEvent playerEvent,
            KeyedProcessFunction<Integer, PlayerEvent, Tuple2<Long, Long>>.Context context,
            Collector<Tuple2<Long, Long>> collector) throws Exception {

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

        long timerValue = playerEvent.getEventTime().plusSeconds(20).toEpochMilli();
        stateOutput.put(timerValue, Tuple2.of(registrations, online));

        context.timerService().registerEventTimeTimer(timerValue);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<Integer, PlayerEvent, Tuple2<Long, Long>>.OnTimerContext ctx,
            Collector<Tuple2<Long, Long>> out) throws Exception {
        Tuple2<Long, Long> outputElement = stateOutput.get(timestamp);
        out.collect(outputElement);
        stateOutput.remove(timestamp);
    }
}
