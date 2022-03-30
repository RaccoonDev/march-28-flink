package com.devraccoon;

import com.devraccoon.models.GameFound;
import com.devraccoon.models.GameType;
import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.PlayerLookingForGameEvent;
import com.devraccoon.params.KafkaParameters;
import com.devraccoon.params.OutputParams;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.Array;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MatchGamesJob {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        KafkaParameters kafkaParameters = KafkaParameters.fromParamTool(params);
        OutputParams outputParams = OutputParams.fromParamTool(params);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamSetup.setupRestartsAndCheckpoints(env, outputParams.getCheckpointsPath());

        DataStream<PlayerLookingForGameEvent> playerLookingForGameEventDataStream = PlayerEventsSource.getSource(env, kafkaParameters)
                .flatMap(new FlatMapFunction<PlayerEvent, PlayerLookingForGameEvent>() {
                    @Override
                    public void flatMap(PlayerEvent value, Collector<PlayerLookingForGameEvent> out) throws Exception {
                        if (value instanceof PlayerLookingForGameEvent) {
                            out.collect((PlayerLookingForGameEvent) value);
                        }
                    }
                });

        playerLookingForGameEventDataStream
                .keyBy(e -> e.getGameType().getTotalNumberOfPlayers())
                .process(new FindGameMatch())
                .print();

        env.execute("Matching Players to Games");
    }
}


class FindGameMatch extends KeyedProcessFunction<Integer, PlayerLookingForGameEvent, GameFound> {

    private transient MapState<Long, Set<UUID>> stateEvents;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.stateEvents = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "playersQueue",
                BasicTypeInfo.LONG_TYPE_INFO,
                TypeInformation.of(new TypeHint<Set<UUID>>() {})
        ));
    }

    @Override
    public void processElement(
            PlayerLookingForGameEvent value,
            KeyedProcessFunction<Integer, PlayerLookingForGameEvent, GameFound>.Context ctx,
            Collector<GameFound> out) throws Exception {

        Set<UUID> playerIds = StreamSupport.stream(stateEvents.values().spliterator(), false)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        playerIds.add(value.getPlayerId());

        if (playerIds.size() >= ctx.getCurrentKey()) {
            out.collect(new GameFound(playerIds, GameType.forValue(ctx.getCurrentKey()).get()));
            stateEvents.clear();
        } else {
            long timerValue = value.getEventTime().plusSeconds(1).toEpochMilli();
            Set<UUID> players = Optional.ofNullable(stateEvents.get(timerValue)).orElse(new HashSet<>());
            players.add(value.getPlayerId());
            stateEvents.put(timerValue, players);

            ctx.timerService().registerEventTimeTimer(timerValue);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<Integer, PlayerLookingForGameEvent, GameFound>.OnTimerContext ctx,
            Collector<GameFound> out) throws Exception {
        if (stateEvents.contains(timestamp)) {
            stateEvents.remove(timestamp);
        }
    }
}
