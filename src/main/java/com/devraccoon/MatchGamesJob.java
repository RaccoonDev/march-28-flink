package com.devraccoon;

import com.devraccoon.models.GameFound;
import com.devraccoon.models.GameType;
import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.PlayerLookingForGameEvent;
import com.devraccoon.params.KafkaParameters;
import com.devraccoon.params.OutputParams;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
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

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineFormat(), new Path("./data/maps/"))
                .monitorContinuously(Duration.ofSeconds(2))
                .build();

        FileSource<String> fileSourceTwo = FileSource.forRecordStreamFormat(new TextLineFormat(), new Path("./data/maps_v2/"))
                .monitorContinuously(Duration.ofSeconds(2))
                .build();

        DataStream<String> mapNames_one = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "Source of Map Names 1");
        DataStream<String> mapNames_two = env.fromSource(fileSourceTwo, WatermarkStrategy.noWatermarks(), "Source of Map Names 2");

        DataStream<String> mapNames = mapNames_one.union(mapNames_two);

        DataStream<PlayerLookingForGameEvent> playerLookingForGameEventDataStream = PlayerEventsSource.getSource(env, kafkaParameters)
                .flatMap(new FlatMapFunction<PlayerEvent, PlayerLookingForGameEvent>() {
                    @Override
                    public void flatMap(PlayerEvent value, Collector<PlayerLookingForGameEvent> out) throws Exception {
                        if (value instanceof PlayerLookingForGameEvent) {
                            out.collect((PlayerLookingForGameEvent) value);
                        }
                    }
                });

        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>(
                "mapNamesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        BroadcastStream<String> broadcastStreamOfMapNames = mapNames.broadcast(mapStateDescriptor);

        playerLookingForGameEventDataStream
                .keyBy(e -> e.getGameType().getTotalNumberOfPlayers())
                .connect(broadcastStreamOfMapNames)
                .process(new FindGameMatch())
                .uid("find-game-process")
                .print();

        env.execute("Matching Players to Games");
    }
}


class FindGameMatch extends KeyedBroadcastProcessFunction<Integer, PlayerLookingForGameEvent, String, GameFound> {

    private transient MapState<Long, Set<UUID>> stateEvents;

    private transient MapStateDescriptor<String, String> mapStateDescriptor;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.stateEvents = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "playersQueue",
                BasicTypeInfo.LONG_TYPE_INFO,
                TypeInformation.of(new TypeHint<Set<UUID>>() {})
        ));

        this.mapStateDescriptor = new MapStateDescriptor<>(
                "mapNamesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
    }

    @Override
    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<Integer, PlayerLookingForGameEvent, String, GameFound>.OnTimerContext ctx, Collector<GameFound> out) throws Exception {
        if (stateEvents.contains(timestamp)) {
            stateEvents.remove(timestamp);
        }
    }

    private static Random rand = new Random();

    @Override
    public void processElement(
            PlayerLookingForGameEvent value,
            KeyedBroadcastProcessFunction<Integer, PlayerLookingForGameEvent, String, GameFound>.ReadOnlyContext ctx,
            Collector<GameFound> out) throws Exception {
        Set<UUID> playerIds = StreamSupport.stream(stateEvents.values().spliterator(), false)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        playerIds.add(value.getPlayerId());

        if (playerIds.size() >= ctx.getCurrentKey()) {

            String mapName = getRandomMapName(ctx);

            out.collect(new GameFound(playerIds, GameType.forValue(ctx.getCurrentKey()).get(), mapName));
            stateEvents.clear();
        } else {
            long timerValue = value.getEventTime().plusSeconds(1).toEpochMilli();
            Set<UUID> players = Optional.ofNullable(stateEvents.get(timerValue)).orElse(new HashSet<>());
            players.add(value.getPlayerId());
            stateEvents.put(timerValue, players);

            ctx.timerService().registerEventTimeTimer(timerValue);
        }
    }

    private String getRandomMapName(KeyedBroadcastProcessFunction<Integer, PlayerLookingForGameEvent, String, GameFound>.ReadOnlyContext ctx) throws Exception {
        List<String> mapNames = StreamSupport
                .stream(ctx.getBroadcastState(mapStateDescriptor).immutableEntries().spliterator(), false)
                .map(Map.Entry::getValue).collect(Collectors.toList());
        return mapNames.isEmpty() ? "NO MAPS YET" : mapNames.get(rand.nextInt(mapNames.size()));
    }

    @Override
    public void processBroadcastElement(
            String value,
            KeyedBroadcastProcessFunction<Integer, PlayerLookingForGameEvent, String, GameFound>.Context ctx,
            Collector<GameFound> out) throws Exception {
        ctx.getBroadcastState(mapStateDescriptor).put(value, value);
    }
}
