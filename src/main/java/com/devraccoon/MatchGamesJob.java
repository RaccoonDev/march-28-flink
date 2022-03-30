package com.devraccoon;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.PlayerLookingForGameEvent;
import com.devraccoon.params.KafkaParameters;
import com.devraccoon.params.OutputParams;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
                        if(value instanceof PlayerLookingForGameEvent) {
                            out.collect((PlayerLookingForGameEvent) value);
                        }
                    }
                });

        playerLookingForGameEventDataStream.print();


        env.execute("Matching Players to Games");
    }
}
