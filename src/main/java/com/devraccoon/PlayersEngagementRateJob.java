package com.devraccoon;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.params.KafkaParameters;
import com.devraccoon.params.OutputParams;
import com.devraccoon.processes.PlayerEventToCounts;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

/*
  Pass as Program Arguments when submitting this job in Apache Flink Dashboard:
  --schema-registry-url http://schema-registry:8081 --bootstrap-servers kafka:29092 --output-path s3://outputs
 */

public class PlayersEngagementRateJob {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        KafkaParameters kafkaParameters = KafkaParameters.fromParamTool(params);
        OutputParams outputParams = OutputParams.fromParamTool(params);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamSetup.setupRestartsAndCheckpoints(env, outputParams.getCheckpointsPath());

        DataStream<PlayerEvent> playerEvents = PlayerEventsSource.getSource(env, kafkaParameters);

        processPlayersEngagement(playerEvents, outputParams);

        env.execute("Battle Net Engagement Rate Job");
    }

    private static void processPlayersEngagement(DataStream<PlayerEvent> playerEvents, OutputParams outputParams) {

        DataStream<Tuple2<Long, Long>> onlineAndRegistrationCountsStream = playerEvents
                .keyBy(k -> 0)
                .process(new PlayerEventToCounts())
                .uid("player-events-processor")
                .name("Player Events Processor");

        DataStream<String> playersEngagementRatesStream = onlineAndRegistrationCountsStream.map(new MapFunction<Tuple2<Long, Long>, String>() {
            @Override
            public String map(Tuple2<Long, Long> pair) throws Exception {
                Long registrations = pair.f0;
                Long online = pair.f1;
                return String.format(
                        "Number of registered players: %d; Online players: %d; Rate of online users: %.2f",
                        registrations, online, ((double) online / (double) registrations) * 100.0);
            }
        });

        FileSink<String> fileSink = FileSink.forRowFormat(
                        new Path(outputParams.getOutputPath() + "/longRides"),
                        new SimpleStringEncoder<String>("UTF-8")
                )
                .build();

        playersEngagementRatesStream
                .sinkTo(fileSink);
    }
}
