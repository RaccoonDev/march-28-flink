package com.devraccoon;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.params.KafkaParameters;
import com.devraccoon.params.OutputParams;
import com.devraccoon.processes.DetectOfflineEvent;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;

public class CountOfflineJob {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        KafkaParameters kafkaParameters = KafkaParameters.fromParamTool(params);
        OutputParams outputParams = OutputParams.fromParamTool(params);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamSetup.setupRestartsAndCheckpoints(env, outputParams.getCheckpointsPath());

        DataStream<PlayerEvent> playerEvents = PlayerEventsSource.getSource(env, kafkaParameters);

        processCountOffline(playerEvents, outputParams);

        env.execute("Battle Net Engagement Rate Job");

    }

    private static void processCountOffline(DataStream<PlayerEvent> playerEvents, OutputParams outputParams) {

        FileSink<String> fileSink = FileSink.forRowFormat(
                new Path(outputParams.getOutputPath() + "/countOfUsersWithoutOfflineEvents"),
                new SimpleStringEncoder<String>("UTF-8")
        ).build();

        playerEvents
                .keyBy(PlayerEvent::getPlayerId)
                .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)))
                .process(new DetectOfflineEvent())
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)))
                .apply(new AllWindowFunction<Integer, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Integer> iterable, Collector<String> collector) throws Exception {
                        long count = StreamSupport.stream(iterable.spliterator(), false).count();
                        String message = String.format("Window: [%s]; Number of people without offline event: %d", timeWindow.toString(), count);
                        collector.collect(message);
                    }
                })
                .sinkTo(fileSink);
    }
}

