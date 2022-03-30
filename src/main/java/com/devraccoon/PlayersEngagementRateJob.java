package com.devraccoon;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.processes.DetectOfflineEvent;
import com.devraccoon.processes.PlayerEventAvroDeserializerScheme;
import com.devraccoon.processes.PlayerEventToCounts;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.StreamSupport;

/*
  Pass as Program Arguments when submitting this job in Apache Flink Dashboard:
  --schema-registry-url http://schema-registry:8081 --bootstrap-servers kafka:29092 --output-path s3://outputs
 */

public class PlayersEngagementRateJob {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        final String topic = "battlenet.server.events.v1";
        final String schemaRegistryUrl = Optional.ofNullable(params.get("schema-registry-url")).orElse("http://localhost:8081");
        final String bootstrapServers = Optional.ofNullable(params.get("bootstrap-servers")).orElse("localhost:9092");
        final String pwd = System.getenv().get("PWD");
        final String outputPath = params.get("output-path") != null ? params.get("output-path") : "file://" + pwd + "/out/";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(Duration.ofSeconds(10).toMillis());
        env.getCheckpointConfig().setCheckpointStorage(outputPath + "/checkpoints");

        KafkaSource<PlayerEvent> kafkaSource = KafkaSource.<PlayerEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("battle-net-events-processor-group-1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new PlayerEventAvroDeserializerScheme(schemaRegistryUrl, topic))
                .build();

        WatermarkStrategy<PlayerEvent> watermarkStrategy = WatermarkStrategy
                .<PlayerEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<PlayerEvent>() {
                    @Override
                    public long extractTimestamp(PlayerEvent playerEvent, long previouslyAssignedTimestampToThisEvent) {
                        return playerEvent.getEventTime().toEpochMilli();
                    }
                })
                .withIdleness(Duration.ofSeconds(2));

        DataStream<PlayerEvent> playerEvents = env.fromSource(kafkaSource, watermarkStrategy, "player-events");

        KeyedStream<PlayerEvent, UUID> playerEventUUIDKeyedStream = playerEvents.keyBy(PlayerEvent::getPlayerId);

        WindowedStream<PlayerEvent, UUID, TimeWindow> window = playerEventUUIDKeyedStream.window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)));

        DataStream<Integer> process = window.process(new DetectOfflineEvent());

        DataStream<String> countOfUsersWithoutOfflineEvents = process
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)))
                .apply(new AllWindowFunction<Integer, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Integer> iterable, Collector<String> collector) {
                        long count = StreamSupport.stream(iterable.spliterator(), false).count();
                        String message = String.format("Window: [%s]; Number of people without offline event: %d", timeWindow.toString(), count);
                        collector.collect(message);
                    }
                });

        FileSink<String> fileSink = FileSink.forRowFormat(
                new Path(outputPath + "/countOfUsersWithoutOfflineEvents"),
                new SimpleStringEncoder<String>("UTF-8")
        ).build();

        countOfUsersWithoutOfflineEvents.sinkTo(fileSink);

        processPlayersEngagement(playerEvents, outputPath);

        env.execute("Battle Net Engagement Rate Job");
    }

    private static void processPlayersEngagement(DataStream<PlayerEvent> playerEvents, String outputPath) {

        DataStream<Tuple2<Long, Long>> onlineAndRegistrationCountsStream = playerEvents.process(new PlayerEventToCounts());

        DataStream<String> playersEngagementRatesStream = onlineAndRegistrationCountsStream.map((MapFunction<Tuple2<Long, Long>, String>) pair -> {
            Long registrations = pair.f0;
            Long online = pair.f1;
            return String.format(
                    "Number of registered players: %d; Online players: %d; Rate of online users: %.2f",
                    registrations, online, ((double) online / (double) registrations) * 100.0);
        });

        playersEngagementRatesStream.writeAsText(outputPath + "/engagementRates.txt", FileSystem.WriteMode.OVERWRITE);
    }
}
