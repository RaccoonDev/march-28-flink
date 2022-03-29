package com.devraccoon;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.PlayerEventType;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.StreamSupport;

/*
  For flink cluster:
  --schema-registry-url http://schema-registry:8081 --bootstrap-servers broker:29092 --checkpoint-path s3://outputs/checkpoints

  For intellij:
  --schema-registry-url http://localhost:8081 --bootstrap-servers localhost:9092
 */
public class PlayersEngagementRateJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        final String topic = "battlenet.server.events.v1";
        final String schemaRegistryUrl = Optional.ofNullable(params.get("schema-registry-url")).orElse("http://localhost:8081");
        final String bootstrapServers = Optional.ofNullable(params.get("bootstrap-servers")).orElse("localhost:9092");
        final String checkpointPath = params.getRequired("checkpoint-path");

        env.enableCheckpointing(Duration.ofSeconds(10).toMillis());
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);

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

        DataStream<PlayerEvent> playerEvents = env.fromSource(
                kafkaSource,
                watermarkStrategy,
                "player-events");

        KeyedStream<PlayerEvent, UUID> playerEventUUIDKeyedStream = playerEvents.keyBy(PlayerEvent::getPlayerId);
        WindowedStream<PlayerEvent, UUID, TimeWindow> window = playerEventUUIDKeyedStream.window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)));
        DataStream<Integer> process = window.process(new DetectOfflineEvent());
        DataStream<String> countOfUsersWithoutOfflineEvents = process
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)))
                .apply(new AllWindowFunction<Integer, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Integer> iterable, Collector<String> collector) throws Exception {
                        long count = StreamSupport.stream(iterable.spliterator(), false).count();
                        String message = String.format("Window: [%s]; Number of people without offline event: %d", timeWindow.toString(), count);
                        collector.collect(message);
                    }
                });

        FileSink<String> fileSink = FileSink.forRowFormat(
                        new Path("s3://outputs/countOfUsersWithoutOfflineEvents"),
                        new SimpleStringEncoder<String>("UTF-8")
                )
                .build();
        countOfUsersWithoutOfflineEvents.sinkTo(fileSink);

        processPlayersEngagement(playerEvents);

        env.execute("Battle Net Engagement Rate Job");
    }

    private static void processPlayersEngagement(DataStream<PlayerEvent> playerEvents) {
        DataStream<Tuple2<Long, Long>> onlineAndRegistrationCountsStream = playerEvents.process(new PlayerEventToCounts());
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

        playersEngagementRatesStream.writeAsText("s3://outputs/engagementrates.txt", FileSystem.WriteMode.OVERWRITE);
    }
}

class PlayerEventAvroDeserializerScheme implements KafkaRecordDeserializationSchema<PlayerEvent> {
    transient private KafkaAvroDeserializer deserializer;
    private String schemaRegistryUrl;
    private String topic;

    public PlayerEventAvroDeserializerScheme(String schemaRegistryUrl, String topic) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {

        Properties props = new Properties();
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
                schemaRegistryUrl,
                AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT
        );

        this.deserializer = new KafkaAvroDeserializer(schemaRegistryClient, typeCastConvert(props));
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<PlayerEvent> collector) throws IOException {
        GenericRecord r = (GenericRecord) deserializer.deserialize(topic, consumerRecord.value());
        String schemaClassName = r.getSchema().getName();

        Instant eventTime = Instant.ofEpochMilli((long) r.get("eventTime"));
        Optional<UUID> maybePlayerId = Optional.ofNullable(r.get("playerId")).map(Object::toString).map(UUID::fromString);

        maybePlayerId.ifPresent(playerId -> {
            switch (schemaClassName) {
                case "PlayerRegistered":
                    collector.collect(new PlayerEvent(eventTime, PlayerEventType.REGISTERED, playerId));
                    break;
                case "PlayerOnline":
                    collector.collect(new PlayerEvent(eventTime, PlayerEventType.ONLINE, playerId));
                    break;
                case "PlayerOffline":
                    collector.collect(new PlayerEvent(eventTime, PlayerEventType.OFFLINE, playerId));
                    break;
            }
        });
    }

    @Override
    public TypeInformation<PlayerEvent> getProducedType() {
        return TypeExtractor.getForClass(PlayerEvent.class);
    }

    @SuppressWarnings("unchecked")
    private static HashMap<String, ?> typeCastConvert(Properties prop) {
        Map step1 = prop;
        Map<String, ?> step2 = (Map<String, ?>) step1;
        return new HashMap<>(step2);
    }


}

class PlayerEventToCounts extends ProcessFunction<PlayerEvent, Tuple2<Long, Long>> {

    private long registrations;
    private long online;

    private transient Counter counterRegistrations;
    private transient Counter counterOnline;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.counterRegistrations = getRuntimeContext().getMetricGroup().counter("registrations_total");
        this.counterOnline = getRuntimeContext().getMetricGroup().counter("online_total");
    }

    @Override
    public void processElement(
            PlayerEvent playerEvent,
            ProcessFunction<PlayerEvent, Tuple2<Long, Long>>.Context context,
            Collector<Tuple2<Long, Long>> collector) throws Exception {

        switch (playerEvent.getEventType()) {
            case REGISTERED:
                registrations++;
                counterRegistrations.inc();
                break;
            case ONLINE:
                online++;
                counterOnline.inc();
                break;
            case OFFLINE:
                online--;
                counterOnline.dec();
        }

        collector.collect(Tuple2.of(registrations, online));
    }
}


class DetectOfflineEvent extends ProcessWindowFunction<PlayerEvent, Integer, UUID, TimeWindow> {
    @Override
    public void process(
            UUID uuid,
            ProcessWindowFunction<PlayerEvent, Integer, UUID, TimeWindow>.Context context,
            Iterable<PlayerEvent> iterable,
            Collector<Integer> collector) throws Exception {
        if (StreamSupport.stream(iterable.spliterator(), false)
                .anyMatch(e -> e.getEventType() == PlayerEventType.OFFLINE)) {
            collector.collect(1);
        }
    }
}