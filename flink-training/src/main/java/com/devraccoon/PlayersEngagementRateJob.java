package com.devraccoon;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.PlayerEventType;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PlayersEngagementRateJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setMaxParallelism(1);

        final String topic = "battlenet.server.events.v1";
        final String schemaRegistryUrl = "http://localhost:8081";
        KafkaSource<PlayerEvent> kafkaSource = KafkaSource.<PlayerEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("battle-net-events-processor-group-1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new PlayerEventAvroDeserializerScheme(schemaRegistryUrl, topic))
                .build();

        DataStream<PlayerEvent> playerEvents = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(), "player-events");

        DataStream<Tuple2<Long, Long>> onlineAndRegistrationCountsStream = playerEvents.process(new ProcessFunction<PlayerEvent, Tuple2<Long, Long>>() {

            private long registrations;
            private long online;

            @Override
            public void processElement(
                    PlayerEvent playerEvent,
                    ProcessFunction<PlayerEvent, Tuple2<Long, Long>>.Context context,
                    Collector<Tuple2<Long, Long>> collector) throws Exception {

                switch (playerEvent.getEventType()) {
                    case REGISTERED:
                        registrations++;
                        break;
                    case ONLINE:
                        online++;
                        break;
                    case OFFLINE:
                        online--;
                }

                collector.collect(Tuple2.of(registrations, online));
            }
        });

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

        playersEngagementRatesStream.print();

        env.execute("Battle Net Engagement Rate Job");
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

        switch(schemaClassName) {
            case "PlayerRegistered":
                collector.collect(new PlayerEvent(eventTime, PlayerEventType.REGISTERED));
                break;
            case "PlayerOnline":
                collector.collect(new PlayerEvent(eventTime, PlayerEventType.ONLINE));
                break;
            case "PlayerOffline":
                collector.collect(new PlayerEvent(eventTime, PlayerEventType.OFFLINE));
                break;
        }
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