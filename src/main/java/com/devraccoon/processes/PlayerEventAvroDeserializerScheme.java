package com.devraccoon.processes;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.PlayerEventType;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.util.*;

public class PlayerEventAvroDeserializerScheme implements KafkaRecordDeserializationSchema<PlayerEvent> {

    transient private KafkaAvroDeserializer deserializer;
    private final String schemaRegistryUrl;
    private final String topic;

    public PlayerEventAvroDeserializerScheme(String schemaRegistryUrl, String topic) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
    }

    @SuppressWarnings("unchecked")
    private static HashMap<String, ?> typeCastConvert(Properties prop) {
        Map step1 = prop;
        Map<String, ?> step2 = (Map<String, ?>) step1;
        return new HashMap<>(step2);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {

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
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<PlayerEvent> collector) {
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

}
