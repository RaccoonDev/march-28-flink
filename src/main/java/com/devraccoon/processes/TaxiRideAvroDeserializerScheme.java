package com.devraccoon.processes;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.TaxiRide;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TaxiRideAvroDeserializerScheme implements KafkaRecordDeserializationSchema<TaxiRide> {
    transient private KafkaAvroDeserializer deserializer;
    private final String schemaRegistryUrl;
    private final String topic;
    transient ObjectMapper objectMapper;

    public TaxiRideAvroDeserializerScheme(String schemaRegistryUrl, String topic) {
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
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }


    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<TaxiRide> collector) throws IOException {
        GenericRecord r = (GenericRecord) deserializer.deserialize(topic, consumerRecord.value());
        String schemaClassName = r.getSchema().getName();
        long rideId = (long) r.get("rideId");
        long taxiId = (long) r.get("taxiId");
        long driverId = (long) r.get("driverId");
        long passengerCnt = (long)r.get("passengerCnt");
        long eventTime = (long) r.get("eventTime");
        boolean start = (boolean) r.get("isStart");
        TaxiRide taxiRide = new TaxiRide(rideId, start, passengerCnt, taxiId, driverId, eventTime);

        collector.collect(taxiRide);
    }

    @Override
    public TypeInformation<TaxiRide> getProducedType() {
        return TypeExtractor.getForClass(TaxiRide.class);
    }
}
