package com.devraccoon;

import com.devraccoon.models.PlayerEvent;
import com.devraccoon.params.KafkaParameters;
import com.devraccoon.processes.PlayerEventAvroDeserializerScheme;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

public class PlayerEventsSource {
    public static DataStream<PlayerEvent> getSource(StreamExecutionEnvironment env, KafkaParameters params) {
        KafkaSource<PlayerEvent> kafkaSource = KafkaSource.<PlayerEvent>builder()
                .setBootstrapServers(params.getBootstrapServers())
                .setTopics(params.getTopic())
                .setGroupId(params.getGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new PlayerEventAvroDeserializerScheme(params.getSchemaRegistryUrl(), params.getTopic()))
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

        return env.fromSource(
                kafkaSource,
                watermarkStrategy,
                "player-events");
    }
}
