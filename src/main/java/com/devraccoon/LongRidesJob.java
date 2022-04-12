package com.devraccoon;
import com.devraccoon.models.PlayerEvent;
import com.devraccoon.models.TaxiRide;
import com.devraccoon.processes.TaxiRideAvroDeserializerScheme;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
//import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class LongRidesJob {
    //private final SourceFunction<TaxiRide> source;
    //private final SinkFunction<Long> sink;

    /** Creates a job using the source and sink provided. */
    public LongRidesJob(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {

        //this.source = source;
        //this.sink = sink;
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setMaxParallelism(1);
        KafkaSource<TaxiRide> kafkaSource = KafkaSource.<TaxiRide>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("taxi-ride-topic1")
                .setGroupId("taxi-ride-consumer2")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new TaxiRideAvroDeserializerScheme("http://localhost:8081", "taxi-ride-topic1"))
                //.setDeserializer(new TaxiRideKafkaRecordDeserializationSchema())
                .build();
        // start the data generator


        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy1 =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        WatermarkStrategy<TaxiRide> watermarkStrategy = WatermarkStrategy
                .<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TaxiRide>() {

                    @Override
                    public long extractTimestamp(TaxiRide ride, long l) {
                        return ride.getEventTimeMillis();
                    }
                })
                .withIdleness(Duration.ofSeconds(2));

        DataStream<TaxiRide> rides = env.fromSource(kafkaSource, watermarkStrategy, "taxi-rides1");
        final DataStreamSource<TaxiRide> rides1 = env.fromElements(new TaxiRide(1, true, (short) 1, 1, 1, Instant.now().toEpochMilli()),
                new TaxiRide(2, true,  (short) 2, 2, 2, Instant.now().toEpochMilli()));
        //env.f
        //rides.print();
        // create the pipeline
        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.getRideId())
                .process(new AlertFunction()).print();

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }

    class TaxiRideKafkaRecordDeserializationSchema implements KafkaRecordDeserializationSchema<TaxiRide> {
        transient ObjectMapper objectMapper = new ObjectMapper();

        public TaxiRideKafkaRecordDeserializationSchema() {

        }

        @Override
        public void open(DeserializationSchema.InitializationContext context) throws Exception {
            objectMapper.registerModule(new JavaTimeModule());
            objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<TaxiRide> collector) throws IOException {
            System.out.println("record = " + consumerRecord.value());
            final TaxiRide taxiRide = objectMapper.readValue(consumerRecord.value(), TaxiRide.class);
            collector.collect(taxiRide);

        }

        @Override
        public TypeInformation getProducedType() {
            return TypeExtractor.getForClass(TaxiRide.class);
        }
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        LongRidesJob job =
                new LongRidesJob(null, new PrintSinkFunction<>());

        job.execute();
    }

    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        private ValueState<TaxiRide> rideState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TaxiRide> rideStateDescriptor =
                    new ValueStateDescriptor<>("ride event", TaxiRide.class);
            rideState = getRuntimeContext().getState(rideStateDescriptor);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out)
                throws Exception {

            TaxiRide firstRideEvent = rideState.value();

            if (firstRideEvent == null) {
                // whatever event comes first, remember it
                rideState.update(ride);

                if (ride.isStart()) {
                    // we will use this timer to check for rides that have gone on too long and may
                    // not yet have an END event (or the END event could be missing)
                    context.timerService().registerEventTimeTimer(getTimerTime(ride));
                }
            } else {
                if (ride.isStart()) {
                    if (rideTooLong(ride, firstRideEvent)) {
                        System.out.println("ride = " + ride.getRideId());
                        out.collect(ride.getRideId());
                    }
                } else {
                    // the first ride was a START event, so there is a timer unless it has fired
                    context.timerService().deleteEventTimeTimer(getTimerTime(firstRideEvent));

                    // perhaps the ride has gone on too long, but the timer didn't fire yet
                    if (rideTooLong(firstRideEvent, ride)) {
                        System.out.println("ride = " + ride.getRideId());
                        out.collect(ride.getRideId());
                    }
                }

                // both events have now been seen, we can clear the state
                // this solution can leak state if an event is missing
                // see DISCUSSION.md for more information
                rideState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {

            // the timer only fires if the ride was too long
            out.collect(rideState.value().getRideId());

            // clearing now prevents duplicate alerts, but will leak state if the END arrives
            rideState.clear();
        }

        private boolean rideTooLong(TaxiRide startEvent, TaxiRide endEvent) {
            return Duration.between(startEvent.getEventTime(), endEvent.getEventTime())
                    .compareTo(Duration.ofHours(2))
                    > 0;
        }

        private long getTimerTime(TaxiRide ride) throws RuntimeException {
            if (ride.isStart()) {
                return ride.getEventTime().plusSeconds(2).toEpochMilli();
            } else {
                throw new RuntimeException("Can not get start time from END event.");
            }
        }
    }

}
