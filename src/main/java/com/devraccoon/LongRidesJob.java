package com.devraccoon;
import com.devraccoon.models.TaxiRide;
import com.devraccoon.params.KafkaParameters;
import com.devraccoon.params.OutputParams;
import com.devraccoon.processes.TaxiRideAvroDeserializerScheme;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.time.Instant;

public class LongRidesJob {

    private KafkaParameters kafkaParameters;
    private OutputParams outputParams;

    /** Creates a job using the source and sink provided. */
    public LongRidesJob(KafkaParameters kafkaParameters, OutputParams outputParams) {
        this.kafkaParameters = kafkaParameters;
        this.outputParams = outputParams;
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
        StreamSetup.setupRestartsAndCheckpoints(env, outputParams.getCheckpointsPath());
        KafkaSource<TaxiRide> kafkaSource = KafkaSource.<TaxiRide>builder()
                .setBootstrapServers(kafkaParameters.getBootstrapServers())
                .setTopics(kafkaParameters.getTopic())
                .setGroupId(kafkaParameters.getGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new TaxiRideAvroDeserializerScheme(kafkaParameters.getSchemaRegistryUrl(), kafkaParameters.getTopic()))
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

        final SingleOutputStreamOperator<Long> longRides = rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.getRideId())
                .process(new AlertFunction());

        FileSink<Long> fileSink = FileSink.forRowFormat(
                new Path(outputParams.getOutputPath() + "/taxi-rides"),
                new SimpleStringEncoder<Long>("UTF-8")
        )
                .build();

        longRides.sinkTo(fileSink);
        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }


    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        KafkaParameters kafkaParameters = KafkaParameters.fromParamToolForLongRideJob(params);
        OutputParams outputParams = OutputParams.fromParamTool(params);

        LongRidesJob job = new LongRidesJob(kafkaParameters, outputParams);
                //new LongRidesJob(null, new PrintSinkFunction<>());

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
            if(rideState.value()!=null) {
                // the timer only fires if the ride was too long
                out.collect(rideState.value().getRideId());

                // clearing now prevents duplicate alerts, but will leak state if the END arrives
                rideState.clear();
            }
        }

        private boolean rideTooLong(TaxiRide startEvent, TaxiRide endEvent) {
            return Duration.between(startEvent.getEventTime(), endEvent.getEventTime())
                    .compareTo(Duration.ofHours(2))
                    > 0;
        }

        private long getTimerTime(TaxiRide ride) throws RuntimeException {
            if (ride.isStart()) {
                return ride.getEventTime().plusSeconds(60*120).toEpochMilli();
            } else {
                throw new RuntimeException("Can not get start time from END event.");
            }
        }
    }

}
