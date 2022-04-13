package com.devraccoon;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class StreamSetup {

    public static void setupRestartsAndCheckpoints(StreamExecutionEnvironment env, String checkpointStoragePath) {
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        5, Duration.ofMinutes(1).toMillis()
                )
        );

        env.enableCheckpointing(Duration.ofSeconds(10).toMillis());
        env.getCheckpointConfig().setCheckpointTimeout(Duration.ofMinutes(1).toMillis());
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setCheckpointStorage(checkpointStoragePath);

        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
    }
}
