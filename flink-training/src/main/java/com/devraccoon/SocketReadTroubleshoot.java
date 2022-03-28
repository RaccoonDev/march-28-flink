package com.devraccoon;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketReadTroubleshoot {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999).print();

        env.execute("Job to Troubleshoot socket read");
    }
}
