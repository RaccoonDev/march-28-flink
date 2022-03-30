package com.devraccoon.params;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Objects;

public class OutputParams {
    private final String outputPath;


    public OutputParams(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String getCheckpointsPath() {
        return outputPath + "/checkpoints";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutputParams that = (OutputParams) o;
        return Objects.equals(outputPath, that.outputPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputPath);
    }

    @Override
    public String toString() {
        return "OutputParams{" +
                "outputPath='" + outputPath + '\'' +
                '}';
    }

    public static OutputParams fromParamTool(ParameterTool params) {
        final String pwd = System.getenv().get("PWD");
        final String outputPath = params.get("output-path") != null ? params.get("output-path") : "file://" + pwd + "/out/";

        return new OutputParams(outputPath);
    }
}
