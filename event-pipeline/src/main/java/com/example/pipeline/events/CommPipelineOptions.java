package com.example.pipeline.events;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface CommPipelineOptions extends DataflowPipelineOptions {

    @Description("Path for Dataflow job to store temp files")
    String getTempLocation();
    void setTempLocation(String value);

    @Description("The window duration in which data will be written. Time unit is minute.")
    @Default.Integer(1)
    Integer getWindowDuration();
    void setWindowDuration(Integer value);

    @Description("Number of shards to utilize")
    @Default.Integer(1)
    Integer getNumShards();
    void setNumShards(Integer value);

    @Description("Prefix of files to write")
    @Default.String("output")
    ValueProvider<String> getOutputFilePrefix();
    void setOutputFilePrefix(ValueProvider<String> value);

    @Description("Suffix of files to write")
    @Default.String("")
    ValueProvider<String> getOutputFileSuffix();
    void setOutputFileSuffix(ValueProvider<String> value);

    @Description("Path for Data Lake with Avro files")
    @Validation.Required
    ValueProvider<String> getAvroDirectory();
    void setAvroDirectory(ValueProvider<String> value);
}
