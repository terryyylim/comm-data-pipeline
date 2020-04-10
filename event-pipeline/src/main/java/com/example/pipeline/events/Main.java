package com.example.pipeline.events;

import com.example.pipeline.events.transform.PayloadToString;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Main {

    private static final String PROJECT_ID = "gcp-enrichment";
    private static final String TOPIC = "comm-events";
    private static String projTopic;

    private static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        if (TOPIC.equals("comm-events")) {
            System.out.println("Starting comm-events schema builder...");
            fields.add(new TableFieldSchema().setName("event_type").setType("STRING"));
            fields.add(new TableFieldSchema().setName("event_version").setType("STRING"));
            fields.add(new TableFieldSchema().setName("created_at").setType("STRING"));
            fields.add(new TableFieldSchema().setName("user_id").setType("STRING"));
            fields.add(new TableFieldSchema().setName("device_type").setType("STRING"));
            fields.add(new TableFieldSchema().setName("item_name").setType("STRING"));
        }
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

    private static class ExtractJSONToTable extends DoFn<PubsubMessage, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (TOPIC.equals("comm-events")) {
                String message = new String(c.element().getPayload());

                // extract event attributes from string
                JsonObject jsonObject = new JsonParser().parse(message).getAsJsonObject();
                String eventType = jsonObject.get("eventType").getAsString();
                String eventVersion = jsonObject.get("eventVersion").getAsString();
                String userID = jsonObject.get("userID").getAsString();
                String itemName = jsonObject.get("itemName").getAsString();
                String deviceType = jsonObject.get("deviceType").getAsString();
                final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String createdAt = dateFormat.format(new Date());

                TableRow row = new TableRow()
                        .set("event_type", eventType)
                        .set("event_version", eventVersion)
                        .set("created_at", createdAt)
                        .set("user_id", userID)
                        .set("device_type", deviceType)
                        .set("item_name", itemName);
                c.output(row);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting Events Streaming Pipeline...");

        // comm-pipeline topic
        projTopic = "projects/" + PROJECT_ID + "/topics/" + TOPIC;

        CommPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(CommPipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        // read purchase events from Pubsub
        PCollection<PubsubMessage> events = p.apply(PubsubIO.readMessagesWithAttributes().fromTopic(projTopic));

        events.apply(ParDo.of(new ExtractJSONToTable()))
                .apply("WRITE", BigQueryIO.writeTableRows()
                        .to(String.format("%s:ecomm_demo.event_stream", options.getProject()))
                        .withSchema(getTableSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        // Output streaming data into data lake (collection of raw data to be used in future)
        events
                // Convert Payload to String
                .apply("To String", ParDo.of(new PayloadToString()))

                // Batch events into default 1 minute windows
                .apply("Batch Events", Window.<String>into(
                        FixedWindows.of(Duration.standardMinutes(options.getWindowDuration())))
                        .triggering(
                                Repeatedly.forever(
                                    AfterWatermark.pastEndOfWindow()
                                )
                        )
                        .discardingFiredPanes()
                        .withAllowedLateness(Duration.standardMinutes(5)))

                // Save the events in ARVO format
                .apply("To AVRO", AvroIO.write(String.class)
                        .to(options.getAvroDirectory().toString() + options.getOutputFilePrefix())
                        .withNumShards(options.getNumShards())
                        .withWindowedWrites()
                        .withSuffix(options.getOutputFileSuffix().toString()));

        p.run().waitUntilFinish();
    }


}
