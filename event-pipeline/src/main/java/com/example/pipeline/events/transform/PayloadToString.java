package com.example.pipeline.events.transform;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

public class PayloadToString extends DoFn<PubsubMessage, String> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String message = new String(c.element().getPayload());
        c.output(message);
    }
}
