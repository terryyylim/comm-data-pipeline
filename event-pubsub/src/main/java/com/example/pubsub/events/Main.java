package com.example.pubsub.events;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final String PROJECT_ID = "gcp-enrichment";
    private static final String TOPIC = "comm-events";
    private static TopicAdminClient client = null;
    private static Publisher publisher = null;

    private static void createTopic(TopicName topic) throws Exception {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            topicAdminClient.createTopic(topic);
        }
    }

    public static String formatMessage(String eventType, String eventVersion, HashMap<String, String> eventAttributes) {
        StringBuilder event = new StringBuilder(
                "{\"eventType\":\"" + eventType
                + "\",\"eventVersion\":\"" + eventVersion + "\"");
        for (Map.Entry<String, String> eventAttribute: eventAttributes.entrySet()) {
            event.append(",\"" + eventAttribute.getKey() + "\":\"" + eventAttribute.getValue() + "\"");
        }
        event.append("}");

        return event.toString();
    }

    public static void publishMessages() throws Exception {
        TopicName topic = TopicName.of(PROJECT_ID, TOPIC);

        // Create a publisher instance with default settings bound to the topic
        publisher = Publisher.newBuilder(topic).build();
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        for (int idx=0; idx<10; idx++) {
            // generate event type
            String eventType = Math.random() < 0.5 ? "Session": (Math.random() < 0.5 ? "AddToCart": "AddToFavourites");

            // generate event attributes
            HashMap<String, String> attributes = new HashMap<String, String>();
            attributes.put("userID", "" + (int)(Math.random()*10));
            attributes.put("itemName", Math.random() < 0.5 ? "Kitchen Paper Rolls" : (Math.random() < 0.5 ? "Chocolate Bars": "Rice"));
            attributes.put("deviceType", Math.random() < 0.5 ? "iOS" : (Math.random() < 0.5 ? "Android" : "Web"));

            try {
                String event = formatMessage(eventType, "v1", attributes);

                // convert event to bytes
                ByteString eventBytes = ByteString.copyFromUtf8(event.toString());
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(eventBytes).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                messageIdFutures.add(messageIdFuture);
            } finally {
                // wait on any pending publish requests.
                List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

                for (String messageId : messageIds) {
                    System.out.println("published with message ID: " + messageId);
                }
            }
        }
        if (publisher != null) {
            // When finished with the publisher, shutdown to free up resources.
            publisher.shutdown();
            publisher.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Events Pubsub Message Generator...");
        if (client == null) {
            client = TopicAdminClient.create();
        }
        // Create topic if it doesn't exist
        TopicName topic = TopicName.of(PROJECT_ID, TOPIC);
        if (client.getTopic(topic) == null) {
            createTopic(topic);
        }
        publishMessages();
    }
}
