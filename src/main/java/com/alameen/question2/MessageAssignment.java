package com.alameen.question2;

public class MessageAssignment {
    private final Message message;
    private final Consumer consumer;
    private final long timestamp;

    public MessageAssignment(Message message, Consumer consumer, long timestamp) {
        this.message = message;
        this.consumer = consumer;
        this.timestamp = timestamp;
    }

    public Message getMessage() {
        return message;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
