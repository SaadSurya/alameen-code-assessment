package com.alameen.question2;

public class Producer {
    private final String producerId;
    private final MessageQueue queue;

    public Producer(String producerId, MessageQueue queue) {
        this.producerId = producerId;
        this.queue = queue;
    }

    public void publish(String topic, String content) {
        var msg = new Message(content);
        queue.publishMessage(topic, msg);
        System.out.println("Producer " + producerId + " published Message ID " + msg.getId() + ": " + msg.getContent());
    }
}
