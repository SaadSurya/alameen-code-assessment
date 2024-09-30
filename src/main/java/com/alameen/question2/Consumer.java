package com.alameen.question2;

public class Consumer {
    private final String consumerId;
    private final String topic;
    private final int partitionId;
    private final MessageQueue queue;

    public Consumer(String consumerId, String topic, int partitionId, MessageQueue queue) {
        this.consumerId = consumerId;
        this.topic = topic;
        this.partitionId = partitionId;
        this.queue = queue;
    }

    public void consume() throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            var msg = queue.consumeMessage(topic, partitionId);
            if (msg != null) {
                queue.acknowledgeMessage(topic, partitionId, msg.getId());
                System.out.println("Consumer " + consumerId + " consumed Message ID " + msg.getId() + ": " + msg.getContent());
            } else {
                Thread.sleep(100);
            }
        }
    }
}
