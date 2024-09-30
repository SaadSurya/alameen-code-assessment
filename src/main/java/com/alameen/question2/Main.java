package com.alameen.question2;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        var queue = new MessageQueue(3, 2);
        queue.createTopic("topic1");

        var producer1 = new Producer("Producer1", queue);
        var producer2 = new Producer("Producer2", queue);

        var consumer1 = queue.subscribe("topic1", "Consumer1");
        var consumer2 = queue.subscribe("topic1", "Consumer2");
        var consumer3 = queue.subscribe("topic1", "Consumer3");

        var consumerThread1 = new Thread(() -> {
            try {
                consumer1.consume();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        var consumerThread2 = new Thread(() -> {
            try {
                consumer2.consume();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        var consumerThread3 = new Thread(() -> {
            try {
                consumer3.consume();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        consumerThread1.start();
        consumerThread2.start();
        consumerThread3.start();

        var producerThread1 = new Thread(() -> {
            for (var i = 1; i <= 10; i++) {
                producer1.publish("topic1", "Message" + i + " from Producer1");
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        var producerThread2 = new Thread(() -> {
            for (var i = 1; i <= 10; i++) {
                producer2.publish("topic1", "Message" + i + " from Producer2");
                try {
                    Thread.sleep(70);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        producerThread1.start();
        producerThread2.start();

        var failureSimulator = new Thread(() -> {
            try {
                Thread.sleep(500);
                queue.failPartition("topic1", 1);

                Thread.sleep(500);
                queue.recoverPartition("topic1", 1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        failureSimulator.start();

        Thread.sleep(5000);

        consumerThread1.interrupt();
        consumerThread2.interrupt();
        consumerThread3.interrupt();
        producerThread1.join();
        producerThread2.join();
        failureSimulator.join();

        System.out.println("\nFinal distribution:");
        System.out.println("Current key distribution:");
        var partitions = queue.getTopicPartitions().get("topic1");
        for (var partition : partitions) {
            var active = partition.isActive();
            var size = partition.getMessageQueue().size();
            var lastAcked = partition.getLastAckedId();
            System.out.println("Topic: topic1 | Partition: " + partition.getPartitionId() + " | Active: " + active + " | Queue Size: " + size + " | Last Acked ID: " + lastAcked);
        }
    }
}
