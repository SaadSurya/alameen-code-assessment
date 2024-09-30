package com.alameen.question2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MessageQueue {
    private final int numPartitions;
    private final int numReplicas;
    private final Map<String, List<Partition>> topicPartitions;
    private final Map<String, Set<Integer>> topicAssignedPartitions;
    private final Map<String, List<Consumer>> topicConsumers;
    private final Random random;

    public MessageQueue(int partitions, int replicas) {
        this.numPartitions = partitions;
        this.numReplicas = replicas;
        this.topicPartitions = new ConcurrentHashMap<>();
        this.topicAssignedPartitions = new ConcurrentHashMap<>();
        this.topicConsumers = new ConcurrentHashMap<>();
        this.random = new Random();
    }

    public Map<String, List<Partition>> getTopicPartitions() {
        return topicPartitions;
    }

    public void publish(String topic, String content) {
        var msg = new Message(content);
        publishMessage(topic, msg);
    }

    public void publishMessage(String topic, Message msg) {
        var partitions = topicPartitions.get(topic);
        if (partitions == null) {
            throw new RuntimeException("Topic does not exist: " + topic);
        }
        var partitionIndex = msg.getId() % numPartitions;
        var partition = partitions.get(partitionIndex);
        if (partition.isActive()) {
            partition.getMessageQueue().add(msg);
            for (var replica : partition.getReplicas()) {
                replica.getReplicaQueue().add(msg);
            }
        } else {
            var replica = partition.getReplicas().get(0);
            replica.getReplicaQueue().add(msg);
            System.out.println("Partition " + partition.getPartitionId() + " is inactive. Published to Replica " + replica.getReplicaId());
        }
    }

    public Consumer subscribe(String topic, String consumerId) {
        var partitions = topicPartitions.get(topic);
        if (partitions == null) {
            throw new RuntimeException("Topic does not exist: " + topic);
        }
        topicAssignedPartitions.putIfAbsent(topic, ConcurrentHashMap.newKeySet());
        var assignedPartitions = topicAssignedPartitions.get(topic);
        Partition assignedPartition = null;
        synchronized (assignedPartitions) {
            var availablePartitions = partitions.stream()
                    .filter(p -> p.isActive() && !assignedPartitions.contains(p.getPartitionId()))
                    .collect(Collectors.toList());
            if (availablePartitions.isEmpty()) {
                throw new RuntimeException("No available partitions for consumer to subscribe.");
            }
            assignedPartition = availablePartitions.get(random.nextInt(availablePartitions.size()));
            assignedPartitions.add(assignedPartition.getPartitionId());
        }
        var consumer = new Consumer(consumerId, topic, assignedPartition.getPartitionId(), this);
        topicConsumers.computeIfAbsent(topic, k -> new ArrayList<>()).add(consumer);
        System.out.println("Consumer " + consumerId + " subscribed to Topic " + topic + " on Partition " + assignedPartition.getPartitionId());
        return consumer;
    }

    public Message consumeMessage(String topic, int partitionId) {
        var partitions = topicPartitions.get(topic);
        if (partitions == null) {
            return null;
        }
        var partition = partitions.get(partitionId);
        if (partition.isActive()) {
            var msg = partition.getMessageQueue().peek();
            if (msg != null) {
                return partition.getMessageQueue().poll();
            }
        } else {
            var replica = partition.getReplicas().get(0);
            var msg = replica.getReplicaQueue().peek();
            if (msg != null) {
                return replica.getReplicaQueue().poll();
            }
        }
        return null;
    }

    public void acknowledgeMessage(String topic, int partitionId, int messageId) {
        var partitions = topicPartitions.get(topic);
        if (partitions == null) {
            return;
        }
        var partition = partitions.get(partitionId);
        partition.acknowledge(messageId);
    }

    public void createTopic(String topic) {
        if (topicPartitions.containsKey(topic)) {
            throw new RuntimeException("Topic already exists: " + topic);
        }
        var partitions = new ArrayList<Partition>();
        for (var i = 0; i < numPartitions; i++) {
            var partition = new Partition(i);
            for (var r = 0; r < numReplicas; r++) {
                var replica = new ReplicaPartition(r);
                partition.addReplica(replica);
            }
            partitions.add(partition);
        }
        topicPartitions.put(topic, partitions);
        topicAssignedPartitions.put(topic, ConcurrentHashMap.newKeySet());
        System.out.println("Created Topic " + topic + " with " + numPartitions + " partitions and " + numReplicas + " replicas each.");
    }

    public void failPartition(String topic, int partitionId) {
        var partitions = topicPartitions.get(topic);
        if (partitions == null) {
            throw new RuntimeException("Topic does not exist: " + topic);
        }
        var partition = partitions.get(partitionId);
        partition.deactivate();
        topicAssignedPartitions.get(topic).remove(partitionId);
        System.out.println("Partition " + partitionId + " in Topic " + topic + " has failed.");
    }

    public void recoverPartition(String topic, int partitionId) {
        var partitions = topicPartitions.get(topic);
        if (partitions == null) {
            throw new RuntimeException("Topic does not exist: " + topic);
        }
        var partition = partitions.get(partitionId);
        partition.activate();
        System.out.println("Partition " + partitionId + " in Topic " + topic + " has recovered.");
    }
}
