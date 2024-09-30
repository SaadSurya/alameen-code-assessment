package com.alameen.question2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Partition {
    private final int partitionId;
    private final BlockingQueue<Message> messageQueue;
    private final List<ReplicaPartition> replicas;
    private volatile boolean isActive;
    private final AtomicInteger lastAckedId;

    public Partition(int partitionId) {
        this.partitionId = partitionId;
        this.messageQueue = new LinkedBlockingQueue<>();
        this.replicas = new ArrayList<>();
        this.isActive = true;
        this.lastAckedId = new AtomicInteger(0);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public BlockingQueue<Message> getMessageQueue() {
        return messageQueue;
    }

    public void addReplica(ReplicaPartition replica) {
        replicas.add(replica);
    }

    public List<ReplicaPartition> getReplicas() {
        return replicas;
    }

    public boolean isActive() {
        return isActive;
    }

    public void deactivate() {
        isActive = false;
    }

    public void activate() {
        isActive = true;
    }

    public int getLastAckedId() {
        return lastAckedId.get();
    }

    public void acknowledge(int messageId) {
        lastAckedId.updateAndGet(current -> Math.max(current, messageId));
    }
}
