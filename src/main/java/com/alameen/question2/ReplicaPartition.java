package com.alameen.question2;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ReplicaPartition {
    private final int replicaId;
    private final BlockingQueue<Message> replicaQueue;

    public ReplicaPartition(int replicaId) {
        this.replicaId = replicaId;
        this.replicaQueue = new LinkedBlockingQueue<>();
    }

    public int getReplicaId() {
        return replicaId;
    }

    public BlockingQueue<Message> getReplicaQueue() {
        return replicaQueue;
    }
}
