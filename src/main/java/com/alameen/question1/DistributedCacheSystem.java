package com.alameen.question1;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

public class DistributedCacheSystem {
    private final TreeMap<Integer, CacheNode> hashRing;
    private final Map<String, CacheNode> nodes;
    private final int nodeCapacity;

    public DistributedCacheSystem(int nodeCount, int nodeCapacity) {
        this.hashRing = new TreeMap<>();
        this.nodes = new HashMap<>();
        this.nodeCapacity = nodeCapacity;
        for (int i = 0; i < nodeCount; i++) {
            addNode("node-" + i);
        }
    }

    public synchronized void addNode(String nodeId) {
        if (nodes.containsKey(nodeId)) {
            System.out.println("Node already exists: " + nodeId);
            return;
        }

        var newNode = new CacheNode(nodeId, nodeCapacity);
        nodes.put(nodeId, newNode);

        var nodeHash = hash(nodeId);
        // Check for hash collisions
        while (hashRing.containsKey(nodeHash)) {
            nodeHash = hash(nodeId + UUID.randomUUID());
        }
        hashRing.put(nodeHash, newNode);

        redistributeKeysOnAdd(nodeHash, newNode);

        System.out.println("Added node: " + nodeId + " with hash: " + nodeHash);
    }

    public synchronized void removeNode(String nodeId) {
        var removedNode = nodes.remove(nodeId);
        if (removedNode == null) {
            System.out.println("Node not found: " + nodeId);
            return;
        }

        // Find the hash of the node to remove
        Integer nodeHashToRemove = null;
        for (Map.Entry<Integer, CacheNode> entry : hashRing.entrySet()) {
            if (entry.getValue().equals(removedNode)) {
                nodeHashToRemove = entry.getKey();
                break;
            }
        }

        if (nodeHashToRemove == null) {
            System.out.println("Hash not found for node: " + nodeId);
            return;
        }

        hashRing.remove(nodeHashToRemove);

        redistributeKeysOnRemove(nodeHashToRemove, removedNode);

        System.out.println("Removed node: " + nodeId + " with hash: " + nodeHashToRemove);
    }

    public String get(String key) {
        CacheNode node = getNodeForKey(key);
        if (node == null) {
            return null;
        }
        return node.get(key);
    }

    public void put(String key, String value) {
        CacheNode node = getNodeForKey(key);
        if (node == null) {
            System.out.println("No available nodes to store the key: " + key);
            return;
        }
        node.put(key, value);
    }

    public void addNode() {
        String nodeId = "node-" + UUID.randomUUID();
        addNode(nodeId);
    }

    public void removeNode() {
        if (nodes.isEmpty()) {
            System.out.println("No nodes to remove.");
            return;
        }
        String nodeId = nodes.keySet().iterator().next();
        removeNode(nodeId);
    }

    private int hash(String key) {
        int hash = 0x811c9dc5;
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        for (byte b : bytes) {
            hash ^= (b & 0xff);
            hash *= 16777619;
        }
        return hash & 0x7FFFFFFF;
    }

    private boolean isKeyInRange(int keyHash, int startHash, int endHash) {
        if (startHash < endHash) {
            return keyHash > startHash && keyHash <= endHash;
        } else {
            return keyHash > startHash || keyHash <= endHash;
        }
    }

    private CacheNode getNodeForKey(String key) {
        if (hashRing.isEmpty()) {
            return null;
        }
        int hash = hash(key);
        SortedMap<Integer, CacheNode> tailMap = hashRing.tailMap(hash);
        int nodeHash = tailMap.isEmpty() ? hashRing.firstKey() : tailMap.firstKey();
        return hashRing.get(nodeHash);
    }

    private void redistributeKeysOnAdd(int newNodeHash, CacheNode newNode) {
        // Find predecessor node
        Integer predecessorHash = hashRing.lowerKey(newNodeHash);
        if (predecessorHash == null) {
            predecessorHash = hashRing.lastKey();
        }
        CacheNode predecessorNode = hashRing.get(predecessorHash);

        // Collect keys that now belong to the new node
        Map<String, String> keysToMove = new HashMap<>();
        for (Map.Entry<String, String> entry : predecessorNode.getAllKeys().entrySet()) {
            String key = entry.getKey();
            int keyHash = hash(key);
            if (isKeyInRange(keyHash, predecessorHash, newNodeHash)) {
                keysToMove.put(key, entry.getValue());
            }
        }

        // Move keys to the new node
        for (Map.Entry<String, String> entry : keysToMove.entrySet()) {
            predecessorNode.remove(entry.getKey());
            newNode.put(entry.getKey(), entry.getValue());
        }

        System.out.println("Redistributed " + keysToMove.size() + " keys to " + newNode.getNodeId());
    }

    private void redistributeKeysOnRemove(int removedNodeHash, CacheNode removedNode) {
        if (hashRing.isEmpty()) {
            // No nodes left, all keys are lost
            System.out.println("All nodes removed. Cache is empty.");
            return;
        }

        // Find successor node
        SortedMap<Integer, CacheNode> tailMap = hashRing.tailMap(removedNodeHash);
        Integer successorHash = tailMap.isEmpty() ? hashRing.firstKey() : tailMap.firstKey();
        CacheNode successorNode = hashRing.get(successorHash);

        // Move all keys from the removed node to the successor node
        Map<String, String> keysToMove = removedNode.getAllKeys();
        for (Map.Entry<String, String> entry : keysToMove.entrySet()) {
            successorNode.put(entry.getKey(), entry.getValue());
        }

        System.out.println("Redistributed " + keysToMove.size() + " keys from " + removedNode.getNodeId() + " to " + successorNode.getNodeId());
    }

    public void printDistribution() {
        System.out.println("Current key distribution:");
        for (Map.Entry<Integer, CacheNode> entry : hashRing.entrySet()) {
            System.out.println("Hash: " + entry.getKey() + " | " + entry.getValue());
        }
    }
}
