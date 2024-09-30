package com.alameen.question1;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class CacheNode {
    private final String nodeId;
    private final int capacity;
    private final LinkedHashMap<String, String> cache;

    public CacheNode(String nodeId, int capacity) {
        this.nodeId = nodeId;
        this.capacity = capacity;
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > CacheNode.this.capacity;
            }
        };
    }

    public synchronized String get(String key) {
        return cache.get(key);
    }

    public synchronized void put(String key, String value) {
        cache.put(key, value);
    }

    public synchronized void remove(String key) {
        cache.remove(key);
    }

    public synchronized Map<String, String> getAllKeys() {
        return new HashMap<>(cache);
    }

    public synchronized void clear() {
        cache.clear();
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "CacheNode{" +
                "nodeId='" + nodeId + '\'' +
                ", keys=" + cache.keySet() +
                '}';
    }
}
