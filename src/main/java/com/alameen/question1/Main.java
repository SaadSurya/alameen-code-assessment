package com.alameen.question1;

/**
 * Distributed Cache System with Consistent Hashing
 * Problem: Design and implement a distributed cache system that supports multiple cache
 * nodes. When a new node is added or removed, the cache system should redistribute keys in
 * such a way that the majority of keys remain mapped to their original nodes to avoid
 * unnecessary data transfer between nodes. You need to use consistent hashing to achieve
 * this.
 * Requirements:
 * * Implement a cache node class that stores key-value pairs.
 * * Implement the distributed cache system that:
 * ** Supports adding and removing cache nodes.
 * ** Redistributes keys efficiently when nodes are added or removed using consistent hashing.
 * ** Supports basic cache operations: get(key) and put(key, value).
 * ** The cache should use LRU (Least Recently Used) policy for evicting old entries when a node’s capacity is full.
 * Constraints:
 * * Assume the system can handle millions of key-value pairs.
 * * Implement the hashing function and the consistent hashing logic yourself.
 * * Implement this with considerations for performance (e.g., minimize key redistribution when nodes change).
 */
public class Main {
    public static void main(String[] args) {
        // Initialize the distributed cache system with 3 nodes, each with capacity 5
        DistributedCacheSystem cache = new DistributedCacheSystem(3, 10);

        // Insert key-value pairs
        cache.put("user:1001", "Alice");
        cache.put("user:1002", "Bob");
        cache.put("user:1003", "Charlie");
        cache.put("user:1004", "Diana");
        cache.put("user:1005", "Eve");
        cache.put("user:1006", "Frank");
        cache.put("user:1007", "Grace");
        cache.put("user:1008", "Hannah");
        cache.put("user:1009", "Ivan");
        cache.put("user:1010", "Judy");

        System.out.println("Initial distribution:");
        cache.printDistribution();

        // Add a new node
        cache.addNode("node-3");
        System.out.println("\nAfter adding Node-3:");
        cache.printDistribution();

        // Remove a node
        cache.removeNode("node-1");
        System.out.println("\nAfter removing Node-1:");
        cache.printDistribution();

        // Retrieve some values
        System.out.println("\nRetrieve keys:");
        System.out.println("user:1001 -> " + cache.get("user:1001"));
        System.out.println("user:1002 -> " + cache.get("user:1002"));
        System.out.println("user:1003 -> " + cache.get("user:1003"));
        System.out.println("user:1004 -> " + cache.get("user:1004"));
        System.out.println("user:1005 -> " + cache.get("user:1005"));
        System.out.println("user:1006 -> " + cache.get("user:1006"));
        System.out.println("user:1007 -> " + cache.get("user:1007"));
        System.out.println("user:1008 -> " + cache.get("user:1008"));
        System.out.println("user:1009 -> " + cache.get("user:1009"));
        System.out.println("user:1010 -> " + cache.get("user:1010"));
    }
}