package com.alameen.question3;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        var rateLimiter = new RateLimiter(
                "redis-sentinel",
                26379,
                "mymaster",
                "masterpassword"
        );

        rateLimiter.setLimit("tenantA", 100); // 100 requests per minute
        rateLimiter.setLimit("tenantB", 50);  // 50 requests per minute

        // Simulate API calls for tenantA
        for (int i = 1; i <= 105; i++) {
            var allowed = rateLimiter.allowRequest("tenantA");
            System.out.println("tenantA - Request " + i + " allowed: " + allowed);
            if (!allowed) {
                System.out.println("tenantA - Rate limit exceeded. Waiting for window reset.");
            }
            Thread.sleep(500); // Simulate delay between requests
        }
    }
}