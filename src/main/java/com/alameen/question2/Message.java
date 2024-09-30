package com.alameen.question2;

import java.util.concurrent.atomic.AtomicInteger;

public class Message {
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);
    private final int id;
    private final String content;

    public Message(String content) {
        this.id = ID_GENERATOR.incrementAndGet();
        this.content = content;
    }

    public int getId() {
        return id;
    }

    public String getContent() {
        return content;
    }
}
