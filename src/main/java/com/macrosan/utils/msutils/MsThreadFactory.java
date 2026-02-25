package com.macrosan.utils.msutils;

import lombok.extern.log4j.Log4j2;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class MsThreadFactory implements ThreadFactory {
    private AtomicLong counterReference = new AtomicLong(0);
    private String name;

    @Override
    public Thread newThread(Runnable r) {
        String newThreadName = name + counterReference.incrementAndGet();
        Thread t = new Thread(r, newThreadName);
        t.setDaemon(false);
        t.setUncaughtExceptionHandler((th, e) -> {
            if (e.getMessage() == null || !e.getMessage().endsWith("Disposed")) {
                log.error("{} failed with an uncaught exception", t.getName(), e);
            }
        });
        return t;
    }

    public MsThreadFactory(String name) {
        this.name = name + "-";
    }
}
