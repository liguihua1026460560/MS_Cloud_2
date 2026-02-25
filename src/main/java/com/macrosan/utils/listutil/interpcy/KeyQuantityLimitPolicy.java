package com.macrosan.utils.listutil.interpcy;

import lombok.extern.log4j.Log4j2;

import java.util.concurrent.atomic.AtomicLong;

public class KeyQuantityLimitPolicy<T> implements InterruptPolicy<T> {
    private final AtomicLong current = new AtomicLong(0);
    private final long maxKeys;
    public KeyQuantityLimitPolicy(long maxKeys) {
        this.maxKeys = maxKeys;
    }
    @Override
    public boolean shouldInterrupt(T metaData) {
        return current.incrementAndGet() > maxKeys;
    }
}
