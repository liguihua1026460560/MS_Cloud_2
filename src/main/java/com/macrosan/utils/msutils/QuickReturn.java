package com.macrosan.utils.msutils;

import lombok.extern.log4j.Log4j2;

import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class QuickReturn {
    static AtomicLong num = new AtomicLong();

    public static boolean acquire() {
        if (num.incrementAndGet() < 1000) {
            return true;
        } else {
            num.decrementAndGet();
            return false;
        }
    }

    public static void release() {
        num.decrementAndGet();
    }
}
