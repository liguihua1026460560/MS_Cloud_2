package com.macrosan.filesystem;

import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class TimeStat {
    private static final Map<String, TimeStat> timeMap = new ConcurrentHashMap<>();
    AtomicLong num = new AtomicLong();
    AtomicLong time = new AtomicLong();

    public static void addTime(String key, long t) {
        TimeStat stat = timeMap.computeIfAbsent(key, k -> new TimeStat());
        stat.num.incrementAndGet();
        stat.time.addAndGet(t);
    }


    static {
        new Thread(() -> {
            while (true) {
                synchronized (Thread.currentThread()) {
                    try {
                        Thread.currentThread().wait(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    AtomicBoolean loged = new AtomicBoolean(false);


                    timeMap.keySet().stream().sorted().forEach(k -> {

                        TimeStat v = timeMap.get(k);
                        long n = v.num.get();
                        if (n > 0) {
                            loged.set(true);
                            long time = v.time.get();
                            double avg = time / 1000_000.0 / n;
                            log.info("{}:{} {} {}ms", k, time, n, String.format("%.2f", avg));

                            v.num.addAndGet(-n);
                            v.time.addAndGet(-time);
                        }
                    });
                    if (loged.get()) {
                        log.info("");
                    }
                }
            }
        }).start();
    }
}
