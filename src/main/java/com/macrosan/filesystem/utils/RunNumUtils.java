package com.macrosan.filesystem.utils;

import io.vertx.reactivex.core.streams.ReadStream;
import lombok.extern.log4j.Log4j2;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class RunNumUtils {
    private static final AtomicLong running = new AtomicLong();
    private static final int MAX_RUN_NUM = 1600;

    public static AtomicLong getRunNum() {
        return running;
    }

    public static int getMaxRunNum() {
        return MAX_RUN_NUM;
    }

    private static final Queue<ReadStream> pauseQueue = new ConcurrentLinkedQueue<>();

    public static void checkRunning(ReadStream socket, boolean debug) {
        long curRun = running.incrementAndGet();
        if (debug) {
            log.info("running {}", curRun);
        }

        if (curRun > MAX_RUN_NUM) {
            socket.pause();
            pauseQueue.add(socket);
        }

    }

    public static void releaseRunning() {
        if (running.decrementAndGet() < MAX_RUN_NUM) {
            ReadStream s;
            for (int i = 0; i < 10; i++) {
                if ((s = pauseQueue.poll()) != null) {
                    s.resume();
                } else {
                    break;
                }
            }
        }
    }
}
