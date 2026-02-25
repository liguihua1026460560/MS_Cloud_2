package com.macrosan.ec.rebuild;

import com.macrosan.storage.StoragePool;
import lombok.AllArgsConstructor;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RebuildLog {
    private static final long LOG_NUM = 1000;
    private static final Queue<Log> logQueue = new ConcurrentLinkedQueue<Log>() {
        {
            for (long i = 0; i < LOG_NUM; i++) {
                this.add(new Log(null, null, null));
            }
        }
    };

    public static void log(StoragePool pool, String vnode, String message) {
        logQueue.poll();
        logQueue.add(new Log(pool, vnode, message));
    }

    @AllArgsConstructor
    private static class Log {
        StoragePool pool;
        String vnode;
        String message;
    }
}
