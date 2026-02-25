package com.macrosan.utils.msutils;

import lombok.extern.log4j.Log4j2;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class Scheduler {
    private final static ThreadFactory DEFAULT_PARALLEL_THREAD_FACTORY = new ThreadFactory() {
        private AtomicLong counterReference = new AtomicLong(0);

        @Override
        public Thread newThread(Runnable r) {
            String newThreadName = "moss-parallel-" + counterReference.incrementAndGet();
            Thread t = new Thread(r, newThreadName);
            t.setDaemon(false);

            t.setUncaughtExceptionHandler((th, e) ->
                    log.error("{} failed with an uncaught exception", t.getName(), e));
            return t;
        }
    };

    @SuppressWarnings("unchecked")
    public static void init() {
        try {
            MsExecutor executor = new MsExecutor(Runtime.getRuntime().availableProcessors(),
                    Math.max(1, Runtime.getRuntime().availableProcessors() / 8),
                    DEFAULT_PARALLEL_THREAD_FACTORY);
            reactor.core.scheduler.Scheduler scheduler = Schedulers.fromExecutor(executor);

            for (Class c : Schedulers.class.getDeclaredClasses()) {
                if (c.getName().contains("CachedScheduler")) {
                    Constructor constructor = c.getDeclaredConstructor(String.class, reactor.core.scheduler.Scheduler.class);
                    constructor.setAccessible(true);
                    Object cachedScheduler = constructor.newInstance("parallel", scheduler);

                    Field field = Schedulers.class.getDeclaredField("CACHED_PARALLEL");
                    field.setAccessible(true);
                    AtomicReference reference = (AtomicReference) field.get(null);
                    reference.set(cachedScheduler);
                    log.info("replace default parallel scheduler successful");
                }
            }
        } catch (Exception e) {
            log.error("replace default parallel scheduler error", e);
            System.exit(1);
        }
    }
}
