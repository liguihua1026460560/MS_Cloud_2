package com.macrosan.storage.aggregation.aggregator.container;

import lombok.extern.log4j.Log4j2;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class AggregateContainerPool {
    private final static AggregateContainerPool aggregateContainerPool;
    static {
        aggregateContainerPool = new AggregateContainerPool(8);
    }

    public static AggregateContainerPool getInstance() {
        return aggregateContainerPool;
    }

    private final Queue<AggregateContainer> pool;
    private final int maxPoolSize;

    private final AtomicInteger poolSize = new AtomicInteger(0);

    private final ReentrantLock lock = new ReentrantLock();

    private AggregateContainerPool(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        this.pool = new ConcurrentLinkedQueue<>();
    }

    public AggregateContainer getContainer(int maxFilesLimit, int capacity, boolean useDirect) {
        lock.lock();
        try {
            AggregateContainer container = pool.poll();
            if (container == null) {
                if (poolSize.get() < maxPoolSize) {
                    container = createNewContainer(maxFilesLimit, capacity, useDirect);
                } else {
                    log.debug("No available container in the pool");
                    return null;
                }
            } else if (container.getCapacity() != capacity || container.getMaxFilesLimit() != maxFilesLimit) {
                log.debug("Container capacity or maxFilesLimit changed, clear it");
                container.clear();
                container = createNewContainer(maxFilesLimit, capacity, useDirect);
            }
            container.clear();
            return container;
        } finally {
            lock.unlock();
        }
    }

    public void releaseContainer(AggregateContainer container) {
        lock.lock();
        try {
            if (container != null) {
                pool.offer(container);
            }
        } finally {
            lock.unlock();
        }
    }

    private AggregateContainer createNewContainer(int maxFilesLimit, int capacity, boolean useDirect) {
        return new AggregateContainer("container-" + poolSize.incrementAndGet(), maxFilesLimit, capacity, useDirect);
    }

}
