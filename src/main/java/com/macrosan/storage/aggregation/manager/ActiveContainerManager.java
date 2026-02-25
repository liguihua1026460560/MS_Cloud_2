package com.macrosan.storage.aggregation.manager;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.storage.aggregation.aggregator.BaseAggregateContainerWrapper;
import com.macrosan.storage.aggregation.aggregator.container.AggregateContainer;
import com.macrosan.storage.aggregation.aggregator.container.AggregateContainerPool;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public abstract class ActiveContainerManager<T extends BaseAggregateContainerWrapper<?>> {

    /**
     * 容器组：包含活跃容器和待下刷容器列表
     */
    private static class ContainerGroup<T extends BaseAggregateContainerWrapper<?>> {
        final ReentrantLock switchLock = new ReentrantLock();
        final AtomicReference<T> activeContainer = new AtomicReference<>();
    }

    /**
     * Namespace与活跃容器组的映射关系
     */
    private final ConcurrentHashMap<String, ContainerGroup<T>> namespaceContainers = new ConcurrentHashMap<>();


    /**
     * 获取当前活跃容器，必要时创建新容器
     */
    public T  getActiveContainer(NameSpace namespace) {
        ContainerGroup<T> group = namespaceContainers.computeIfAbsent(namespace.getNameSpaceIdentifier(), ns -> new ContainerGroup<>());
        group.switchLock.lock();
        try {
            T wrapper = group.activeContainer.get();
            // 容器未初始化或已不可变时创建新容器
            if (wrapper == null || wrapper.getContainer() == null || wrapper.isImmutable()) {
                // 异步下刷旧容器
                if (wrapper != null && !wrapper.isEmpty()) {
                    triggerAsyncFlush(wrapper);
                }
                wrapper = createContainer(namespace);
                group.activeContainer.set(wrapper);
            }
            return wrapper;
        } finally {
            group.switchLock.unlock();
        }
    }

    /**
     * 创建新聚合容器
     */
    private T createContainer(NameSpace namespace) {
        int capacity = (int) namespace.getAggregateConfig().getMaxAggregatedFileSize();
        int maxFiles = namespace.getAggregateConfig().getMaxAggregatedFileNum();
        boolean direct = namespace.getAggregateConfig().isUseDirect();
        AggregateContainer container = AggregateContainerPool.getInstance().getContainer(maxFiles, capacity, direct);
        if (container == null) {
            return null;
        }
        return createContainerWrapper(namespace, container);
    }

    protected abstract T createContainerWrapper(NameSpace namespace, AggregateContainer container);

    /**
     * 触发异步下刷
     */
    private void triggerAsyncFlush(T wrapper) {
        if (wrapper.isFlushing()) {
            return;
        }
        wrapper.flush(false)
                .doOnError(t -> {
                    if (!(t instanceof IllegalStateException)) {
                        log.error("Flush error", t);
                        wrapper.clear();
                    }
                })
                .doOnSuccess(v -> wrapper.clear())  // 下刷成功后清理资源
                .subscribeOn(ErasureServer.DISK_SCHEDULER) // 使用独立线程池
                .subscribe();
    }
}
