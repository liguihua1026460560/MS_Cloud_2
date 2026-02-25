package com.macrosan.utils.msutils;

import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.asm.BindEpoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import lombok.extern.log4j.Log4j2;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class MsExecutor implements ScheduledExecutorService {
    public ScheduledExecutorService[] executors;
    private ThreadLocal<AtomicInteger> threadLocal = ThreadLocal.withInitial(() -> new AtomicInteger());

    public MsExecutor(int threadNum, int groupNum, ThreadFactory factory) {
        executors = new ScheduledExecutorService[groupNum];
        int n = threadNum / groupNum;
        //虚拟机版本若内核数小于8，此时n=0.令最小为1
        if (ServerConfig.isVm() && n < 1) {
            n = 1;
        }
        for (int i = 0; i < groupNum; i++) {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(n, factory);
            executors[i] = executor;
            executor.setMaximumPoolSize(n);
            executor.setRemoveOnCancelPolicy(true);
        }
    }

    private ScheduledExecutorService get() {
        int index = threadLocal.get().incrementAndGet() % executors.length;
        return executors[Math.abs(index)];
    }

    @Override
    public void execute(Runnable command) {
        get().execute(command);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return get().submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return get().submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return get().submit(task);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return get().schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return get().schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return get().scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return get().scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public List<Runnable> shutdownNow() {
        return null;
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return null;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }
}
