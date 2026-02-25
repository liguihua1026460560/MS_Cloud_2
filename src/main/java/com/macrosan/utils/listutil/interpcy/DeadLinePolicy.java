package com.macrosan.utils.listutil.interpcy;

public class DeadLinePolicy<T> implements InterruptPolicy<T> {

    private final long maxDeadLine;

    public DeadLinePolicy(long maxDeadLine) {
        this.maxDeadLine = maxDeadLine;
    }

    @Override
    public boolean shouldInterrupt(T item) {
        return System.currentTimeMillis() >= maxDeadLine;
    }
}
