package com.macrosan.utils.listutil.interpcy;

public interface InterruptPolicy<T> {
    boolean shouldInterrupt(T item);
}
