package com.macrosan.utils.listutil.interpcy;

public class ExhaustionPolicy<T> implements InterruptPolicy<T>{
    @Override
    public boolean shouldInterrupt(T item) {
        return false;
    }
}
