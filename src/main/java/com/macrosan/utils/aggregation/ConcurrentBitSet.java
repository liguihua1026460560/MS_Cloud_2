package com.macrosan.utils.aggregation;

import java.util.BitSet;

public class ConcurrentBitSet extends BitSet {
    public ConcurrentBitSet(int size) {
        super(size);
    }

    @Override
    public synchronized void set(int bitIndex) {
        super.set(bitIndex);
    }

    @Override
    public synchronized void set(int fromIndex, int toIndex) {
        super.set(fromIndex, toIndex);
    }

    @Override
    public synchronized boolean get(int bitIndex) {
        return super.get(bitIndex);
    }

    @Override
    public synchronized BitSet get(int fromIndex, int toIndex) {
        return super.get(fromIndex, toIndex);
    }

    @Override
    public synchronized void and(BitSet set) {
        super.and(set);
    }

    @Override
    public synchronized void or(BitSet set) {
        super.or(set);
    }

    @Override
    public synchronized void clear() {
        super.clear();
    }

    @Override
    public synchronized void clear(int bitIndex) {
        super.clear(bitIndex);
    }

    @Override
    public synchronized byte[] toByteArray() {
        return super.toByteArray();
    }
}
