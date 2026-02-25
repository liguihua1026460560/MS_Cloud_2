package com.macrosan.storage.strategy.select;

import com.macrosan.storage.StoragePool;

/**
 * @author gaozhiyuan
 */
public class NoSelector implements Selector {
    @Override
    public StoragePool select(StoragePool[] pool) {
        return pool[0];
    }

    @Override
    public StoragePool[] sort(StoragePool[] pool) {
        return pool;
    }
}
