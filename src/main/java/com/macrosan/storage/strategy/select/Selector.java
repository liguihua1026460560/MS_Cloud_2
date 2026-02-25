package com.macrosan.storage.strategy.select;

import com.macrosan.storage.StoragePool;

/**
 * @author gaozhiyuan
 */
public interface Selector {
    StoragePool select(StoragePool[] pool);

    StoragePool[] sort(StoragePool[] pool);
}
