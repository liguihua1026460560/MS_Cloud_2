package com.macrosan.storage;

import lombok.Data;

/**
 * @author gaozhiyuan
 */
@Data
public class StorageOperate {
    public enum PoolType {
        META, DATA, DEFAULT_META, DEFAULT_DATA, CACHE
    }

    PoolType poolType;
    String key;
    long dataSize;

    public StorageOperate(PoolType type, String key, long dataSize) {
        this.poolType = type;
        this.key = key;
        this.dataSize = dataSize;
    }

    public static final StorageOperate META = new StorageOperate(PoolType.DEFAULT_META, null, 0);
    public static final StorageOperate DATA = new StorageOperate(PoolType.DEFAULT_DATA, null, 0);
}
