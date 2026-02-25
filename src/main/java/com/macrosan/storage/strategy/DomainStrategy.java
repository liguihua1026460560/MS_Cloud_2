package com.macrosan.storage.strategy;

import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.strategy.select.SelectStrategy;

/**
 * @author gaozhiyuan
 */
class DomainStrategy extends SelectorStrategy {
    private StoragePool[] meta;
    private StoragePool[] data;

    DomainStrategy(StoragePool[] meta, StoragePool[] data, SelectStrategy selectStrategy, String maxRate, String minRemainSize) {
        super(selectStrategy, maxRate, minRemainSize);
        this.meta = meta;
        this.data = data;
    }

    @Override
    public StoragePool getStoragePool(StorageOperate operate) {
        switch (operate.getPoolType()) {
            case DEFAULT_META:
                return meta[0];
            case DEFAULT_DATA:
                return data[0];
            case META:
                return selector.select(meta);
            case DATA:
                return selector.select(data);
            default:
                return null;
        }
    }

    @Override
    public StoragePool[] getStoragePools(StorageOperate operate) {
        switch (operate.getPoolType()) {
            case DEFAULT_META:
                return meta;
            case DEFAULT_DATA:
                return data;
            case META:
                return selector.sort(meta);
            case DATA:
                return selector.sort(data);
            default:
                return null;
        }
    }

}
