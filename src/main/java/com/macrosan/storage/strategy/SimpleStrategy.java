package com.macrosan.storage.strategy;

import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;

/**
 * @author gaozhiyuan
 */
class SimpleStrategy extends StorageStrategy {
    private StoragePool meta;
    private StoragePool data;
    private StoragePool[] metas;
    private StoragePool[] datas;

    SimpleStrategy(StoragePool meta, StoragePool data) {
        this.meta = meta;
        this.data = data;
    }

    @Override
    public StoragePool getStoragePool(StorageOperate operate) {
        switch (operate.getPoolType()) {
            case DEFAULT_META:
            case META:
                return meta;
            case DATA:
            case DEFAULT_DATA:
                return data;
            default:
                return null;
        }
    }

    @Override
    public StoragePool[] getStoragePools(StorageOperate operate) {
        switch (operate.getPoolType()) {
            case DEFAULT_META:
            case META:
                return metas;
            case DATA:
            case DEFAULT_DATA:
                return datas;
            default:
                return null;
        }
    }
}
