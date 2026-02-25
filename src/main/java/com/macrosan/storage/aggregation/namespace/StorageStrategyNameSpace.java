package com.macrosan.storage.aggregation.namespace;

import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.AggregateConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * 对某个存储策略下的小文件进行聚合操作
 */
public class StorageStrategyNameSpace implements NameSpace {

    private final String storageStrategy;

    private AggregateConfig aggregateConfig;

    public StorageStrategyNameSpace(String storageStrategy) {
        this(storageStrategy, new AggregateConfig());
    }

    public StorageStrategyNameSpace(String storageStrategy, AggregateConfig aggregateConfig) {
        this.storageStrategy = storageStrategy;
        this.aggregateConfig = aggregateConfig;
    }

    @Override
    public String getNameSpaceIdentifier() {
        return "SS_" + storageStrategy;
    }

    @Override
    public void setAggregateConfig(AggregateConfig aggregateConfig) {
        this.aggregateConfig = aggregateConfig;
    }

    @Override
    public AggregateConfig getAggregateConfig() {
        return aggregateConfig;
    }

    @Override
    public StoragePool getStoragePool(StorageOperate operate) {
        return StoragePoolFactory.getStoragePoolByStrategyName(storageStrategy, operate);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        } else if (this == obj) {
            return true;
        } else if (obj instanceof StorageStrategyNameSpace) {
            StorageStrategyNameSpace other = (StorageStrategyNameSpace) obj;
            return getNameSpaceIdentifier().equals(other.getNameSpaceIdentifier());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getNameSpaceIdentifier().hashCode();
    }
}
