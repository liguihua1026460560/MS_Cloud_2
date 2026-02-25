package com.macrosan.storage.aggregation.namespace;

import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.aggregation.AggregateConfig;

public interface NameSpace {
    String getNameSpaceIdentifier();
    void setAggregateConfig(AggregateConfig aggregateConfig);
    AggregateConfig getAggregateConfig();
    StoragePool getStoragePool(StorageOperate operate);
}
