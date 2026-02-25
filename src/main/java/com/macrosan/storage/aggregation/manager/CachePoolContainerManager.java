package com.macrosan.storage.aggregation.manager;

import com.macrosan.storage.aggregation.aggregator.CachePoolAggregateContainerWrapper;
import com.macrosan.storage.aggregation.aggregator.container.AggregateContainer;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import lombok.Getter;

public class CachePoolContainerManager extends ActiveContainerManager<CachePoolAggregateContainerWrapper> {
    @Getter
    private static CachePoolContainerManager instance = new CachePoolContainerManager();
    private CachePoolContainerManager() {super();}
    @Override
    protected CachePoolAggregateContainerWrapper createContainerWrapper(NameSpace namespace, AggregateContainer container) {
        return new CachePoolAggregateContainerWrapper(namespace, container);
    }
}
