package com.macrosan.storage.aggregation.manager;

import com.macrosan.storage.aggregation.aggregator.CompactionAggregateContainerWrapper;
import com.macrosan.storage.aggregation.aggregator.container.AggregateContainer;
import com.macrosan.storage.aggregation.namespace.NameSpace;
import lombok.Getter;

public class CompactionContainerManager extends ActiveContainerManager<CompactionAggregateContainerWrapper> {
    @Getter
    private static final CompactionContainerManager instance = new CompactionContainerManager();
    private CompactionContainerManager() {
        super();
    }
    @Override
    protected CompactionAggregateContainerWrapper createContainerWrapper(NameSpace namespace, AggregateContainer container) {
        return new CompactionAggregateContainerWrapper(namespace, container);
    }
}
