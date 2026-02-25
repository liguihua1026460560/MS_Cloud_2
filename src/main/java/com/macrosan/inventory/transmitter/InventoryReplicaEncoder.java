package com.macrosan.inventory.transmitter;

import com.macrosan.inventory.datasource.DataSource;
import com.macrosan.storage.coder.ReplicaEncoder;

public class InventoryReplicaEncoder extends ReplicaEncoder implements InventoryEncoder {

    private InventoryLimiter limiter;

    public InventoryReplicaEncoder(int k, int m, int packageSize, DataSource dataSource) {
        super(k, m, packageSize);
        limiter = new InventoryLimiter(dataSource, super.data().length, k);
    }

    @Override
    public void put(byte[] bytes) {
        super.put(bytes);
        limiter.addFetchN();
    }

    @Override
    public void flush() {
        super.flush();
        limiter.addPublish();
    }

    @Override
    public void request(int index, long n) {
        limiter.request(index, n);
    }
}
