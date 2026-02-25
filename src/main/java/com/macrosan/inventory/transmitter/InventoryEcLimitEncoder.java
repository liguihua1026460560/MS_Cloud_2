package com.macrosan.inventory.transmitter;

import com.macrosan.inventory.datasource.DataSource;
import com.macrosan.storage.codec.ErasureCodc;
import com.macrosan.storage.coder.ErasureEncoder;

public class InventoryEcLimitEncoder extends ErasureEncoder implements InventoryEncoder {

    private InventoryLimiter limiter;

    public InventoryEcLimitEncoder(int k, int m, int packageSize, ErasureCodc codc, DataSource dataSource) {
        super(k, m, packageSize, codc);
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
