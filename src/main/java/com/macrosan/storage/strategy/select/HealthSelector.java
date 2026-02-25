package com.macrosan.storage.strategy.select;

import com.macrosan.storage.PoolHealth.HealthState;
import com.macrosan.storage.StoragePool;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class HealthSelector extends SizeSelector {

    public HealthSelector(double maxRate, long minRemainSize) {
        super(maxRate, minRemainSize);
    }

    @Override
    public StoragePool select(StoragePool[] pool) {
        if (pool.length == 1) {
            return pool[0];
        }

        for (HealthState state : HealthState.values()) {
            StoragePool[] healthPool =
                    Arrays.stream(pool).filter(p -> p.getState() == state).toArray(StoragePool[]::new);

            if (healthPool.length > 0) {
                return super.select(healthPool);
            }
        }

        return pool[0];
    }

    @Override
    public StoragePool[] sort(StoragePool[] pool) {
        List<StoragePool> list = new LinkedList<>();

        for (HealthState state : HealthState.values()) {
            if (state != HealthState.Broken) {
                StoragePool[] healthPool =
                        Arrays.stream(pool).filter(p -> p.getState() == state).toArray(StoragePool[]::new);

                if (healthPool.length > 0) {
                    StoragePool[] pools = super.sort(healthPool);
                    Collections.addAll(list, pools);
                }
            }
        }

        return list.toArray(new StoragePool[0]);
    }
}
