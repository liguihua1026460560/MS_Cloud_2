package com.macrosan.storage.strategy.select;

import com.macrosan.storage.StoragePool;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author gaozhiyuan
 */
public class RandomSelector implements Selector {
    static ThreadLocalRandom random = ThreadLocalRandom.current();

    @Override
    public StoragePool select(StoragePool[] pool) {
        int index = random.nextInt(pool.length);
        return pool[index];
    }

    @Override
    public StoragePool[] sort(StoragePool[] pool) {
        StoragePool[] res = Arrays.copyOf(pool, pool.length);
        StoragePool tmp;

        for (int i = res.length - 1; i > 0; i--) {
            int j = random.nextInt(i - 1);
            tmp = res[j];
            res[j] = res[i];
            res[i] = tmp;
        }

        return res;
    }

}
