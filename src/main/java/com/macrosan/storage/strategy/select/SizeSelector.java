package com.macrosan.storage.strategy.select;

import com.macrosan.storage.StoragePool;
import lombok.extern.log4j.Log4j2;

import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class SizeSelector implements Selector {
    public double maxRate;
    public double minRemainSize;

    public SizeSelector(double maxRate, long minRemainSize) {
        this.maxRate = maxRate;
        this.minRemainSize = minRemainSize;
    }

    @Override
    public StoragePool select(StoragePool[] pools) {
        long total = 0L;
        long[] nums = new long[pools.length];
        for (int i = 0; i < pools.length; i++) {
            StoragePool pool = pools[i];
            long lastSize = pool.getCache().totalSize - pool.getCache().size;
            double rate = ((double) pool.getCache().size) / pool.getCache().totalSize;
            if (lastSize > minRemainSize && rate < maxRate) {
                nums[i] = pool.getCache().totalSize - pool.getCache().size;
                total += nums[i];
            } else {
                nums[i] = 0;
            }
        }

        if (total == 0) {
            int index = ThreadLocalRandom.current().nextInt(pools.length);
            return pools[index];
        }

        long n = ThreadLocalRandom.current().nextLong(total);
        for (int i = 0; i < pools.length; i++) {
            if (n < nums[i]) {
                return pools[i];
            } else {
                n -= nums[i];
            }
        }

        return pools[0];
    }

    @Override
    public StoragePool[] sort(StoragePool[] pools) {
        long total = 0L;
        long[] nums = new long[pools.length];
        LinkedList<Integer> last = new LinkedList<>();

        for (int i = 0; i < pools.length; i++) {
            StoragePool pool = pools[i];
            nums[i] = pool.getCache().totalSize - pool.getCache().size;
            total += nums[i];
            last.add(i);
        }

        if (total == 0) {
            return pools;
        }

        StoragePool[] res = new StoragePool[pools.length];

        for (int i = 0; i < pools.length - 1; i++) {
            long n = ThreadLocalRandom.current().nextLong(total);
            int index = 0;
            for (int j : last) {
                if (n < nums[j]) {
                    index = j;
                    break;
                } else {
                    n -= nums[j];
                }
            }

            res[i] = pools[index];
            last.remove(Integer.valueOf(index));
            total -= nums[index];
        }

        res[pools.length - 1] = pools[last.poll()];
        return res;
    }
}
