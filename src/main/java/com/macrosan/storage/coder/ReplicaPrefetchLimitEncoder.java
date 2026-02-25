package com.macrosan.storage.coder;

import com.macrosan.fs.BlockDevice;
import com.macrosan.storage.StoragePoolFactory;
import io.vertx.core.streams.ReadStream;
import lombok.extern.log4j.Log4j2;

import static com.macrosan.storage.coder.ErasurePrefetchLimitEncoder.MAX_INFLIGHT_PUBLISH_NUM;

/**
 * @author zhaoyang
 * @date 2025/10/22
 **/
@Log4j2
public class ReplicaPrefetchLimitEncoder extends ReplicaLimitEncoder{
    private final int chunkNum;
    private final int publishNumForOneFlush;
    private long[] completeNum;
    private int maxInflightPublishNum;
    public ReplicaPrefetchLimitEncoder(int k, int m, int packageSize, ReadStream request) {
        super(k, m, packageSize, request);
        chunkNum = k + m;
        publishNumForOneFlush = (BlockDevice.MIN_ALLOC_SIZE / StoragePoolFactory.DEFAULT_PACKAGE_SIZE);
        completeNum = new long[chunkNum];
        getMaxInflightPublishNum();
    }

    public void getMaxInflightPublishNum() {
        if (chunkNum <= 6) {
            maxInflightPublishNum = MAX_INFLIGHT_PUBLISH_NUM;
        } else if (chunkNum <= 9) {
            maxInflightPublishNum = MAX_INFLIGHT_PUBLISH_NUM / 2;
        } else if (chunkNum <= 12) {
            maxInflightPublishNum = MAX_INFLIGHT_PUBLISH_NUM / 4;
        } else {
            maxInflightPublishNum = MAX_INFLIGHT_PUBLISH_NUM / 6;
        }
    }
    @Override
    public void flush() {
        super.flush();
        tryPrefetch();
    }

    @Override
    public void completeN(int index, long n) {
        if (n == Long.MAX_VALUE) {
            completeNum[index] = Long.MAX_VALUE;
        } else {
            completeNum[index] += n;
        }
    }

    private void tryPrefetch() {
        if (getLimiter().publish < publishNumForOneFlush) {
            return;
        }
        for (int i = 0; i < chunkNum; i++) {
            if (completeNum[i] == Long.MAX_VALUE) {
                continue;
            }
            long inFlightPublishNum = getLimiter().publish - completeNum[i];
            if (inFlightPublishNum / publishNumForOneFlush >= maxInflightPublishNum) {
                log.debug("inFlight flush num is:{}", inFlightPublishNum / publishNumForOneFlush);
                return;
            }
        }
        for (int i = 0; i < chunkNum; i++) {
            if (completeNum[i] == Long.MAX_VALUE) {
                request(i, Long.MAX_VALUE);
            } else {
                request(i, 1);
            }
        }
    }
}
