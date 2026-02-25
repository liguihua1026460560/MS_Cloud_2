package com.macrosan.storage.coder;

import com.macrosan.fs.BlockDevice;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.codec.ErasureCodc;
import io.vertx.core.streams.ReadStream;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhaoyang
 * @date 2025/09/25
 **/
@Slf4j
public class ErasurePrefetchLimitEncoder extends ErasureLimitEncoder {
    public static final boolean DISABLE_PREFETCH;
    public static final int MAX_INFLIGHT_PUBLISH_NUM;

    static {
        DISABLE_PREFETCH = Boolean.parseBoolean(System.getProperty("macrosan.prefetch.disable", "false"));
        MAX_INFLIGHT_PUBLISH_NUM = Integer.parseInt(System.getProperty("macrosan.prefetch.maxInflightPublish", "12"));
        log.info("prefetch is {} maxInflightFlush:{}", DISABLE_PREFETCH ? "disable" : "enable", MAX_INFLIGHT_PUBLISH_NUM);
    }

    private final int chunkNum;
    private final int publishNumForOneFlush;
    private long[] completeNum;
    private int maxInflightPublishNum;

    public ErasurePrefetchLimitEncoder(int k, int m, int packageSize, ErasureCodc codc, ReadStream request) {
        super(k, m, packageSize, codc, request);
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
            if (inFlightPublishNum >= maxInflightPublishNum) {
                log.debug("publish:{} complete:{}",getLimiter().publish,completeNum[i]);
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
