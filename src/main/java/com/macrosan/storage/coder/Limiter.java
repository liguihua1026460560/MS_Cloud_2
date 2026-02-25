package com.macrosan.storage.coder;

import io.vertx.core.streams.ReadStream;
import lombok.extern.log4j.Log4j2;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class Limiter {
    private static final int DEFAULT_ALLOW_PUT_PAYLOAD_NUM = 16;
    private static int ALLOW_PUT_PAYLOAD_NUM;
    public static final long DEFAULT_FETCH_AMOUNT = 16;

    static {
        String i = System.getProperty("macrosan.server.maxAllowPutPayloadNum");
        try {
            int c = Integer.parseInt(i);
            if (c <= 0) {
                ALLOW_PUT_PAYLOAD_NUM = 0;
            } else {
                ALLOW_PUT_PAYLOAD_NUM = c;
            }
        } catch (Exception e) {
            ALLOW_PUT_PAYLOAD_NUM = DEFAULT_ALLOW_PUT_PAYLOAD_NUM;
        }

        log.info("{} {}", i, ALLOW_PUT_PAYLOAD_NUM);
    }

    /**
     * 已经向下游发送数据块的个数
     */
    protected long publish = 0;
    /**
     * 等待向下游flush的数据块数量
     */
    private long requestN = 0;
    /**
     * 上游发送的数据块个数
     */
    private long fetchN = 0;
    /**
     * 等待上游发送的数据块个数
     * （已从上游fetch的数据块个数）
     */
    private long waitFetchN = 0;
    /**
     * 原始http请求，作为上游
     */
    private ReadStream request;
    /**
     * k+m个数据块分别需要的数据块个数
     */
    private long[] requestNs;

    private int k;


    public Limiter(ReadStream request, int num, int k) {
        this.k = k;
        this.request = request;
        requestNs = new long[num];
    }

    /**
     * 向下游发送了一次数据
     */
    protected void addPublish() {
        publish++;
    }

    /**
     * 从上游获得了一次数据
     */
    public void addFetchN() {
        fetchN++;
        tryFetch();
    }

    /**
     * 下游增加需要的请求数据个数
     * 在k个下游的数据块都需要数据块的时候
     * 实际增加等待的数据块数量
     *
     * @param index 数据块索引
     * @param n     请求N个
     */
    public void request(int index, long n) {
        if (n == Long.MAX_VALUE) {
            requestNs[index] = n;
        } else {
            requestNs[index] += n;
        }

        if (requestNs[index] > requestN) {
            long minRequest = Long.MAX_VALUE;
            long min = Long.MAX_VALUE;
            int count = 0;
            for (int i = 0; i < requestNs.length; i++) {
                if (requestNs[i] > requestN) {
                    count++;
                    if (requestNs[i] < minRequest) {
                        minRequest = requestNs[i];
                    }
                }

                if (requestNs[i] < min) {
                    min = requestNs[i];
                }
            }

            if (count >= requestNs.length || (count >= k && minRequest - min < ALLOW_PUT_PAYLOAD_NUM)) {
                request(minRequest - requestN);
            }
        }
    }

    /**
     * 实际增加等待向下游flush的数据块的个数
     *
     * @param n 请求N个
     */
    private void request(long n) {
        requestN += n;
        tryFetch();
    }

    /**
     * 根据当前上下游情况尝试从上游请求数据
     * 每次request.fetch都是从上游请求数据
     */
    private synchronized void tryFetch() {
        //当向下游发送的数据块数量没有达到需求数量，且上游put的数据块个数大于已从request获取（fetch）的数据块个数，
        //则继续从httpRequest中获取数据。（一次从pause状态的request中fetch16个byte[]）
        if (publish < requestN) {
            if (fetchN >= waitFetchN) {
                request.fetch(DEFAULT_FETCH_AMOUNT);
                waitFetchN += DEFAULT_FETCH_AMOUNT;
            }
        }
    }
}
