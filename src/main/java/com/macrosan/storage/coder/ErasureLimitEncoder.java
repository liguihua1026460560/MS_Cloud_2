package com.macrosan.storage.coder;

import com.macrosan.storage.codec.ErasureCodc;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

/**
 * 在ErasureEncoder的基础上增加限流，限制内存的使用
 *
 * @author gaozhiyuan
 */
@Log4j2
public class ErasureLimitEncoder extends ErasureEncoder implements LimitEncoder {
    @Getter
    private Limiter limiter;


    public ErasureLimitEncoder(int k, int m, int packageSize, ErasureCodc codc, ReadStream request) {
        super(k, m, packageSize, codc);
        limiter = new Limiter(request, super.data().length, k);
    }

    /**
     * 向下游发送了一次数据
     */
    @Override
    public void flush() {
        super.flush();
        limiter.addPublish();
    }

    /**
     * 从上游获得了一次数据
     */
    @Override
    public void put(byte[] bytes) {
        super.put(bytes);
        limiter.addFetchN();
    }

    @Override
    public void put(Buffer bytes) {
        super.put(bytes);
        limiter.addFetchN();
    }

    /**
     * 下游增加需要的请求数据个数
     * 在k个下游的数据块都需要数据块的时候
     * 实际增加等待的数据块数量
     *
     * @param index 数据块索引
     * @param n     请求N个
     */
    @Override
    public void request(int index, long n) {
        limiter.request(index, n);
    }
}
