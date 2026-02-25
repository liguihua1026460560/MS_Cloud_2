package com.macrosan.utils.asm;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import io.rsocket.internal.RateLimitableRequestPublisher;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import org.reactivestreams.Processor;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author gaozhiyuan
 */
public class MsIntObjectHashMap<V> extends SynchronizedIntObjectHashMap<V> {
    private AtomicBoolean end = new AtomicBoolean(false);

    /**
     * 对应connection已经关闭的情况下
     * 直接将对应的value取消
     */
    @Override
    public synchronized V put(int key, V value) {
        if (end.get()) {
            if (value instanceof Processor) {
                try {
                    ((Processor) value).onError(new MsException(ErrorNo.UNKNOWN_ERROR, "closed connection"));
                } catch (Throwable t) {

                }

                return null;
            } else if (value instanceof RateLimitableRequestPublisher) {
                try {
                    ((RateLimitableRequestPublisher) value).cancel();
                } catch (Throwable t) {

                }
                return null;
            } else {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "closed connection");
            }
        } else {
            return super.put(key, value);
        }
    }

    /**
     * RSocketRequester中senders、receivers使用values的时候
     * 对应的连接connection已经关闭
     */
    @Override
    public synchronized Collection<V> values() {
        end.set(true);
        return super.values();
    }
}
