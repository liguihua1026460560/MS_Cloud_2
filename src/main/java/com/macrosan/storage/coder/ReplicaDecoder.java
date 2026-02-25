package com.macrosan.storage.coder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.doubleActive.MsClientRequest;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class ReplicaDecoder implements Decoder {

    private class DecoderSubscriber extends BaseSubscriber<byte[]> {
        private final MsHttpRequest request;
        private MsClientRequest clientRequest;
        private int index;
        private long curGetLen;
        private Handler<Void> handler;

        private DecoderSubscriber(int i, MsHttpRequest request, MsClientRequest clientRequest) {
            this.index = i;
            this.request = request;
            this.clientRequest = clientRequest;
        }

        @Override
        public void hookOnSubscribe(Subscription s) {
            handler = v -> s.cancel();
            Optional.ofNullable(request).ifPresent(request -> {
                request.addResponseCloseHandler(handler);
            });
            Optional.ofNullable(clientRequest).ifPresent(request -> {
                request.addResponseCloseHandler(handler);
            });

            if (requestN.get() > 0L) {
                s.request(requestN.get());
            }
        }

        @Override
        public void hookOnNext(byte[] bytes) {
            if (curGetLen + bytes.length < getLen) {
                curGetLen += bytes.length;
            } else if (curGetLen < getLen) {
                int needByteLen = (int) (curGetLen + bytes.length - getLen);
                byte[] res = new byte[needByteLen];
                System.arraycopy(bytes, (int) (getLen - curGetLen), res, 0, needByteLen);
                resFlux.onNext(res);
                curGetLen += bytes.length;
                getLen += needByteLen;
            } else {
                resFlux.onNext(bytes);
                curGetLen += bytes.length;
                getLen += bytes.length;
            }

            if (getLen >= objectLen) {
                resFlux.onComplete();
            }

            request(1L);
        }

        @Override
        public void hookOnError(Throwable t) {
            if (t instanceof MsException && (((MsException) t).getErrCode() == ErrorNo.GET_OBJECT_CANCELED || ((MsException) t).getErrCode() == ErrorNo.GET_OBJECT_TIME_OUT)) {
                resFlux.onError(t);
            } else {
                log.error("DecoderSubscriber onError", t);
                hookOnComplete();
            }
            Optional.ofNullable(request).ifPresent(request -> {
                request.removeResponseCloseHandler(handler);
            });
            Optional.ofNullable(clientRequest).ifPresent(request -> {
                request.removeResponseCloseHandler(handler);
            });
        }

        @Override
        public void hookOnComplete() {
            lastIndex.remove(index);

            if (runningIndex == index) {
                if (getLen < objectLen) {
                    if (lastIndex.isEmpty()) {
                        resFlux.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "get object fail"));
                    } else {
                        int nextIndex = lastIndex.iterator().next();
                        subscriber[nextIndex].request0(1L);
                        runningIndex = nextIndex;
                    }
                } else {
                    resFlux.onComplete();
                }
            }
        }

        AtomicLong requestN = new AtomicLong(0L);

        public void request0(long n) {
            requestN.addAndGet(n);
            Subscription s = upstream();
            if (s != null) {
                s.request(requestN.getAndAdd(-n));
            }
        }
    }

    private UnicastProcessor<byte[]> resFlux = UnicastProcessor.create(Queues.<byte[]>unboundedMultiproducer().get());
    private long objectLen;
    private long getLen;
    private int runningIndex = 0;
    // 从0开始遍历每个数据块，出错就换下一个下载
    private Set<Integer> lastIndex = new HashSet<>();
    private DecoderSubscriber[] subscriber;

    private LinkedHashSet<Integer> checkIndexSet;

    public ReplicaDecoder(int k, int m, Flux<byte[]>[] dataFluxes, long objectLen, MsHttpRequest request, MsClientRequest clientRequest) {
        if (dataFluxes.length != k + m) {
            log.error("input dataFluxes length error");
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "");
        }

        this.objectLen = objectLen;

        subscriber = new DecoderSubscriber[k + m];
        for (int i = 0; i < subscriber.length; i++) {
            lastIndex.add(i);
            subscriber[i] = new DecoderSubscriber(i, request, clientRequest);
        }

        for (int i = 0; i < subscriber.length; i++) {
            dataFluxes[i].subscribe(subscriber[i]);
        }

        String header = "";
        if (request != null) {
            header = request.getHeader(SysConstants.GET_CHUNKS_INDEX);
        }
        if (StringUtils.isNotBlank(header)) {
            checkIndexSet = Json.decodeValue(header, new TypeReference<LinkedHashSet<Integer>>() {
            });
        }

        int i = 0;
        if (checkIndexSet != null) {
            if (checkIndexSet.size() != 1) {
                log.debug("checkIndexes length error");
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "");
            }
            i = checkIndexSet.toArray(new Integer[0])[0];
            int finalI = i;
            lastIndex.removeIf(integer -> integer != finalI);
        }

        subscriber[i].request(1);
    }

    @Override
    public Flux<byte[]> res() {
        return resFlux;
    }
}
