package com.macrosan.storage.coder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.doubleActive.MsClientRequest;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.storage.codec.ErasureCodc;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * 根据输入的流，反向计算出原始的数据块
 * 要求输入的流除了最后一块，其它块的大小都是packetSize
 *
 * @author gaozhiyuan
 */
@Log4j2
public class ErasureDecoder implements Decoder {
    private class DecoderSubscriber implements Subscriber<byte[]> {
        private Subscription s;
        private int index;
        private long count = 0;
        private final MsHttpRequest request;
        private Handler<Void> handler;
        private MsClientRequest clientRequest;

        private DecoderSubscriber(int i, MsHttpRequest request, MsClientRequest clientRequest) {
            index = i;
            this.request = request;
            this.clientRequest = clientRequest;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
            handler = v -> s.cancel();
            Optional.ofNullable(request).ifPresent(request -> {
                request.addResponseCloseHandler(handler);
            });
            Optional.ofNullable(clientRequest).ifPresent(request -> {
                request.addResponseCloseHandler(handler);
            });
        }

        @Override
        public void onNext(byte[] bytes) {
            if (checkIndexSet != null && !checkIndexSet.contains(index)) {
                processor.onNext(new Tuple3<>(index, count - 1, null));
            } else{
                processor.onNext(new Tuple3<>(index, count++, bytes));
            }
        }

        @Override
        public void onError(Throwable t) {
            if (t instanceof MsException && (((MsException) t).getErrCode() == ErrorNo.GET_OBJECT_CANCELED || ((MsException) t).getErrCode() == ErrorNo.GET_OBJECT_TIME_OUT)) {
                processor.onError(t);
            } else {
                log.error("DecoderSubscriber onError", t);
                processor.onNext(new Tuple3<>(index, count - 1, null));
            }

            Optional.ofNullable(request).ifPresent(request -> {
                request.removeResponseCloseHandler(handler);
            });
            Optional.ofNullable(clientRequest).ifPresent(request -> {
                request.removeResponseCloseHandler(handler);
            });

        }

        @Override
        public void onComplete() {
            if (this.count < total) {
                processor.onNext(new Tuple3<>(index, count - 1, null));
            }

            Optional.ofNullable(request).ifPresent(request -> {
                request.removeResponseCloseHandler(handler);
            });
            Optional.ofNullable(clientRequest).ifPresent(request -> {
                request.removeResponseCloseHandler(handler);
            });

        }

        public void request(long n) {
            s.request(n);
        }
    }

    private byte[][] srcBytes;

    /**
     * 发布反向计算后的原始数据流
     */
    private UnicastProcessor<byte[]> resFlux = UnicastProcessor.create();

    private UnicastProcessor<Tuple3<Integer, Long, byte[]>> processor = UnicastProcessor.create(
            Queues.<Tuple3<Integer, Long, byte[]>>unboundedMultiproducer().get());

    private int[] notLost;

    private long decodeCount = 0;
    private long objectLen;
    private long total;
    private int count = 0;
    private boolean normal = true;
    private int[] nums;
    private Set<Integer> numsSet;
    private Set<Integer> allSet;

    private DecoderSubscriber[] subscriber;
    private Handler<Void> closeHandler;
    private MsHttpRequest request;
    private int k;
    private int m;
    private int packetSize;
    private ErasureCodc codc;
    private MsClientRequest clientRequest;

    private Set<Integer> checkIndexSet;

    public ErasureDecoder(int k, int m, int packetSize, ErasureCodc codc,
                          Flux<byte[]>[] dataFluxes, long objectLen, MsHttpRequest request, MsClientRequest clientRequest) {
        this.k = k;
        this.m = m;
        this.packetSize = packetSize;
        this.codc = codc;

        if (dataFluxes.length != k + m) {
            log.error("input dataFluxes length error");
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "");
        }


        this.request = request;
        this.clientRequest = clientRequest;
        this.objectLen = objectLen;

        srcBytes = new byte[k + m][];
        notLost = new int[k + m];
        nums = new int[k];
        numsSet = new HashSet<>(k + m);
        allSet = new HashSet<>(k + m);
        subscriber = new DecoderSubscriber[k + m];

        long peerSize = packetSize * k;
        total = objectLen / peerSize + (objectLen % peerSize == 0 ? 0 : 1);

        String header = "";
        if (request != null) {
            header = request.getHeader(SysConstants.GET_CHUNKS_INDEX);
        }
        if (StringUtils.isNotBlank(header)) {
            checkIndexSet = Json.decodeValue(header, new TypeReference<LinkedHashSet<Integer>>() {
            });
//            log.info("xxx get checkIndexSet {}", checkIndexSet);
        }
        if (checkIndexSet != null && checkIndexSet.size() != k) {
            log.debug("checkIndexes length error, {}",checkIndexSet);
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "");
        }

        // 接收处理各节点发来的生数据块dataFlux
        for (int i = 0; i < k + m; i++) {
            subscriber[i] = new DecoderSubscriber(i, request, clientRequest);
            dataFluxes[i].subscribe(subscriber[i]);
            if (i < k) {
                nums[i] = i;
                subscriber[i].request(1);
                notLost[i] = 1;
                numsSet.add(i);
            }

            allSet.add(i);
        }

        Disposable subscribe = processor.subscribe(tuple -> {
            try {
//                if (checkIndexSet != null && !checkIndexSet.contains(tuple.var1)) {
//                    tuple.var3 = null;
//                }
                if (tuple.var3 == null) {
                    notLost[tuple.var1] = 0;
                    allSet.remove(tuple.var1);

                    if (numsSet.contains(tuple.var1)) {
                        normal = false;
                        if (tuple.var2 == decodeCount) {
                            count--;
                            srcBytes[tuple.var1] = null;
                        }

                        Iterator<Integer> iterator = allSet.iterator();

                        // 可用数据块不满足k个
                        if (allSet.size() < k) {
                            processor.onComplete();
                            if (!resFlux.isDisposed()) {
                                resFlux.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "get object fail"));
                                return;
                            }
                        } else {
                            numsSet.clear();
                            for (int i = 0; i < k - 1; i++) {
                                nums[i] = iterator.next();
                                numsSet.add(nums[i]);
                            }

                            int next = iterator.next();
                            // 请求数据
                            subscriber[next].request(decodeCount + 1);
                            notLost[next] = 1;
                            nums[k - 1] = next;
                            numsSet.add(next);
                        }

                        return;
                    }
                }

                if (tuple.var2 > decodeCount) {
                    log.error("decode sequence error {} {}", tuple, decodeCount);
                }

                if (tuple.var2 == decodeCount) {
                    srcBytes[tuple.var1] = tuple.var3;
                    count++;
                }

                if (count == k) {
                    flush();
                }
            } catch (Exception e) {
                log.error("decode fail {} {} {} {} {}",
                        numsSet, nums, allSet, Arrays.toString(srcBytes), notLost, e);
            }
        }, e -> resFlux.onError(e));

        closeHandler = v -> subscribe.dispose();
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(closeHandler));
        Optional.ofNullable(clientRequest).ifPresent(r -> r.addResponseCloseHandler(closeHandler));
    }

    private void flush() {
        for (int i : nums) {
            subscriber[i].request(1);
        }

        if (normal) {
            normalFlush();
        } else {
            decodeFlush();
        }

        count = 0;
        decodeCount++;
    }

    private void decodeFlush() {
        byte[][] resBytes;

        if (decodeCount < total - 1) {
            byte[][] needDecodeBytes = new byte[k][];
            for (int i = 0; i < k; i++) {
                needDecodeBytes[i] = srcBytes[nums[i]];
            }

            resBytes = codc.decode(needDecodeBytes, notLost);
            for (int i = 0; i < k; i++) {
                resFlux.onNext(resBytes[i]);
            }
        } else {
            byte[][] needDecodeBytes = new byte[k][packetSize];
            for (int i = 0; i < k; i++) {
                System.arraycopy(srcBytes[nums[i]], 0, needDecodeBytes[i], 0, srcBytes[nums[i]].length);
            }

            resBytes = codc.decode(needDecodeBytes, notLost);

            long lastLen = objectLen - decodeCount * k * packetSize;
            for (int i = 0; i < k; i++) {
                if (lastLen > srcBytes[nums[i]].length) {
                    resFlux.onNext(Arrays.copyOf(resBytes[i], srcBytes[nums[i]].length));
                    lastLen -= srcBytes[nums[i]].length;
                } else {
                    resFlux.onNext(Arrays.copyOf(resBytes[i], (int) lastLen));
                    lastLen = 0;
                }
            }

            resFlux.onComplete();
        }
    }

    private void normalFlush() {
        if (decodeCount < total - 1) {
            for (int i = 0; i < k; i++) {
                resFlux.onNext(srcBytes[i]);
            }
        } else {
            long lastLen = objectLen - decodeCount * k * packetSize;
            for (int i = 0; i < k; i++) {
                if (lastLen > srcBytes[i].length) {
                    resFlux.onNext(srcBytes[i]);
                    lastLen -= srcBytes[i].length;
                } else if (lastLen > 0) {
                    resFlux.onNext(Arrays.copyOf(srcBytes[i], (int) lastLen));
                    lastLen = 0;
                }
            }

            processor.onComplete();
            resFlux.onComplete();
        }
    }

    @Override
    public Flux<byte[]> res() {
        return resFlux.doFinally(s -> {
            Optional.ofNullable(request).ifPresent(r -> r.removeResponseCloseHandler(closeHandler));
            Optional.ofNullable(clientRequest).ifPresent(request -> request.removeResponseCloseHandler(closeHandler));
        });
    }

}
