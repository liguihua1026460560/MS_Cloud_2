package com.macrosan.storage.client;

import com.macrosan.constants.ErrorNo;
import com.macrosan.doubleActive.MsClientRequest;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.coder.Decoder;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class GetObjectClientHandler {
    private static Payload COMPLETE_GET_OBJECT_PAYLOAD = DefaultPayload.create("", COMPLETE_GET_OBJECT.name());
    private long curStripe = 0L;
    private long[] requestCount;
    private boolean[] first;

    UnicastProcessor<Tuple2<Integer, Long>> downLoadController = UnicastProcessor.create(Queues.<Tuple2<Integer, Long>>unboundedMultiproducer().get());
    private Disposable controllerSubscribe;
    private Decoder decoder;
    private Handler<Void> closeHandler;
    private Handler<Throwable> errorHandler;
    private int curStream = 0;
    private List<UnicastProcessor<Payload>> publisher;

    /**
     * 当前所在条块的头一个字节在整个数据流的位置
     */
    private long resCount = 0L;
    private long start;
    private long end;
    private String fileName;
    private StoragePool storagePool;
    private MsHttpRequest request;
    List<SocketReqMsg> msgs;
    private MsClientRequest clientRequest;

    final public static String DATA_CORRUPTED_MESSAGE = "data may have been corrupted!";

    /**
     * 生成和其他节点通信的UnicastProcessor
     */
    private List<SocketReqMsg> buildMsgs(String fileName, long fileSize, long startStripe, long endStripe,
                                         List<Tuple3<String, String, String>> nodeList) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("packetSize", String.valueOf(storagePool.getPackageSize()))
                .put("startStripe", String.valueOf(startStripe))
                .put("endStripe", String.valueOf(endStripe))
                .put("fileSize", String.valueOf(fileSize))
                .put("fileName", fileName);

        return nodeList.stream()
                .map(tuple -> msg.copy().put("lun", tuple.var2))
                .collect(Collectors.toList());
    }

    /**
     * 订阅控制流，控制下载是读文件的进度
     * 一个stripe的数据完整的发送给客户端后
     * 实际返回数据时每次发送一个strip大小的数据，
     * 发送k次则完整的发送了一个stripe的数据
     *
     * @param stripeCount 总共有stripeCount个stripe需要发送
     * @param publisher   和其他节点通信的UnicastProcessor
     */
    private void subscribeDownController(long stripeCount, List<UnicastProcessor<Payload>> publisher) {
        controllerSubscribe = downLoadController.subscribe(t -> {
            if (t.var1 >= 0) {
                int index = t.var1;
                long num = requestCount[index] - curStripe;
                if (num <= 0) {
                    for (long c = 0; c < t.var2; c++) {
                        if (first[index]) {
                            publisher.get(index).onNext(DefaultPayload.create(Json.encode(msgs.get(index)), GET_OBJECT.name()));
                            first[index] = false;
                        } else {
                            publisher.get(index).onNext(DefaultPayload.create("", GET_OBJECT.name()));
                        }
                    }
                }

                requestCount[index] += t.var2;
            } else {
                if (++curStream == storagePool.getK()) {
                    curStripe++;
                    if (curStripe <= stripeCount) {
                        for (int i = 0; i < publisher.size(); i++) {
                            if (curStripe < requestCount[i]) {
                                publisher.get(i).onNext(DefaultPayload.create("", GET_OBJECT.name()));
                            }
                        }

                    } else {
                        for (UnicastProcessor<Payload> processor : publisher) {
//                            processor.onNext(DefaultPayload.create("", COMPLETE_GET_OBJECT.name()));
                            processor.onComplete();
                        }

                        controllerSubscribe.dispose();
                    }

                    curStream = 0;
                }
            }
        });
    }

    /**
     * 从其他节点获得数据并转换成Decoder需要的数据流
     *
     * @param publisher 发送消息的生产者
     * @param nodeList  节点信息
     * @return 转换后的数据流
     */
    private Flux<byte[]>[] transformData(List<SocketReqMsg> msgs, List<UnicastProcessor<Payload>> publisher, List<Tuple3<String, String, String>> nodeList) {
        UnicastProcessor<byte[]>[] dataProcessors = new UnicastProcessor[nodeList.size()];
        Flux<byte[]>[] dataFluxes = new Flux[nodeList.size()];

        ClientTemplate.ResponseInfo<byte[]> responseInfo = ClientTemplate.multiResponse(publisher, byte[].class, nodeList);
        for (int i = 0; i < dataFluxes.length; i++) {
            dataProcessors[i] = UnicastProcessor.create();
            int index = i;
            dataFluxes[i] = dataProcessors[i].doOnRequest(l -> {
                downLoadController.onNext(new Tuple2<>(index, l));
            });
        }

        Disposable subscribe = responseInfo.responses.doOnNext(t -> {
            if (t.var2 == CONTINUE) {
                dataProcessors[t.var1].onNext(t.var3);
            } else if (t.var2 == TIME_OUT) {
                dataProcessors[t.var1].onError(new MsException(ErrorNo.GET_OBJECT_TIME_OUT, "get object " + fileName + " time out!"));
            } else {
                dataProcessors[t.var1].onComplete();
            }
        }).doOnComplete(() -> {
            for (int i = 0; i < dataFluxes.length; i++) {
                dataProcessors[i].onComplete();
            }
        }).subscribe();

        closeHandler = v -> {
            for (UnicastProcessor<Payload> processor : publisher) {
                processor.onNext(ERROR_PAYLOAD);
                processor.onComplete();
            }
            subscribe.dispose();
            controllerSubscribe.dispose();
        };
        errorHandler = e -> {
            closeHandler.handle(null);
            // 将异常传递到 Decoder中
            for (UnicastProcessor<byte[]> dataProcessor : dataProcessors) {
                dataProcessor.onError(e);
            }
        };
        Optional.ofNullable(request).ifPresent(r -> {
            r.addResponseCloseHandler(closeHandler);
        });

        Optional.ofNullable(clientRequest).ifPresent(r -> {
            r.addResponseCloseHandler(closeHandler);
        });

        return dataFluxes;
    }

    public GetObjectClientHandler(StoragePool storagePool, long start, long end, String fileName, long fileSize,
                                  List<Tuple3<String, String, String>> nodeList, UnicastProcessor<Long> streamController,
                                  MsHttpRequest request, MsClientRequest clientRequest) {
        this.start = start;
        this.end = end;
        this.fileName = fileName;
        this.storagePool = storagePool;
        this.requestCount = new long[nodeList.size()];
        this.first = new boolean[nodeList.size()];
        Arrays.fill(first, true);
        this.request = request;
        this.clientRequest = clientRequest;

        long startStripe = start / (storagePool.getK() * storagePool.getPackageSize());
        long endStripe = end / (storagePool.getK() * storagePool.getPackageSize());
        long length = (endStripe - startStripe + 1) * storagePool.getK() * storagePool.getPackageSize();
        long off = (end + 1) % (storagePool.getK() * storagePool.getPackageSize());
        if (off == 0) {
            fileSize = length;
        } else {
            fileSize = length - (storagePool.getK() * storagePool.getPackageSize() - off);
        }

        long stripeCount = endStripe - startStripe + 1;
        // 当前所在条块的头一个字节在整个数据流的位置
        resCount = startStripe * storagePool.getK() * storagePool.getPackageSize();
        if (resCount < 0) {
            resCount = 0;
        }

        msgs = buildMsgs(fileName, fileSize, startStripe, endStripe, nodeList);

        publisher = msgs.stream()
                .map(m -> UnicastProcessor.create(Queues.<Payload>unboundedMultiproducer().get()))
                .collect(Collectors.toList());

        streamController.subscribe(l -> downLoadController.onNext(new Tuple2<>(-1, l)), e -> errorHandler.handle(e));

        Flux<byte[]>[] dataFluxes = transformData(msgs, publisher, nodeList);

        decoder = storagePool.getDecoder(dataFluxes, length, fileSize, request, clientRequest);

        subscribeDownController(stripeCount, publisher);
    }


    /**
     * 截取读取结果中从start到end的数据
     */
    public Flux<byte[]> getBytes() {
        return decoder.res()
                .map(bytes -> {
                    int len = bytes.length;
                    try {
                        //截取start-end中间的bytes。对两种边界情况特殊处理。
                        if (resCount < start) {
                            if (resCount + bytes.length > start) {
                                bytes = Arrays.copyOfRange(bytes, (int) (start - resCount), bytes.length);
                                len -= start - resCount;
                                resCount = start;
                            } else {
                                return new byte[0];
                            }
                        }

                        if (resCount + bytes.length > end) {
                            if (resCount <= end) {
                                bytes = Arrays.copyOfRange(bytes, 0, (int) (end - resCount + 1));
                            } else {
                                bytes = new byte[0];
                            }
                        }

                        return bytes;
                    } finally {
                        resCount += len;
                    }
                })
                .doFinally(s -> {
                    Optional.ofNullable(request).ifPresent(r -> r.removeResponseCloseHandler(closeHandler));
                    for (UnicastProcessor<Payload> processor : publisher) {
                        processor.onNext(COMPLETE_GET_OBJECT_PAYLOAD);
                        processor.onComplete();
                    }
                })
                .onErrorMap(e -> {
                    if (e instanceof MsException && (((MsException) e).getErrCode() == ErrorNo.GET_OBJECT_CANCELED || ((MsException) e).getErrCode() == ErrorNo.GET_OBJECT_TIME_OUT)) {
                        return e;
                    }
                    return new MsException(ErrorNo.UNKNOWN_ERROR, "get object " + fileName + " fail ," + DATA_CORRUPTED_MESSAGE);
                });
    }
}
