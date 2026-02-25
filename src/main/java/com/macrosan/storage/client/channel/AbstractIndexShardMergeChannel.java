package com.macrosan.storage.client.channel;

import com.google.common.collect.MinMaxPriorityQueue;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.AbstractListClient;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple3;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 用于查询桶下所有分片的数据进行合并以及排序
 */
@Log4j2
public abstract class AbstractIndexShardMergeChannel<T> implements Channel<T> {

    /**
     * 返回一次list是否成功的结果
     */
    protected final MonoProcessor<Boolean> res = MonoProcessor.create();

    /**
     * 发起一次list请求
     */
    protected final MonoProcessor<SocketReqMsg> req = MonoProcessor.create();

    private final StoragePool storagePool;

    private final List<String> shardNodeList;

    private final MsHttpRequest request;

    /**
     * 提供一个同步有界优先队列，用于保存合并分片后的所有结果
     */
    protected Queue<T> queue;

    private final int maxKeys;

    /**
     * 列举对象的起始前缀
     */
    protected String beginPrefix = "";

    protected final String prefix;

    protected final String bucket;
    protected  String currentSnapshotMark;
    protected  String snapshotLink;

    @Setter
    @Getter
    private List<String> updateDirList = new LinkedList<>();

    /***
     * 是否按照range 模式进行列举
     */
    @Setter
    @Getter
    private boolean range = true;

    protected AbstractIndexShardMergeChannel(String bucket, StoragePool storagePool, List<String> shardNodes, MsHttpRequest request, int maxKeys, String prefix) {
        this.bucket = bucket;
        this.storagePool = storagePool;
        this.shardNodeList = shardNodes;
        this.request = request;
        this.maxKeys = maxKeys;
        this.prefix = prefix;

        @SuppressWarnings("UnstableApiUsage")
        MinMaxPriorityQueue<T> queue = MinMaxPriorityQueue
                .orderedBy(comparator())
                .maximumSize(maxKeys + 1)
                .expectedSize(maxKeys + 1)
                .create();

        this.queue = com.google.common.collect.Queues.synchronizedQueue(queue);

        req.publishOn(ErasureServer.DISK_SCHEDULER).subscribe(this::spread, log::error);
    }

    @Override
    public void request(SocketReqMsg msg) { req.onNext(msg);}

    /**
     * 设置列举起始前缀
     * @param prefix 前缀
     * @param marker 上一次列举的标记
     * @param delimiter 分隔符
     */
    public AbstractIndexShardMergeChannel<T> withBeginPrefix(String prefix, String marker, String delimiter) {
        String realMarker;
        if (StringUtils.isBlank(marker) && StringUtils.isBlank(prefix)) {
            realMarker = "";
        } else if (prefix.compareTo(marker) > 0) {
            realMarker = prefix;
        } else if (StringUtils.isNotBlank(delimiter) && marker.endsWith(delimiter)) {
            byte[] bytes = marker.getBytes();
            bytes[bytes.length - 1] += 1;
            realMarker = new String(bytes);
        } else {
            realMarker = marker;
        }
        this.beginPrefix = realMarker;
        return this;
    }

    /**
     * 将桶的List请求广播至各个分片
     * @param msg socket请求信息
     */
    protected void spread(SocketReqMsg msg) {
        String startVnode;
        int startIndex;
        int endIndex;
        int maxRunning;
        // 默认按照range模式列举
        if (range) {
            // 寻找起始分片
            startVnode = storagePool.getBucketVnodeId(bucket, beginPrefix);
            startIndex = shardNodeList.indexOf(startVnode);
            // 寻找结束分片
            endIndex = shardNodeList.size() - 1;
            if (!StringUtils.isBlank(prefix)) {
                byte[] bytes = prefix.getBytes();
                bytes[bytes.length - 1] += 1;
                String endVnode = storagePool.getBucketVnodeId(bucket, new String(bytes));
                endIndex = shardNodeList.indexOf(endVnode);
            }
            maxRunning = 1;
        } else {
            startIndex = 0;
            endIndex = shardNodeList.size() - 1;
            // 默认2个并发进行处理，两两合并
            maxRunning = 2;
        }

        // 分片的数量
        final int shardAmount = endIndex - startIndex + 1;

        UnicastProcessor<String> processor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        int i,j;
        for (i = startIndex,j=0; i <= endIndex && j < maxRunning; i++,j++) {
            processor.onNext(shardNodeList.get(i));
        }

        AtomicInteger nextRunNumber = new AtomicInteger(i);
        AtomicInteger resultNum = new AtomicInteger(0);
        int finalEndIndex = endIndex;

        Disposable[] disposables = new Disposable[3];
        processor.publishOn(ErasureServer.DISK_SCHEDULER).doOnNext(node ->
                disposables[0] = storagePool.mapToNodeInfo(node)
                        .subscribe(infoList -> {
                            String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                            msg.put("vnode", nodeArr[0]);
                            List<SocketReqMsg> msgs = infoList.stream().map(info -> msg.copy().put("lun", info.var2)).collect(Collectors.toList());

                            ClientTemplate.ResponseInfo<T[]> responseInfo = send(msgs, infoList);
                            AbstractListClient<T> listClientHandler = getListClientHandler(storagePool, responseInfo, infoList, request);
                            disposables[1] = responseInfo.responses.subscribe(listClientHandler::handleResponse, e -> log.error("", e), listClientHandler::handleComplete);
                            disposables[2] = listClientHandler.res
                                    .subscribe(b -> {
                                        if (!b) {
                                            log.error("list vnode {} objects error", nodeArr[0]);
                                            res.onNext(false);
                                            processor.onComplete();
                                            return;
                                        }
                                        // 当队列中的数据条数超过maxKeys，则list结束
                                        if ((range && queue.size() >= maxKeys + 1) || resultNum.incrementAndGet() == shardAmount) {
                                            while (!queue.isEmpty()) {
                                                T poll = queue.poll();
                                                handleResult(poll);
                                            }
                                            publishResult();
                                            processor.onComplete();
                                        } else {
                                            int next = nextRunNumber.getAndIncrement();
                                            if (next <= finalEndIndex) {
                                                processor.onNext(shardNodeList.get(next));
                                            }
                                        }
                                    }, e -> {
                                        log.error("list vnode {} objects error {}", nodeArr[0], e);
                                        res.onNext(false);
                                        processor.onComplete();
                                    });

                            Optional.ofNullable(request).ifPresent(r -> {
                                r.addResponseCloseHandler(v -> {
                                    for (Disposable d : disposables) {
                                        if (null != d) {
                                            d.dispose();
                                        }
                                    }
                                });
                            });

                        })).subscribe();
    }

    /**
     * 发送消息获取响应信息
     * @param msgs rsocket请求信息
     * @param infoList nodeList
     */
    protected abstract ClientTemplate.ResponseInfo<T[]> send(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> infoList);

    /**
     * 处理合并后的list
     */
    protected abstract void handleResult(T t);

    /**
     * 发布合并后的list
     */
    protected abstract void publishResult();

    /**
     * 获取List客户端
     */
    protected abstract AbstractListClient<T> getListClientHandler(StoragePool pool, ClientTemplate.ResponseInfo<T[]> responseInfo, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request);

    /**
     * 将数据将从小到大排列写入list中
     * @param t 列举出的元素
     */
    @Override
    public void write(T t) {
        queue.add(t);
    }

    /**
     * 提供优先队列比较器
     */
    protected abstract Comparator<T> comparator();


    /**
     * @return 返回合并执行结果
     */
    @Override
    public final Mono<Boolean> response() { return res; }
}