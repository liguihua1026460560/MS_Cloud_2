package com.macrosan.inventory.datasource.scanner;


import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.inventory.InventoryService;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.versions.ListVersionsResult;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.AbstractListClient;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.snapshot.utils.SnapshotUtil;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.ec.Utils.ZERO_STR;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_INVENTORY_VERSIONS;

@Log4j2
public class InventoryListAllScanner implements Scanner<Tuple2<byte[], byte[]>> {
    private final String bucket;
    private final String prefix;
    public final Queue<Tuple2<byte[], byte[]>> queue = new ConcurrentLinkedQueue<>();
    private UnicastProcessor<String> bucketVnodeProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
    private final Map<String, UnicastProcessor<String>> processorMap = new ConcurrentHashMap<>();
    private final AtomicInteger currentIndex = new AtomicInteger(0);
    private final AtomicInteger maxIndex = new AtomicInteger(0);
    private final List<String> bucketVnodeList;
    private final AtomicReference<String> seekMarker = new AtomicReference<>();
    private final StoragePool storagePool;
    private final AtomicBoolean interrupt = new AtomicBoolean(false);
    private volatile String currentSnapshotMark;
    public InventoryListAllScanner(String bucket, String prefix) {
        this.bucket = bucket;
        this.prefix = prefix;
        this.storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        bucketVnodeList = storagePool.getBucketVnodeList(bucket);
        String startVnode = storagePool.getBucketVnodeId(bucket, prefix);
        String endVnode;
        if (StringUtils.isBlank(prefix)) {
            endVnode = bucketVnodeList.get(bucketVnodeList.size() - 1);
        } else {
            byte[] bytes = prefix.getBytes();
            bytes[bytes.length - 1] += 1;
            endVnode = storagePool.getBucketVnodeId(bucket, new String(bytes));
        }
        int startIndex = bucketVnodeList.indexOf(startVnode);
        int endIndex = bucketVnodeList.indexOf(endVnode);
        currentIndex.set(startIndex);
        maxIndex.set(endIndex);
        for (int i = startIndex; i <= endIndex; i++) {
            processorMap.put(bucketVnodeList.get(i), UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get()));
        }
        seekMarker.set(prefix);
    }

    @Override
    public void start(String startKey) {
        log.info("startKey:" + startKey);
        String startVnode = storagePool.getBucketVnodeId(bucket, prefix);
        int startIndex = bucketVnodeList.indexOf(startVnode);
        currentIndex.set(startIndex);
        queue.clear();
        bucketVnodeProcessor.clear();
        for (UnicastProcessor<String> processor : processorMap.values()) {
            processor.clear();
        }
        processorMap.clear();

        bucketVnodeProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        for (int i = currentIndex.get(); i <= maxIndex.get(); i++) {
            processorMap.put(bucketVnodeList.get(i), UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get()));
        }

        if (startKey != null) {
            String marker = startKey.substring(0, startKey.lastIndexOf(ZERO_STR));
            log.info("marker:" + marker);
            startVnode = storagePool.getBucketVnodeId(bucket, marker);
            startIndex = bucketVnodeList.indexOf(startVnode);
            if (startIndex > currentIndex.get() && startIndex <= maxIndex.get()) {
                currentIndex.set(startIndex);
            }
            seekMarker.set(startKey);
        }
        log.info("bucketVnodeList size: {}, startIndex:{}, endIndex:{}", bucketVnodeList.size(), currentIndex.get(), maxIndex.get());
        bucketVnodeProcessor.publishOn(InventoryService.INVENTORY_SCHEDULER)
                .subscribe(vnode -> {
                    UnicastProcessor<String> listController = processorMap.get(vnode);
                    if (listController == null) {
                        bucketVnodeProcessor.onComplete();
                        return;
                    }
                    listController.subscribe(marker -> {

                        ListVersionsResult listVersionsRes = new ListVersionsResult()
                                .setKeyMarker(marker)
                                .setName(bucket)
                                .setMaxKeys(1000)
                                .setPrefix(prefix);

                        SocketReqMsg msg = new SocketReqMsg("", 0)
                                .put("bucket", bucket)
                                .put("maxKeys", String.valueOf(1000))
                                .put("prefix", prefix)
                                .put("marker", marker);

                        storagePool.mapToNodeInfo(vnode)
                                .flatMap(infoList -> SnapshotUtil.fetchBucketSnapshotInfo(bucket, msg).thenReturn(infoList))
                                .subscribe(infoList -> {
                                    String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                                    msg.put("vnode", nodeArr[0]);
                                    List<SocketReqMsg> msgs = infoList.stream().map(info -> msg.copy().put("lun", info.var2)).collect(Collectors.toList());
                                    ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_INVENTORY_VERSIONS, new TypeReference<Tuple3<Boolean, String, MetaData>[]>() {
                                    }, infoList);
                                    this.currentSnapshotMark=msg.get("currentSnapshotMark");
                                    InventoryListVersionsHandler listObjectClientHandler = new InventoryListVersionsHandler(listVersionsRes, responseInfo, infoList, bucket);
                                    responseInfo.responses.publishOn(ErasureServer.DISK_SCHEDULER).subscribe(listObjectClientHandler::handleResponse, e -> log.error("", e), listObjectClientHandler::handleComplete);
                                    listObjectClientHandler.res.publishOn(ErasureServer.DISK_SCHEDULER).subscribe(b -> {
                                        synchronized (this) {
                                            if (b) {
                                                for (Tuple2<byte[], byte[]> tuple2 : listObjectClientHandler.linkedList) {
                                                    queue.offer(tuple2);
                                                }
                                                String nextMarker = listVersionsRes.getNextKeyMarker();
                                                if (StringUtils.isNotBlank(nextMarker)) {
                                                    seekMarker.set(nextMarker);
                                                    this.notifyAll();
                                                } else {
                                                    listController.onComplete();
                                                    this.notifyAll();
                                                }
                                            } else {
                                                interrupt.set(true);
                                                this.notifyAll();
                                                listController.onComplete();
                                                bucketVnodeProcessor.onComplete();
                                            }
                                        }
                                    });
                                }, e -> {
                                    synchronized (this) {
                                        interrupt.set(true);
                                        this.notifyAll();
                                        listController.onComplete();
                                        bucketVnodeProcessor.onComplete();
                                        log.error("", e);
                                    }
                                });

                    }, e -> {
                        synchronized (this) {
                            interrupt.set(true);
                            this.notifyAll();
                            listController.onComplete();
                            bucketVnodeProcessor.onComplete();
                            log.error("", e);
                        }
                    }, () -> {
                        synchronized (this) {
                            if (!interrupt.get()) {
                                int next = currentIndex.incrementAndGet();
                                if (next <= maxIndex.get()) {
                                    bucketVnodeProcessor.onNext(bucketVnodeList.get(next));
                                }
                            }
                        }
                    });
                });

        bucketVnodeProcessor.onNext(bucketVnodeList.get(currentIndex.get()));
    }

    @Override
    public void seek(byte[] point) { }

    @Override
    public synchronized Tuple2<byte[], byte[]> next() throws Exception {
        if (!queue.isEmpty()) {
            return queue.poll();
        } else {
            try {
                if (currentIndex.get() > maxIndex.get() || currentIndex.get() >= bucketVnodeList.size()) {
                    return null;
                }
                processorMap.get(bucketVnodeList.get(currentIndex.get())).onNext(seekMarker.get());
                long start = System.nanoTime();
                this.wait(60000);
                long end = System.nanoTime();
                double cost = (end - start) / 1000000000.0;
                if (cost >= 60) {
                    throw new TimeoutException("inventory list all timeout!");
                }
                if (interrupt.get()) {
                    throw new UnsupportedOperationException("Inventory scanner list error!");
                }
                return next();
            } catch (Exception e) {
                log.error("", e);
                throw e;
            }
        }
    }

    @Override
    public void release() {
    }

    @Override
    public String getCurrentSnapshotMark() {
        return currentSnapshotMark;
    }

    public static String getKeyStampVersionId(MetaData metaData) {
        return metaData.key + ZERO_STR + metaData.stamp + File.separator + metaData.versionId;
    }

    public static class InventoryListVersionsHandler extends AbstractListClient<Tuple3<Boolean, String, MetaData>> {
        public ListVersionsResult listVersionsResult;
        public LinkedList<Tuple2<byte[], byte[]>> linkedList = new LinkedList<>();
        private int resCount = 0;

        public InventoryListVersionsHandler(ListVersionsResult listVersionsResult,
                                             ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo,
                                             List<Tuple3<String, String, String>> nodeList,
                                             String bucket) {
            super(StoragePoolFactory.getMetaStoragePool(bucket), responseInfo, nodeList);
            if (listVersionsResult != null) {
                this.listVersionsResult = listVersionsResult;
            }
        }

        @Override
        protected void handleResult(Tuple3<Boolean, String, MetaData> t) {
            MetaData metaData = t.var3;
            if (!metaData.deleteMark) {
                if (resCount < listVersionsResult.getMaxKeys()) {
                    linkedList.add(new Tuple2<>(getKeyStampVersionId(metaData).getBytes(), Json.encode(t.var3).getBytes()));
                    listVersionsResult.setNextKeyMarker(getKeyStampVersionId(metaData));
                    resCount++;
                } else {
                    listVersionsResult.setTruncated(true);
                }
            }
        }

        @Override
        protected void publishResult() {
            if (!listVersionsResult.isTruncated()) {
                listVersionsResult.setNextKeyMarker("");
            }
            res.onNext(true);
        }


        @Override
        protected String getKey(Tuple3<Boolean, String, MetaData> t) {
            return Utils.getMetaDataKey(vnode, t.var3.getBucket(), t.var3.getKey(), t.var3.versionId, t.var3.stamp, t.var3.snapshotMark);
        }

        @Override
        protected int compareTo(Tuple3<Boolean, String, MetaData> t1, Tuple3<Boolean, String, MetaData> t2) {
            if (t2.var3.versionNum.equals(t1.var3.versionNum)) {
                // versionNum一致按key排序
                return t2.var3.key.compareTo(t1.var3.key);
            } else {
                //versionNum不一致最新versionNum为较小值
                return t1.var3.versionNum.compareTo(t2.var3.versionNum);
            }
        }


        @Override
        protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList) { return Mono.just(true); }
        @Override
        protected void putErrorList(Counter counter) {}
        @Override
        protected void publishErrorList() {}
    }
}
