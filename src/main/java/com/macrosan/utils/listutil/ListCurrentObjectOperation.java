package com.macrosan.utils.listutil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.snapshot.utils.SnapshotUtil;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.NoRepairListObjectsHandler;
import com.macrosan.utils.listutil.interpcy.InterruptPolicy;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_OBJECTS;

@Log4j2
public class ListCurrentObjectOperation extends ConsecutiveListOperations<MetaData> {
    private final String bucketName;
    private final String prefix;
    private final StoragePool storagePool;

    ListCurrentObjectOperation(String bucketName, String prefix, InterruptPolicy<MetaData> interruptPolicy, ListOptions listOptions) {
        super();
        this.bucketName = bucketName;
        this.prefix = prefix;
        this.storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        if (interruptPolicy != null) {
            this.interruptPolicy = interruptPolicy;
        }
        this.listOptions = listOptions;
    }

    @Override
    protected Flux<MetaData> listOnce(String marker) {
        UnicastProcessor<MetaData> result = UnicastProcessor.create();
        String startKey = StringUtils.isEmpty(marker) ? prefix : marker;
        String bucketVnodeId = storagePool.getBucketVnodeId(bucketName, startKey);
        List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucketName);
        AtomicInteger count = new AtomicInteger();
        AtomicInteger startIndex = new AtomicInteger(bucketVnodeList.indexOf(bucketVnodeId));
        int endIndex = bucketVnodeList.size() - 1;
        final int shardAmount = endIndex - startIndex.get() + 1;
        AtomicInteger completeShardCount = new AtomicInteger(0);
        UnicastProcessor<String> shardProcessor = UnicastProcessor.create();
        shardProcessor.subscribe(shardId -> {
            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("bucket", bucketName)
                    .put("vnode", shardId)
                    .put("maxKeys", String.valueOf(listOptions.getMaxListCount()))
                    .put("prefix", prefix)
                    .put("marker", marker);
            storagePool.mapToNodeInfo(shardId)
                    .flatMap(infoList -> SnapshotUtil.fetchBucketSnapshotInfo(bucketName, msg).thenReturn(infoList))
                    .subscribe(infoList -> {
                        List<SocketReqMsg> msgList = infoList.stream().map(info -> msg.copy().put("lun", info.var2)).collect(Collectors.toList());
                        ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgList, LIST_OBJECTS, new TypeReference<Tuple3<Boolean, String, MetaData>[]>() {
                        }, infoList);
                        NoRepairListObjectsHandler listObjectsHandler = new NoRepairListObjectsHandler(storagePool, responseInfo, infoList);
                        responseInfo.responses.publishOn(ErasureServer.DISK_SCHEDULER).subscribe(listObjectsHandler::handleResponse, e -> log.error("", e), listObjectsHandler::handleComplete);
                        listObjectsHandler.res.publishOn(ErasureServer.DISK_SCHEDULER).subscribe(b -> {
                            if (!b) {
                                result.onError(new Exception("list objects error"));
                            } else {
                                for (Tuple3<Boolean, String, MetaData> tuple3 : listObjectsHandler.list) {
                                    MetaData metaData = tuple3.var3;
                                    if (!tuple3.var1) {
                                        result.onNext(metaData);
                                    }
                                    count.incrementAndGet();
                                }
                                if (count.get() >= listOptions.getMaxListCount() + 1 || completeShardCount.incrementAndGet() == shardAmount) {
                                    result.onComplete();
                                    shardProcessor.onComplete();
                                } else {
                                    int next = startIndex.incrementAndGet();
                                    if (next <= endIndex) {
                                        shardProcessor.onNext(bucketVnodeList.get(next));
                                    }
                                }
                            }
                        });
                    });
        });
        shardProcessor.onNext(bucketVnodeList.get(startIndex.get()));
        return result;
    }

    @Override
    protected String getNextMarker(MetaData item) {
        return item.key;
    }

    @Override
    protected long getSize(MetaData item) {
        int res = 1024; //default 1KB
        if (item.partUploadId == null) {
            return res;
        }
        return (long) (item.partInfos.length + 1) * res;
    }
}
