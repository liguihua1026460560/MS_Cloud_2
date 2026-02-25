package com.macrosan.utils.listutil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.snapshot.utils.SnapshotUtil;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.NoRepairListVersionsHandler;
import com.macrosan.utils.listutil.interpcy.InterruptPolicy;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_VERSIONS;

@Log4j2
public class ListVersionsObjectOperation extends ConsecutiveListOperations<MetaData> {
    private final String bucketName;
    private final String prefix;
    private final StoragePool storagePool;

    ListVersionsObjectOperation(String bucketName, String prefix, InterruptPolicy<MetaData> interruptPolicy, ListOptions options) {
        super();
        this.bucketName = bucketName;
        this.prefix = prefix;
        this.storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        if (interruptPolicy != null) {
            this.interruptPolicy = interruptPolicy;
        }
        this.listOptions = options;
    }

    @Override
    protected Flux<MetaData> listOnce(String marker) {
        UnicastProcessor<MetaData> result = UnicastProcessor.create(Queues.<MetaData>unboundedMultiproducer().get());
        String startKey = prefix;
        String versionIdMarker;
        if (StringUtils.isNotEmpty(marker)) {
            int index = marker.lastIndexOf("/");
            startKey = index == -1 ? marker : marker.substring(0, index);
            versionIdMarker = index == -1 ? "" : marker.substring(index + 1);
            marker = startKey;
        } else {
            versionIdMarker = "";
        }
        String bucketVnodeId = storagePool.getBucketVnodeId(bucketName, startKey);
        List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucketName);
        AtomicInteger count = new AtomicInteger();
        AtomicInteger startIndex = new AtomicInteger(bucketVnodeList.indexOf(bucketVnodeId));
        int endIndex = bucketVnodeList.size() - 1;
        final int shardAmount = endIndex - startIndex.get() + 1;
        AtomicInteger completeShardCount = new AtomicInteger(0);
        UnicastProcessor<String> shardProcessor = UnicastProcessor.create();
        String finalMarker = marker;
        shardProcessor.subscribe(shardId -> {
            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("bucket", bucketName)
                    .put("vnode", shardId)
                    .put("maxKeys", String.valueOf(listOptions.getMaxListCount()))
                    .put("prefix", prefix)
                    .put("marker", finalMarker)
                    .put("versionIdMarker", versionIdMarker);

            storagePool.mapToNodeInfo(shardId)
                    .flatMap(infoList -> SnapshotUtil.fetchBucketSnapshotInfo(bucketName, msg).thenReturn(infoList))
                    .subscribe(infoList -> {
                        List<SocketReqMsg> msgList = infoList.stream().map(info -> msg.copy().put("lun", info.var2)).collect(Collectors.toList());
                        ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgList, LIST_VERSIONS, new TypeReference<Tuple3<Boolean, String, MetaData>[]>() {
                        }, infoList);
                        NoRepairListVersionsHandler handler = new NoRepairListVersionsHandler(storagePool, responseInfo, infoList, msg.get("snapshotLink"));
                        responseInfo.responses.publishOn(ErasureServer.DISK_SCHEDULER).subscribe(handler::handleResponse, e -> log.error("", e), handler::handleComplete);
                        handler.res.publishOn(ErasureServer.DISK_SCHEDULER).subscribe(b -> {
                            if (!b) {
                                result.onError(new Exception("list versions error"));
                            } else {
                                for (Tuple3<Boolean, String, MetaData> tuple3 : handler.list) {
                                    result.onNext(tuple3.var3);
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
        return item.key + "/" + item.versionId;
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
