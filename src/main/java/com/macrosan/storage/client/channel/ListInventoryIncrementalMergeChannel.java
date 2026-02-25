package com.macrosan.storage.client.channel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.AbstractListClient;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.InventoryIncrementalListVersionsHandler;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_INVENTORY_INCREMENT_VERSIONS;

public class ListInventoryIncrementalMergeChannel extends AbstractIndexShardMergeChannel<Tuple3<Boolean, String, MetaData>>{

    private final int maxKeys;

    private int resCount = 0;

    public LinkedList<Tuple2<byte[], byte[]>> linkedList = new LinkedList<>();

    @Getter
    @Setter
    private String nextMarker;

    private boolean isTruncated = false;

    public ListInventoryIncrementalMergeChannel(String bucket, StoragePool storagePool, List<String> shardNodes, MsHttpRequest request, int maxKeys, String prefix, String snapshotLink) {
        super(bucket, storagePool, shardNodes, request, maxKeys, prefix);
        this.snapshotLink = snapshotLink;
        this.maxKeys = maxKeys;
        setRange(false);
    }

    @Override
    protected void handleResult(Tuple3<Boolean, String, MetaData> tuple3) {
        if (resCount < maxKeys) {
            linkedList.add(new Tuple2<>(getKey(tuple3).getBytes(), Json.encode(tuple3.var3).getBytes()));
            nextMarker = getKey(tuple3);
            resCount++;
        } else {
            isTruncated = true;
        }
    }

    @Override
    protected void publishResult() {
        if (!isTruncated) {
            nextMarker = null;
        }
        res.onNext(true);
    }

    @Override
    protected AbstractListClient<Tuple3<Boolean, String, MetaData>> getListClientHandler(StoragePool pool, ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        InventoryIncrementalListVersionsHandler inventoryIncrementalListVersionsHandler = new InventoryIncrementalListVersionsHandler(null, responseInfo, nodeList, bucket, snapshotLink);
        inventoryIncrementalListVersionsHandler.setChannel(this);
        return inventoryIncrementalListVersionsHandler;
    }

    protected String getKey(Tuple3<Boolean, String, MetaData> t) {
        return getKey(t.var3);
    }

    protected String getKey(MetaData meta) {
        return bucket + File.separator + meta.stamp + File.separator + meta.key + File.separator + meta.versionId;
    }

    @Override
    protected Comparator<Tuple3<Boolean, String, MetaData>> comparator() {
        return Comparator.comparing(this::getKey);
    }

    @Override
    protected ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> send(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> infoList) {
        return ClientTemplate.oneResponse(msgs, LIST_INVENTORY_INCREMENT_VERSIONS, new TypeReference<Tuple3<Boolean, String, MetaData>[]>() {
        }, infoList);
    }
}
