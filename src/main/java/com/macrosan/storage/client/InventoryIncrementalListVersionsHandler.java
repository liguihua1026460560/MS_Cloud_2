package com.macrosan.storage.client;

import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.xmlmsg.versions.ListVersionsResult;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

public class InventoryIncrementalListVersionsHandler extends ListVersionsClientHandler {
    private final ListVersionsResult listVersionsResult;
    protected LinkedList<Counter> result = new LinkedList<>();
    public LinkedList<Tuple2<byte[], byte[]>> linkedList = new LinkedList<>();
    private int resCount = 0;
    protected static class Counter {

        String key;
        @Getter
        MetaData metaData;

        Counter(String key, MetaData metaData) {
            this.metaData = metaData;
            this.key = key;
        }

    }
    public InventoryIncrementalListVersionsHandler(ListVersionsResult listVersionsResult,
                                                   ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo,
                                                   List<Tuple3<String, String, String>> nodeList,
                                                   String bucket, String snapshotLink) {
        super(StoragePoolFactory.getMetaStoragePool(bucket), listVersionsResult, responseInfo, nodeList, snapshotLink);
        this.listVersionsResult = listVersionsResult;
    }

    @Override
    protected String getKey(Tuple3<Boolean, String, MetaData> t) {
        return getKey(t.var3);
    }

    protected String getKey(MetaData meta) {
        return Utils.getLifeCycleMetaKey(vnode, meta.getBucket(), meta.getKey(), meta.getVersionId(), meta.getStamp());
    }

    @Override
    protected void handleResult(Tuple3<Boolean, String, MetaData> t) {

        MetaData metaData = t.var3;
        if (!metaData.deleteMark) {
            if (resCount < listVersionsResult.getMaxKeys()) {
                linkedList.add(new Tuple2<>(getKey(metaData).getBytes(), Json.encode(t.var3).getBytes()));
                listVersionsResult.setNextKeyMarker(getKey(metaData));
                resCount++;
            } else {
                listVersionsResult.setTruncated(true);
            }
        }else {
            if (resCount < listVersionsResult.getMaxKeys()) {
                linkedList.add(new Tuple2<>(getKey(metaData).getBytes(), Json.encode(t.var3).getBytes()));
                listVersionsResult.setNextKeyMarker(getKey(metaData));
                resCount++;
            } else {
                listVersionsResult.setTruncated(true);
            }
        }
    }


    @Override
    protected void publishErrorList() {}

    @Override
    public void handleComplete() {
        repairFlux.onNext(0);
    }

    @Override
    public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, Tuple3<Boolean, String, MetaData>[]> tuple) {
        super.handleResponse(tuple);
    }
}
