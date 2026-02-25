package com.macrosan.storage.client;

import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.xmlmsg.versions.ListVersionsResult;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.AbstractListClient;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListVersionsClientHandler;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import reactor.core.publisher.Mono;

import java.util.LinkedList;
import java.util.List;

public class NoRepairListVersionsHandler extends ListVersionsClientHandler {

    public final LinkedList<Tuple3<Boolean, String, MetaData>> list = new LinkedList<>();

    public NoRepairListVersionsHandler(StoragePool storagePool, ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, List<Tuple3<String, String, String>> nodeList, String snapshotLink) {
        super(storagePool, null, responseInfo, nodeList, snapshotLink);
    }

    @Override
    protected void publishResult() {
        res.onNext(true);
    }

    @Override
    protected void handleResult(Tuple3<Boolean, String, MetaData> t) {
        MetaData metaData = t.var3;
        if (!metaData.deleteMark) {
            list.add(t);
        }
    }

    @Override
    protected Mono<Boolean> repair(AbstractListClient<Tuple3<Boolean, String, MetaData>>.Counter counter, List<Tuple3<String, String, String>> nodeList) {
        // nothing to do
        return Mono.just(true);
    }

    @Override
    protected void putErrorList(AbstractListClient<Tuple3<Boolean, String, MetaData>>.Counter counter) {
        // nothing to do
    }

    @Override
    protected void publishErrorList() {
        // nothing to do
    }
}
