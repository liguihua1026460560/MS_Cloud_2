package com.macrosan.storage.client;

import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.utils.functional.Tuple3;
import reactor.core.publisher.Mono;

import java.util.LinkedList;
import java.util.List;

public class ListCurrentObjectHandler extends AbstractListClient<Tuple3<Boolean, String, MetaData>>{

    private final LinkedList<Tuple3<Boolean, String, MetaData>> result = new LinkedList<>();

    protected ListCurrentObjectHandler(StoragePool pool, ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, List<Tuple3<String, String, String>> nodeList) {
        super(pool, responseInfo, nodeList);
    }

    @Override
    protected void publishResult() {

    }

    @Override
    protected String getKey(Tuple3<Boolean, String, MetaData> booleanStringMetaDataTuple3) {
        return null;
    }

    @Override
    protected int compareTo(Tuple3<Boolean, String, MetaData> t1, Tuple3<Boolean, String, MetaData> t2) {
        return 0;
    }

    @Override
    protected void handleResult(Tuple3<Boolean, String, MetaData> booleanStringMetaDataTuple3) {

    }

    @Override
    protected Mono<Boolean> repair(AbstractListClient<Tuple3<Boolean, String, MetaData>>.Counter counter, List<Tuple3<String, String, String>> nodeList) {
        return Mono.just(true);
    }

    @Override
    protected void putErrorList(AbstractListClient<Tuple3<Boolean, String, MetaData>>.Counter counter) {

    }

    @Override
    protected void publishErrorList() {

    }
}
