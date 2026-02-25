package com.macrosan.storage.client;

import com.macrosan.component.ComponentUtils;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import lombok.Getter;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.LinkedList;
import java.util.List;

public class ListMultiMediaTaskHandler extends AbstractListClient<Tuple2<String, ComponentRecord>> {

    public static final int MAX_KEY = 500;

    //批量删除record时，每次查询record的最大值
    public static final int DELETE_MAX_KEY = 1000;

    @Getter
    String nextMarker = "";

    @Getter
    private int count = 0;

    public MonoProcessor<List<ComponentRecord>> listProcessor = MonoProcessor.create();

    public ListMultiMediaTaskHandler(ClientTemplate.ResponseInfo<Tuple2<String, ComponentRecord>[]> responseInfo,
                                     List<Tuple3<String, String, String>> nodeList, String bucket) {
        super(responseInfo, nodeList, bucket);
    }

    @Override
    protected void publishResult() {
        res.onNext(true);
    }

    @Override
    protected String getKey(Tuple2<String, ComponentRecord> tuple2) {
        return tuple2.var1;
    }

    @Override
    protected int compareTo(Tuple2<String, ComponentRecord> t1, Tuple2<String, ComponentRecord> t2) {
        if (t2.var1.equals(t1.var1)) {
            return t1.var2.getVersionNum().compareTo(t2.var2.getVersionNum());
        } else {
            return t1.var1.compareTo(t2.var1);
        }
    }

    @Override
    protected void handleResult(Tuple2<String, ComponentRecord> tuple2) {

    }

    @Override
    protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList) {
        return Mono.just(true);
    }

    @Override
    protected void putErrorList(Counter counter) {

    }

    @Override
    protected void publishErrorList() {

    }

    @Override
    public void handleComplete() {
        LinkedList<ComponentRecord> recordList = new LinkedList<>();
        for (Counter counter : linkedList) {
            ComponentRecord record = counter.t.var2;
            if (ComponentUtils.startRecordOperation(record)) {
                recordList.add(record);
            }
        }
        if (!recordList.isEmpty()) {
            this.nextMarker = recordList.getLast().rocksKey();
        }
        count = recordList.size();
        listProcessor.onNext(recordList);
        publishResult();
    }

    public void deleteHandleComplete() {
        LinkedList<ComponentRecord> recordList = new LinkedList<>();
        count = linkedList.size();
        for (Counter counter : linkedList) {
            ComponentRecord record = counter.t.var2;
            recordList.add(record);
        }
        if (!recordList.isEmpty()) {
            this.nextMarker = recordList.getLast().rocksKey();
        }
        listProcessor.onNext(recordList);
        publishResult();
    }

}
