package com.macrosan.storage.client;

import com.macrosan.storage.StoragePool;
import com.macrosan.storage.aggregation.transaction.UndoLog;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import lombok.Getter;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.LinkedList;
import java.util.List;


public class ListAggregationUndoLogHandler extends AbstractListClient<Tuple2<String, UndoLog>> {
    public static final int MAX_KEY = 1000;

    public ListAggregationUndoLogHandler(StoragePool pool, ClientTemplate.ResponseInfo<Tuple2<String, UndoLog>[]> responseInfo, List<Tuple3<String, String, String>> nodeList) {
        super(pool, responseInfo, nodeList);
    }

    @Getter
    public String nextMarker;

    @Getter
    public int count;

    public MonoProcessor<List<UndoLog>> listProcessor = MonoProcessor.create();

    @Override
    protected void publishResult() {
        res.onNext(true);
    }

    @Override
    protected String getKey(Tuple2<String, UndoLog> stringUndoLogTuple2) {
        return stringUndoLogTuple2.var1;
    }

    @Override
    protected int compareTo(Tuple2<String, UndoLog> t1, Tuple2<String, UndoLog> t2) {
        if (t2.var1.equals(t1.var1)) {
            return t1.var2.getVersionNum().compareTo(t2.var2.getVersionNum());
        }
        return t1.var1.compareTo(t2.var1);
    }

    @Override
    protected void handleResult(Tuple2<String, UndoLog> stringUndoLogTuple2) {
    }

    @Override
    protected Mono<Boolean> repair(AbstractListClient<Tuple2<String, UndoLog>>.Counter counter, List<Tuple3<String, String, String>> nodeList) {
        return Mono.just(true);
    }

    @Override
    protected void putErrorList(AbstractListClient<Tuple2<String, UndoLog>>.Counter counter) {

    }

    @Override
    protected void publishErrorList() {

    }

    @Override
    public void handleComplete() {
        LinkedList<UndoLog> undoLogs = new LinkedList<>();
        count = linkedList.size();
        for (Counter counter : linkedList) {
            UndoLog undoLog = counter.t.var2;
            undoLogs.add(undoLog);
        }
        if (!undoLogs.isEmpty()) {
            this.nextMarker = undoLogs.getLast().rocksKey();
        }
        listProcessor.onNext(undoLogs);
        publishResult();
    }
}
