package com.macrosan.storage.client;

import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import lombok.Getter;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;

public class ListHistoryRecClientHandler extends AbstractListClient<Tuple2<String, UnSynchronizedRecord>> {
    public MonoProcessor<List<UnSynchronizedRecord>> recordProcessor = MonoProcessor.create();

    public static final int MAX_COUNT = 1000;

    UnicastProcessor<String> listController;

    String marker;

    /**
     * 返回的记录条数，并非最终结果的条数（已在处理的记录会从list结果剔除）
     */
    @Getter
    private int count = 0;

    @Getter
    String nextMarker = "";

    public ListHistoryRecClientHandler(ClientTemplate.ResponseInfo<Tuple2<String, UnSynchronizedRecord>[]> responseInfo,
                                       List<Tuple3<String, String, String>> nodeList, UnicastProcessor<String> listController, String marker, String bucket) {
        super(responseInfo, nodeList, bucket);
        this.listController = listController;
        this.marker = marker;
    }

    @Override
    protected void publishResult() {
        res.onNext(true);
    }

    @Override
    protected String getKey(Tuple2<String, UnSynchronizedRecord> tuple2) {
        return tuple2.var1;
    }

    @Override
    protected int compareTo(Tuple2<String, UnSynchronizedRecord> t1, Tuple2<String, UnSynchronizedRecord> t2) {
        if (t2.var1.equals(t1.var1)) {
            return t1.var2.getVersionNum().compareTo(t2.var2.getVersionNum());
        } else {
            return t1.var1.compareTo(t2.var1);
        }
    }

    @Override
    protected void handleResult(Tuple2<String, UnSynchronizedRecord> tuple2) {

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
        if (responseInfo.successNum < pool.getK()) {
            Mono.delay(Duration.ofSeconds(10)).publishOn(SCAN_SCHEDULER).subscribe(s -> listController.onNext(marker));
            return;
        }
        count = linkedList.size();
        LinkedList<UnSynchronizedRecord> recordList = new LinkedList<>();
        for (Counter counter : linkedList) {
            UnSynchronizedRecord record = counter.t.var2;
            recordList.add(record);
        }

        if (recordList.peekLast() != null) {
            nextMarker = recordList.peekLast().rocksKey();
        }
        recordProcessor.onNext(recordList);
        publishResult();

    }
}
