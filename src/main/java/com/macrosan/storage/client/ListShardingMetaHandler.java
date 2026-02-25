package com.macrosan.storage.client;

import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.metaserver.move.scanner.DefaultMetaDataScanner;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.JsonObject;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.ERROR;
@Log4j2
public class ListShardingMetaHandler extends AbstractListClient<Tuple2<String, JsonObject>> {
    /**
     * 用于保存客户端list合并比较之后的结果
     */
    public UnicastProcessor<Tuple2<String, JsonObject>> listFlux = UnicastProcessor.create();

    /**
     * list扫描的方向
     */
    DefaultMetaDataScanner.SCAN_SEQUENCE sequence;

    @Getter
    private String nextMarker = null;

    public ListShardingMetaHandler(StoragePool storagePool, ClientTemplate.ResponseInfo<Tuple2<String, JsonObject>[]> responseInfo,
                            List<Tuple3<String, String, String>> nodeList, DefaultMetaDataScanner.SCAN_SEQUENCE sequence) {
        super(storagePool, responseInfo, nodeList);
        this.sequence = sequence;
    }

    @Override
    protected void publishResult() {
        listFlux.onComplete();
        res.onNext(true);
    }

    @Override
    protected String getKey(Tuple2<String, JsonObject> tuple2) {
        return tuple2.var1;
    }

    @Override
    protected int compareTo(Tuple2<String, JsonObject> t1, Tuple2<String, JsonObject> t2) {
        if (t2.var1.equals(t1.var1)) {
            if (t1.var2.containsKey("versionNum")) {
                return t1.var2.getString("versionNum").compareTo(t2.var2.getString("versionNum"));
            } else {
                return 0;
            }
        } else {
            return t1.var1.compareTo(t2.var1);
        }
    }

    @Override
    protected void handleResult(Tuple2<String, JsonObject> t) {
        if (StringUtils.isNotEmpty(nextMarker)) {
            if (sequence.equals(DefaultMetaDataScanner.SCAN_SEQUENCE.NEXT)) {
                if (t.var1.compareTo(nextMarker) <= 0) {
                    listFlux.onNext(t);
                }
            } else {
                if (t.var1.compareTo(nextMarker) >= 0) {
                    listFlux.onNext(t);
                }
            }
        }
    }

    @Override
    public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, Tuple2<String, JsonObject>[]> tuple) {
        if (null != tuple.var3 && tuple.var3.length > 0) {
            String marker = tuple.var3[tuple.var3.length - 1].var1;
            if (StringUtils.isEmpty(nextMarker)) {
                nextMarker = marker;
            } else {
                if (sequence.equals(DefaultMetaDataScanner.SCAN_SEQUENCE.NEXT)) {
                    nextMarker = marker.compareTo(nextMarker) < 0 ? marker : nextMarker;
                } else {
                    nextMarker = marker.compareTo(nextMarker) > 0 ? marker : nextMarker;
                }
            }
        }

        if (sequence.equals(DefaultMetaDataScanner.SCAN_SEQUENCE.NEXT)) {
            super.handleResponse(tuple);
        } else {
            //如果这个节点响应的是error，直接返回
            if (ERROR.equals(tuple.var2)) {
                return;
            }
            //如果链表为空，也就是第一个节点响应结果，直接将第一个节点的响应结果放入链表中

            if (linkedList.isEmpty()) {
                for (Tuple2<String, JsonObject> t : tuple.var3) {
                    linkedList.add(new Counter(1, getKey(t), t));
                }
            } else {
                ListIterator<Counter> iterator = linkedList.listIterator();
                Counter curCounter;
                //如果不是第一个响应的节点，遍历这个节点的响应结果
                for (Tuple2<String, JsonObject> t : tuple.var3) {
                    //如果链表已经遍历结束，但当前节点返回的结果还没遍历结束，就将这条记录加到链表中
                    //例如：第一个节点返回3条记录，第二个节点返回4条记录的情况
                    if (!iterator.hasNext()) {
                        iterator.add(new Counter(1, getKey(t), t));
                        continue;
                    }
                    //新响应的节点返回的记录的key
                    String key = getKey(t);
                    curCounter = iterator.next();
                    int i = curCounter.key.compareTo(key);
                    //比较当前这条记录的key与之前节点返回的记录，如果相等
                    //对应位置的counter的c加一，然后比较对应的记录对象，如果不相等，就设置counter的identical属性为false
                    //如果新的记录比之前的记录新，就替换掉旧记录
                    if (i == 0) {
                        curCounter.c++;
                        int j = compareTo(t, curCounter.t);

                        if (j != 0) {
                            curCounter.identical = false;

                            if (j > 0) {
                                curCounter.t = t;
                            }
                        }
                    } else if (i < 0) {
                        //如果新来的key比链表中当前的key大，将新来的记录加到当前key前面
                        iterator.previous();
                        curCounter = new Counter(1, key, t);
                        iterator.add(curCounter);
                    } else {
                        //如果新来的key比链表中当前的key小，遍历链表：
                        //如果有key和当前一致的，对应的counter.c++
                        //如果找不到与当前对应的，遇到比当前大的就在他前面插入这条记录
                        //如果到最后一直是比当前小的，就在最后插入这条记录
                        int j = 1;
                        while (iterator.hasNext()) {
                            curCounter = iterator.next();
                            j = curCounter.key.compareTo(getKey(t));
                            if (j == 0) {
                                curCounter.c++;
                                break;
                            }

                            if (j < 0) {
                                iterator.previous();
                                curCounter = new Counter(1, key, t);
                                iterator.add(curCounter);
                                break;
                            }
                        }

                        if (j > 0) {
                            curCounter = new Counter(1, key, t);
                            iterator.add(curCounter);
                        }
                    }
                }
            }
        }
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
}
