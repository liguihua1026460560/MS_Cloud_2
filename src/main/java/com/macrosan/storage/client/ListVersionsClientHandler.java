package com.macrosan.storage.client;

import com.google.common.collect.Lists;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Prefix;
import com.macrosan.message.xmlmsg.versions.DeleteMarker;
import com.macrosan.message.xmlmsg.versions.ListVersionsResult;
import com.macrosan.message.xmlmsg.versions.Version;
import com.macrosan.message.xmlmsg.versions.VersionBase;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.*;

import static com.macrosan.constants.ServerConstants.ETAG;
import static com.macrosan.constants.ServerConstants.LAST_MODIFY;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_REPAIR_LSVERSION;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_REPAIR_LSVERSION_SNAP;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.ERROR;

/**
 * @author chengyinfeng
 */
@Log4j2
public class ListVersionsClientHandler extends AbstractListClient<Tuple3<Boolean, String, MetaData>> {
    private ListVersionsResult listVersionsResult;
    private List<VersionBase> contents;
    private List<Prefix> prefixes;
    private int resCount = 0;
    private String preKey;
    private String nextKey;
    private boolean nextLatest;
    private List<Tuple3<String, String, String>> errorList = new LinkedList<>();
    private List<Tuple2<String, Tuple3<String, String, String>>> snapshotErrorList = new LinkedList<>();

    public ListVersionsClientHandler(StoragePool storagePool, ListVersionsResult listVersionsResult,
                                     ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo,
                                     List<Tuple3<String, String, String>> nodeList, String snapshotLink) {
        super(storagePool, responseInfo, nodeList);
        super.snapshotLink = snapshotLink;
        if (listVersionsResult != null) {
            contents = new ArrayList<>(listVersionsResult.getMaxKeys());
            prefixes = new ArrayList<>(listVersionsResult.getMaxKeys());
            this.listVersionsResult = listVersionsResult;
        }
    }

    @Override
    protected void publishResult() {
        listVersionsResult.setVersion(contents);
        listVersionsResult.setPrefixlist(prefixes);
        if (!listVersionsResult.isTruncated()) {
            listVersionsResult.setNextKeyMarker("");
            listVersionsResult.setNextVersionIdMarker("");
        }
        res.onNext(true);
    }

    @Override
    protected String getKey(Tuple3<Boolean, String, MetaData> t) {
        return Utils.getMetaDataKey(vnode, t.var3.getBucket(), t.var3.getKey(), null);
    }

    @Override
    protected int compareTo(Tuple3<Boolean, String, MetaData> t1, Tuple3<Boolean, String, MetaData> t2) {
        if (t2.var3.versionNum.equals(t1.var3.versionNum)) {
            // versionNum一致按key排序
            return t2.var3.key.compareTo(t1.var3.key);
        } else {
            //versionNum不一致最新versionNum为较小值
            return t1.var3.versionNum.compareTo(t2.var3.versionNum);
        }
    }

    private String getStampKey(Tuple3<Boolean, String, MetaData> t) {
        return Utils.getMetaDataKey(vnode, t.var3.getBucket(), t.var3.getKey(), t.var3.versionId, t.var3.stamp, null);
    }

    /**
     * 处理从节点拿到的响应
     * 该接口首先根据key按字典排序，其次key相等时安装stamp字典反向排序
     *
     * @param tuple 节点的响应，这个三元元组中数据分别为 ：link序号，响应类型，返回的元数据list，要求rsocket返回的list是有序的
     */
    @Override
    public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, Tuple3<Boolean, String, MetaData>[]> tuple) {
        //如果这个节点响应的是error，直接返回
        if (ERROR.equals(tuple.var2)) {
            return;
        }
        //如果链表为空，也就是第一个节点响应结果，直接将第一个节点的响应结果放入链表中
        if (linkedList.isEmpty()) {
            for (Tuple3<Boolean, String, MetaData> t : tuple.var3) {
                linkedList.add(new Counter(1, getKey(t), t));
            }
        } else {
            ListIterator<Counter> iterator = linkedList.listIterator();
            Counter curCounter;
            //如果不是第一个响应的节点，遍历这个节点的响应结果
            for (Tuple3<Boolean, String, MetaData> t : tuple.var3) {
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
                    if (t.var1) {
                        curCounter.c++;
                        continue;
                    }
                    //判断versionId 不一样表示数据不一致  然后需根据stamp确认位置
                    int k = getStampKey(curCounter.t).compareTo(getStampKey(t));
                    if (k == 0) {
                        curCounter.c++;
                        int j = compareTo(t, curCounter.t);
                        if (j != 0) {
                            curCounter.identical = false;
                            if (j > 0) {
                                curCounter.t = t;
                            }
                        }
                    } else if (k < 0) {
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
                            j = getStampKey(curCounter.t).compareTo(getStampKey(t));
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
                            if (curCounter.t.var3.key.compareTo(t.var3.key) > 0) {
                                iterator.previous();
                                break;
                            }
                        }

                        if (j > 0) {
                            curCounter = new Counter(1, key, t);
                            iterator.add(curCounter);
                        }
                    }

                } else if (i > 0) {
                    //如果新来的key比链表中当前的key小，将新来的记录加到当前key前面
                    iterator.previous();
                    curCounter = new Counter(1, key, t);
                    iterator.add(curCounter);
                } else {
                    //如果新来的key比链表中当前的key大，遍历链表：
                    //如果有key和当前一致的，对应的counter.c++
                    //如果找不到与当前对应的，遇到比当前大的就在他前面插入这条记录
                    //如果到最后一直是比当前小的，就在最后插入这条记录
                    int j = -1;
                    while (iterator.hasNext()) {
                        curCounter = iterator.next();
                        j = curCounter.key.compareTo(getKey(t));

                        if (j > 0) {
                            iterator.previous();
                            curCounter = new Counter(1, key, t);
                            iterator.add(curCounter);
                            break;
                        }
                        if (j == 0) {
                            int k = getStampKey(curCounter.t).compareTo(getStampKey(t));
                            if (k == 0) {
                                curCounter.c++;
                                break;
                            } else if (k < 0) {
                                //如果新来的key比链表中当前的key大，将新来的记录加到当前key前面
                                iterator.previous();
                                curCounter = new Counter(1, key, t);
                                iterator.add(curCounter);
                                break;
                            }
                        }
                    }

                    if (j < 0) {
                        curCounter = new Counter(1, key, t);
                        iterator.add(curCounter);
                    }
                }
            }
        }
    }

    @Override
    protected void handleResult(Tuple3<Boolean, String, MetaData> t) {
        MetaData metaData = t.var3;
        if (!metaData.deleteMark) {
            if (resCount < listVersionsResult.getMaxKeys()) {
                if (t.var1) {
                    prefixes.add(new Prefix().setPrefix(t.var2));
                    listVersionsResult.setNextKeyMarker(metaData.key);
                    listVersionsResult.setNextVersionIdMarker(metaData.versionId);
                } else {
                    contents.add(mapToContents(metaData));
                    listVersionsResult.setNextKeyMarker(metaData.getKey());
                    listVersionsResult.setNextVersionIdMarker(metaData.getVersionId());
                }
                resCount++;
            } else {
                listVersionsResult.setTruncated(true);
            }
        }
    }

    private static VersionBase mapToContents(MetaData metaData) {
        Map<String, String> sysMetaMap = Json.decodeValue(metaData.getSysMetaData(), Map.class);
        if (!metaData.deleteMarker) {
            return new Version()
                    .setKey(metaData.getKey())
                    .setVersionId(metaData.getVersionId())
                    .setEtag(Utils.getEtag(sysMetaMap))
                    .setLastModified(MsDateUtils.dateToISO8601(sysMetaMap.get(LAST_MODIFY)))
                    .setOwner(new Owner().setDisplayName(sysMetaMap.get("displayName")).setId(sysMetaMap.get("owner")))
                    .setSize(Utils.getObjectSize(sysMetaMap, metaData))
                    .setStorageClass("STANDARD")
                    .setLatest(metaData.isLatest());
        } else {
            return new DeleteMarker()
                    .setKey(metaData.getKey())
                    .setVersionId(metaData.getVersionId())
                    .setLastModified(MsDateUtils.dateToISO8601(sysMetaMap.get(LAST_MODIFY)))
                    .setOwner(new Owner().setDisplayName(sysMetaMap.get("displayName")).setId(sysMetaMap.get("owner")))
                    .setStorageClass("STANDARD")
                    .setLatest(metaData.isLatest());
        }
    }

    @Override
    protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList) {
        if (counter.t.var1) {
            return Mono.just(true);
        }
        MetaData metaData = counter.t.var3;
        return ErasureClient.getObjectMetaVersion(metaData.bucket, metaData.key, metaData.versionId, nodeList, request, metaData.snapshotMark, snapshotLink)
                .map(meta -> {
                    boolean res = !meta.equals(MetaData.ERROR_META);
                    if (res) {
                        if (!meta.equals(MetaData.NOT_FOUND_META)) {
                            if (StringUtils.isEmpty(preKey) || !preKey.equals(meta.key)) {
                                meta.setLatest(metaData.latest);
                            }
                            if (metaData.latest) {
                                if (!meta.deleteMark) {
                                    preKey = meta.key;
                                } else {
                                    nextLatest = true;
                                    nextKey = meta.key;
                                }
                            }
                            counter.t.var3 = meta;
                        }
                    }
                    return res;
                });
    }

    @Override
    public void handleComplete() {
        if (responseInfo.successNum < pool.getK()) {
            res.onNext(false);
            return;
        }

        //存放没有得到k+m份返回的元数据
        for (Counter counter : linkedList) {
            //返回响应不及k个的元数据需要马上进行repair，若失败则lsfile请求返500
            //else，没有得到k+m份返回或有不一致的元数据在异常处理中修复，可延后。
            MetaData metaData = counter.t.var3;
            if (metaData.latest && !StringUtils.isEmpty(preKey) && preKey.equals(metaData.key)) {
                metaData.setLatest(false);
            } else if (nextLatest && nextKey.equals(metaData.key)) {
                metaData.setLatest(true);
                nextLatest = false;
            }
            if (counter.c < pool.getK()) {
                repairFlux.onNext(1);
                Disposable subscribe = repair(counter, nodeList).subscribe(b -> {
                    if (b) {
                        repairFlux.onNext(-1);
                    } else {
                        try {
                            res.onNext(false);
                        } catch (Exception e) {

                        }
                    }
                });
                res.onErrorReturn(false);
                Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
            } else {
                if (counter.c != pool.getK() + pool.getM() || !counter.identical) {
                    putErrorList(counter);
                }
            }
        }
        publishErrorList();
        repairFlux.onNext(0);
    }

    @Override
    protected void putErrorList(Counter counter) {
        MetaData metaData = counter.t.var3;
        Tuple3<String, String, String> tuple3 = new Tuple3<>(metaData.bucket, metaData.key, metaData.versionId);
        if (metaData.snapshotMark == null) {
            errorList.add(tuple3);
        } else {
            snapshotErrorList.add(new Tuple2<>(metaData.snapshotMark, tuple3));
        }
    }

    @Override
    protected void publishErrorList() {
        //防止io占用太大，单条消息不能太大，需要分割errorList。
        if (snapshotErrorList.isEmpty()) {
            List<List<Tuple3<String, String, String>>> partition = Lists.partition(errorList, 50);

            for (List<Tuple3<String, String, String>> counterList : partition) {
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("lun", "lsversion")
                        .put("counterList", Json.encode(counterList));
                ObjectPublisher.basicPublishToLowSpeed(ERROR_REPAIR_LSVERSION, errorMsg);
            }
        } else {
            // 开启桶快照
            List<List<Tuple2<String, Tuple3<String, String, String>>>> partition = Lists.partition(snapshotErrorList, 50);

            for (List<Tuple2<String, Tuple3<String, String, String>>> counterList : partition) {
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("lun", "lsversion")
                        .put("counterList", Json.encode(counterList));
                Optional.ofNullable(snapshotLink).ifPresent(v -> errorMsg.put("snapshotLink", v));
                ObjectPublisher.basicPublishToLowSpeed(ERROR_REPAIR_LSVERSION_SNAP, errorMsg);
            }
        }

    }

}
