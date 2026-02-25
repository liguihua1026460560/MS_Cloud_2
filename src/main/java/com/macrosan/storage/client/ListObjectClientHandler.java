package com.macrosan.storage.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ListBucketResult;
import com.macrosan.message.xmlmsg.section.Contents;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Prefix;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import io.vertx.core.json.Json;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.*;

import static com.macrosan.constants.ServerConstants.ETAG;
import static com.macrosan.constants.ServerConstants.LAST_MODIFY;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;

/**
 * @auther wuhaizhong
 * @date 2020/4/24
 */
@Log4j2
public class ListObjectClientHandler extends AbstractListClient<Tuple3<Boolean, String, MetaData>> {
    private ListBucketResult listBucketResult;
    private List<Contents> contentsList;
    private List<Prefix> prefixList;
    private int resCount = 0;
    private List<Tuple3<String, String, String>> errorList = new LinkedList<>();
    private List<Tuple2<String, Tuple3<String, String, String>>> snapshotErrorList = new LinkedList<>();
    @Setter
    private List<String> updateCapDirList = new LinkedList<>();

    public ListObjectClientHandler(ListBucketResult listBucketResult,
                                   ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo,
                                   List<Tuple3<String, String, String>> nodeList,
                                   MsHttpRequest request, String currentSnapshotMark, String snapshotLink) {
        super(StoragePoolFactory.getMetaStoragePool(request.getBucketName()), responseInfo, nodeList, request);
        super.currentSnapshotMark = currentSnapshotMark;
        super.snapshotLink = snapshotLink;
        if (listBucketResult != null) {
            this.listBucketResult = listBucketResult;
            contentsList = new ArrayList<>(listBucketResult.getMaxKeys());
            prefixList = new ArrayList<>(listBucketResult.getMaxKeys());
        }
    }

    @Override
    protected void publishResult() {
        listBucketResult.setPrefixlist(prefixList);
        listBucketResult.setContents(contentsList);
        if (!listBucketResult.isTruncated()) {
            listBucketResult.setNextMarker("");
        }
        res.onNext(true);
    }

    @Override
    protected String getKey(Tuple3<Boolean, String, MetaData> t) {
        return Utils.getMetaDataKey(vnode, t.var3.getBucket(), t.var3.getKey(), null);
    }

    @Override
    protected int compareTo(Tuple3<Boolean, String, MetaData> t1, Tuple3<Boolean, String, MetaData> t2) {
        if (!Objects.equals(t1.var3.snapshotMark, t2.var3.snapshotMark)) {
            // 同名对象不是一个快照下的，则当前快照标记为最新对象
            return t1.var3.snapshotMark.equals(currentSnapshotMark) ? 1 : -1;
        }
        if (t2.var3.versionNum.equals(t1.var3.versionNum)) {
            // versionNum一致按key排序
            return t2.var3.key.compareTo(t1.var3.key);
        } else {
            //versionNum不一致最新versionNum为较小值
            return t1.var3.versionNum.compareTo(t2.var3.versionNum);
        }
    }

    @Override
    protected void handleResult(Tuple3<Boolean, String, MetaData> t) {
        MetaData metaData = t.var3;
        if (!metaData.equals(MetaData.NOT_FOUND_META) && !metaData.deleteMark && !metaData.deleteMarker) {
            if (resCount < listBucketResult.getMaxKeys()) {
                if (t.var1) {
                    prefixList.add(new Prefix().setPrefix(t.var2));
                    listBucketResult.setNextMarker(metaData.key);
                } else {
                    contentsList.add(mapToContents(metaData));
                    listBucketResult.setNextMarker(metaData.getKey());
                }

                resCount++;
            } else {
                listBucketResult.setTruncated(true);
            }
        }
    }

    @Override
    protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList) {
        MetaData metaData = counter.t.var3;
        if (counter.t.var1) {
            return Mono.just(true);
        }
        return ErasureClient.getObjectMetaVersion(metaData.getBucket(), metaData.getKey(), metaData.versionId, nodeList, request, metaData.snapshotMark, snapshotLink)
                .flatMap(meta -> {
                    boolean res = !meta.equals(MetaData.ERROR_META);
                    if (res) {
                        if (meta.deleteMark) {
                            counter.t.var3 = meta;
                            return Mono.just(res);
                        }
                        return ErasureClient.getObjectMeta(metaData.getBucket(), metaData.getKey(), nodeList, request, metaData.snapshotMark, snapshotLink)
                                .flatMap(latestMeta -> {
                                    boolean res1 = !latestMeta.equals(MetaData.ERROR_META);
                                    if (res1) {
                                        if (latestMeta.deleteMarker) {
                                            counter.t.var3 = latestMeta;
                                        } else if (!latestMeta.equals(MetaData.NOT_FOUND_META)) {
                                            counter.t.var3 = meta;
                                        }
                                    }
                                    return Mono.just(res1);
                                });
                    }
                    return Mono.just(res);
                });
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
        if (snapshotErrorList.isEmpty()){
            List<List<Tuple3<String, String, String>>> partition = Lists.partition(errorList, 50);

            for (List<Tuple3<String, String, String>> counterList : partition) {
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("lun", "lsfile")
                        .put("counterList", Json.encode(counterList));
                if (this.updateCapDirList.isEmpty()) {
                    ObjectPublisher.basicPublishToLowSpeed(ERROR_REPAIR_LSFILE, errorMsg);
                }else{
                    errorMsg.put("updateDirList", Json.encode(this.updateCapDirList));
                    ObjectPublisher.basicPublishToLowSpeed(ERROR_REPAIR_LSFILE_WITH_FS_QUOTA, errorMsg);
                }
            }
        }else {
            // 开启桶快照
            List<List<Tuple2<String, Tuple3<String, String, String>>>> partition = Lists.partition(snapshotErrorList, 50);

            for (List<Tuple2<String, Tuple3<String, String, String>>> counterList : partition) {
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("lun", "lsfile")
                        .put("counterList", Json.encode(counterList));
                Optional.ofNullable(snapshotLink).ifPresent(v -> errorMsg.put("snapshotLink", v));
                ObjectPublisher.basicPublishToLowSpeed(ERROR_REPAIR_LSFILE_SNAP, errorMsg);
            }
        }
    }

    private static Contents mapToContents(MetaData metaData) {
        String sysMetaData = metaData.getSysMetaData();
        Map<String, String> sysMetaMap = Json.decodeValue(sysMetaData, new TypeReference<Map<String, String>>() {
        });

        return new Contents()
                .setEtag('"' + sysMetaMap.get(ETAG) + '"')
                .setKey(metaData.getKey())
                .setLastModified(MsDateUtils.dateToISO8601(sysMetaMap.get(LAST_MODIFY)))
                .setOwner(new Owner().setDisplayName(sysMetaMap.get("displayName")).setId(sysMetaMap.get("owner")))
                .setSize(String.valueOf(metaData.endIndex - metaData.startIndex + 1))
                .setStorageClass("STANDARD");
    }
}
