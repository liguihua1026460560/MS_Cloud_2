package com.macrosan.storage.client;

import com.google.common.collect.Lists;
import com.macrosan.ec.part.PartClient;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ListMultipartUploadsResult;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Prefix;
import com.macrosan.message.xmlmsg.section.Upload;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_REPAIR_PART_MULTI_LIST;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_REPAIR_PART_MULTI_LIST_SNAP;
import static com.macrosan.message.jsonmsg.InitPartInfo.ERROR_INIT_PART_INFO;

@Log4j2
public class ListMultiPartClientHandler extends AbstractListClient<Tuple3<Boolean, String, InitPartInfo>> {
    private ListMultipartUploadsResult listMultipartUploads;
    private Owner uploadOwner;
    private List<Upload> uploads;
    private List<Prefix> prefixes;
    private int resCount = 0;
    private List<Tuple3<String, String, String>> errorList = new LinkedList<>();
    private List<Tuple2<String, Tuple3<String, String, String>>> snapshotErrorList = new LinkedList<>();

    public ListMultiPartClientHandler(Owner uploadOwner, ListMultipartUploadsResult listMultipartUploads,
                                      ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> responseInfo,
                                      List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String bucket, String currentSnapshotMark) {

        super(StoragePoolFactory.getMetaStoragePool(bucket), responseInfo, nodeList, request);
        super.currentSnapshotMark = currentSnapshotMark;
        if (listMultipartUploads == null) {
            return;
        }
        uploads = new ArrayList<>(listMultipartUploads.getMaxUploads());
        prefixes = new ArrayList<>(listMultipartUploads.getMaxUploads());
        this.listMultipartUploads = listMultipartUploads;
        this.uploadOwner = uploadOwner;
    }

    @Override
    protected void publishResult() {
        listMultipartUploads.setUploads(uploads);
        listMultipartUploads.setPrefixList(prefixes);
        if (!listMultipartUploads.isTruncated()) {
            listMultipartUploads.setNextKeyMarker("");
            listMultipartUploads.setNextUploadIdMarker("");
        }
        res.onNext(true);
    }

    @Override
    protected String getKey(Tuple3<Boolean, String, InitPartInfo> t) {
        return InitPartInfo.getPartKey(vnode, t.var3.bucket, t.var3.object, t.var3.uploadId);
    }

    @Override
    protected int compareTo(Tuple3<Boolean, String, InitPartInfo> t1, Tuple3<Boolean, String, InitPartInfo> t2) {
        InitPartInfo info1 = t1.var3;
        InitPartInfo info2 = t2.var3;

        if (!Objects.equals(info1.snapshotMark, info2.snapshotMark)) {
            // 不同快照下的分段，当前快照标记下为最新
            return info1.snapshotMark.equals(currentSnapshotMark) ? 1 : -1;
        }
        int res = info2.object.compareTo(info1.object);
        if (res == 0) {
            res = info2.uploadId.compareTo(info1.uploadId);
            if (res == 0) {
                return info1.versionNum.compareTo(info2.versionNum);
            } else {
                return res;
            }

        } else {
            return res;
        }
    }

    @Override
    protected void handleResult(Tuple3<Boolean, String, InitPartInfo> t) {
        if (!t.var3.delete) {
            if (resCount < listMultipartUploads.getMaxUploads()) {
                if (t.var1) {
                    prefixes.add(new Prefix().setPrefix(t.var2));
                    listMultipartUploads.setNextKeyMarker(t.var3.object);
                    listMultipartUploads.setNextUploadIdMarker("");
                } else {
                    InitPartInfo initPartInfo = t.var3;
                    uploads.add(mapToUpload(initPartInfo, uploadOwner));
                    listMultipartUploads.setNextKeyMarker(initPartInfo.object);
                    listMultipartUploads.setNextUploadIdMarker(initPartInfo.uploadId);
                }

                resCount++;
            } else {
                listMultipartUploads.setTruncated(true);
            }
        }
    }

    private static Upload mapToUpload(InitPartInfo info, Owner uploadOwner) {
        return new Upload()
                .setInitiated(info.initiated)
                .setUploadId(info.uploadId)
                .setInitiator(new Owner().setDisplayName(info.initAccountName).setId(info.initAccount))
                .setOwner(uploadOwner)
                .setKey(info.object);
    }

    @Override
    protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList) {
        return PartClient.getInitPartInfo(counter.t.var3.bucket, counter.t.var3.object, counter.t.var3.uploadId, nodeList, request, counter.t.var3.snapshotMark, null).map(res -> res != ERROR_INIT_PART_INFO);
    }

    @Override
    protected void putErrorList(Counter counter) {
        InitPartInfo initPartInfo = counter.t.var3;
        Tuple3<String, String, String> tuple3 = new Tuple3<>(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId);
        if (initPartInfo.snapshotMark == null) {
            errorList.add(tuple3);
        } else {
            snapshotErrorList.add(new Tuple2<>(initPartInfo.snapshotMark, tuple3));
        }
    }

    @Override
    protected void publishErrorList() {
        //防止io占用太大，单条消息不能太大，需要分割errorList。
        if (snapshotErrorList.isEmpty()){
            List<List<Tuple3<String, String, String>>> partition = Lists.partition(errorList, 50);

            for (List<Tuple3<String, String, String>> counterList : partition) {
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("lun", "partMultiList")
                        .put("counterList", Json.encode(counterList));
                ObjectPublisher.basicPublishToLowSpeed(ERROR_REPAIR_PART_MULTI_LIST, errorMsg);
            }
        }else {
            List<List<Tuple2<String, Tuple3<String, String, String>>>> partition = Lists.partition(snapshotErrorList, 50);

            for (List<Tuple2<String, Tuple3<String, String, String>>> counterList : partition) {
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("lun", "partMultiList")
                        .put("counterList", Json.encode(counterList));
                ObjectPublisher.basicPublishToLowSpeed(ERROR_REPAIR_PART_MULTI_LIST_SNAP, errorMsg);
            }
        }
    }
}
