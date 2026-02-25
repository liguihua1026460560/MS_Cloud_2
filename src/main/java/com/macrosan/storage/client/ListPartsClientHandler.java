package com.macrosan.storage.client;

import com.google.common.collect.Lists;
import com.macrosan.ec.part.PartClient;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ListPartsResult;
import com.macrosan.message.xmlmsg.section.Part;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import reactor.core.publisher.Mono;

import java.util.*;

import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_REPAIR_PART_LIST;

/**
 * @author gaozhiyuan
 */
public class ListPartsClientHandler extends AbstractListClient<PartInfo> {
    private List<Part> parts;
    private ListPartsResult listPartsResult;
    private int resCount = 0;
    public List<PartInfo> partInfoList;
    private List<PartInfo> errorList = new LinkedList<>();

    public boolean publishError;

    public ListPartsClientHandler(ListPartsResult listPartsResult, ResponseInfo<PartInfo[]> responseInfo,
                                  List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink) {
        this(listPartsResult, responseInfo, nodeList, request, true, currentSnapshotMark,snapshotLink );
    }

    public ListPartsClientHandler(ListPartsResult listPartsResult, ResponseInfo<PartInfo[]> responseInfo,
                                  List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, boolean publishError, String currentSnapshotMark, String snapshotLink) {
        super(StoragePoolFactory.getMetaStoragePool(request != null ? request.getBucketName() : listPartsResult.getBucket()), responseInfo, nodeList, request);
        super.currentSnapshotMark = currentSnapshotMark;
        super.snapshotLink = snapshotLink;
        if (listPartsResult == null) {
            return;
        }
        parts = new ArrayList<>(listPartsResult.getMaxParts());
        partInfoList = new ArrayList<>(listPartsResult.getMaxParts());
        this.listPartsResult = listPartsResult;
        this.publishError = publishError;
    }

    public ListPartsClientHandler(ListPartsResult listPartsResult, ResponseInfo<PartInfo[]> responseInfo,
                                  List<Tuple3<String, String, String>> nodeList, String bucketName) {
        super(StoragePoolFactory.getMetaStoragePool(bucketName), responseInfo, nodeList, null);
        parts = new ArrayList<>(listPartsResult.getMaxParts());
        partInfoList = new ArrayList<>(listPartsResult.getMaxParts());
        this.listPartsResult = listPartsResult;
    }

    @Override
    protected void publishResult() {
        listPartsResult.setParts(parts);
        if (!listPartsResult.isTruncated()) {
            listPartsResult.setNextPartNumberMarker(0);
        }
        res.onNext(true);
    }

    @Override
    protected String getKey(PartInfo partInfo) {
        return PartInfo.getPartKey(vnode, partInfo.bucket, partInfo.object, partInfo.uploadId, partInfo.partNum, null);
    }

    @Override
    protected int compareTo(PartInfo partInfo1, PartInfo partInfo2) {
        int partNum1 = Integer.parseInt(partInfo1.partNum);
        int partNum2 = Integer.parseInt(partInfo2.partNum);
        if (!Objects.equals(partInfo1.snapshotMark, partInfo2.snapshotMark)) {
            // 不同快照下的分段，当前快照标记下为最新
            return partInfo1.snapshotMark.equals(currentSnapshotMark) ? 1 : -1;
        }

        if (partNum1 == partNum2) {
            return partInfo1.versionNum.compareTo(partInfo2.versionNum);
        } else {
            return partNum2 - partNum1;
        }
    }

    @Override
    protected void handleResult(PartInfo partInfo) {
        if (partInfo.delete) {
            return;
        }
        if (resCount < listPartsResult.getMaxParts()) {
            Part part = new Part()
                    .setEtag('"' + partInfo.getEtag() + '"')
                    .setLastModified(partInfo.getLastModified())
                    .setSize(partInfo.getPartSize())
                    .setPartNumber(Integer.valueOf(partInfo.getPartNum()))
                    .setFileName(partInfo.getFileName());

            listPartsResult.setNextPartNumberMarker(Integer.parseInt(partInfo.getPartNum()));
            parts.add(part);
            partInfoList.add(partInfo);
            resCount++;
        } else {
            listPartsResult.setTruncated(true);
        }
    }

    @Override
    protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList) {
        PartInfo info = counter.t;
        String partKey = info.getPartKey(nodeList.get(0).var3);
        return PartClient.getPartInfo(info.bucket, info.object, info.uploadId, info.partNum, partKey, nodeList, request, info.snapshotMark, snapshotLink,null)
                .map(partInfo -> {
                    boolean res = !partInfo.equals(PartInfo.ERROR_PART_INFO);
                    if (res && !partInfo.equals(PartInfo.NOT_FOUND_PART_INFO) && !partInfo
                            .equals(PartInfo.NO_SUCH_UPLOAD_ID_PART_INFO)) {
                        counter.t = partInfo;
                    }
                    return res;
                });
    }

    @Override
    protected void putErrorList(Counter counter) {
        PartInfo partInfo = counter.t;
        errorList.add(partInfo);
    }

    @Override
    protected void publishErrorList() {
        if (!publishError) {
            return;
        }
        //防止io占用太大，单条消息不能太大，需要分割errorList。
        List<List<PartInfo>> partition = Lists.partition(errorList, 50);

        for (List<PartInfo> counterList : partition) {
            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                    .put("lun", "partList")
                    .put("counterList", Json.encode(counterList));
            Optional.ofNullable(snapshotLink).ifPresent(v->errorMsg.put("snapshotLink",v));
            ObjectPublisher.basicPublishToLowSpeed(ERROR_REPAIR_PART_LIST, errorMsg);
        }
    }
}
