package com.macrosan.storage.client.channel;

import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ListMultipartUploadsResult;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Prefix;
import com.macrosan.message.xmlmsg.section.Upload;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.AbstractListClient;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListMultiPartClientHandler;
import com.macrosan.utils.functional.Tuple3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.macrosan.ec.part.PartClient.LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_MULTI_PART_UPLOAD;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className LisrMultiPartClientMergeChannel
 * @date 2023/2/24 13:59
 */
public class ListMultiPartClientMergeChannel extends AbstractIndexShardMergeChannel<Tuple3<Boolean, String, InitPartInfo>> {
    private ListMultipartUploadsResult listMultipartUploads;
    private Owner uploadOwner;
    private List<Upload> uploads;
    private List<Prefix> prefixes;
    private int resCount = 0;

    public ListMultiPartClientMergeChannel(Owner uploadOwner, ListMultipartUploadsResult listMultipartUploads, StoragePool storagePool, List<String> shardNodes, MsHttpRequest request, String currentSnapshotMark) {
        super(listMultipartUploads.getBucket(), storagePool, shardNodes, request, listMultipartUploads.getMaxUploads(), listMultipartUploads.getPrefix());
        uploads = new ArrayList<>(listMultipartUploads.getMaxUploads());
        prefixes = new ArrayList<>(listMultipartUploads.getMaxUploads());
        this.listMultipartUploads = listMultipartUploads;
        this.uploadOwner = uploadOwner;
        super.currentSnapshotMark = currentSnapshotMark;
    }

    protected String getKey(Tuple3<Boolean, String, InitPartInfo> t) {
        return InitPartInfo.getPartKey("", t.var3.bucket, t.var3.object, t.var3.uploadId);
    }

    @Override
    protected Comparator<Tuple3<Boolean, String, InitPartInfo>> comparator() {
        return new Comparator<Tuple3<Boolean, String, InitPartInfo>>() {
            @Override
            public int compare(Tuple3<Boolean, String, InitPartInfo> t1, Tuple3<Boolean, String, InitPartInfo> t2) {
                int cmp = getKey(t1).compareTo(getKey(t2));
                if (cmp != 0) {
                    return cmp;
                }
                InitPartInfo info1 = t1.var3;
                InitPartInfo info2 = t2.var3;

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
        };
    }

    @Override
    protected ClientTemplate.ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> send(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> infoList) {
        return ClientTemplate.oneResponse(msgs, LIST_MULTI_PART_UPLOAD, LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE, infoList);
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
    protected AbstractListClient<Tuple3<Boolean, String, InitPartInfo>> getListClientHandler(StoragePool pool, ClientTemplate.ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> responseInfo, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        ListMultiPartClientHandler handler = new ListMultiPartClientHandler(null, null, responseInfo, nodeList, request, listMultipartUploads.getBucket(), currentSnapshotMark);
        handler.setChannel(this);
        return handler;
    }

    private static Upload mapToUpload(InitPartInfo info, Owner uploadOwner) {
        return new Upload()
                .setInitiated(info.initiated)
                .setUploadId(info.uploadId)
                .setInitiator(new Owner().setDisplayName(info.initAccountName).setId(info.initAccount))
                .setOwner(uploadOwner)
                .setKey(info.object);
    }
}
