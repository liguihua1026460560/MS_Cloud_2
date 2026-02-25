package com.macrosan.storage.client;

import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.xmlmsg.ListMultipartUploadsResult;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Prefix;
import com.macrosan.message.xmlmsg.section.Upload;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Log4j2
public class ListOriginPartHandler extends AbstractListClient<Tuple3<Boolean, String, InitPartInfo>> {
    private ListMultipartUploadsResult listMultipartUploads;
    private Owner uploadOwner;
    private List<Upload> uploads;
    private List<Prefix> prefixes;
    private int resCount = 0;
    private List<Tuple3<String, String, String>> errorList = new LinkedList<>();

    public ListOriginPartHandler(StoragePool pool, Owner uploadOwner, ListMultipartUploadsResult listMultipartUploads,
                                 ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> responseInfo,
                                 List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        super(pool, responseInfo, nodeList, request);
        if (uploadOwner == null || listMultipartUploads == null) {
            return;
        }
        uploads = new ArrayList<>(listMultipartUploads.getMaxUploads());
        prefixes = new ArrayList<>(listMultipartUploads.getMaxUploads());
        this.listMultipartUploads = listMultipartUploads;
        this.uploadOwner = uploadOwner;
    }

    public ListOriginPartHandler(Owner uploadOwner, ListMultipartUploadsResult listMultipartUploads,
                                 ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> responseInfo,
                                 List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String bucket) {
        super(responseInfo, nodeList, request, bucket);
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
        return t.var3.getPartKey(vnode);
    }

    @Override
    protected int compareTo(Tuple3<Boolean, String, InitPartInfo> t1, Tuple3<Boolean, String, InitPartInfo> t2) {
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
        return Mono.just(true);
    }

    @Override
    protected void putErrorList(Counter counter) {
    }

    @Override
    protected void publishErrorList() {
    }
}
