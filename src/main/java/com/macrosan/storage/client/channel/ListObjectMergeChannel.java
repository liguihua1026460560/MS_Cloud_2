package com.macrosan.storage.client.channel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.Utils;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ListBucketResult;
import com.macrosan.message.xmlmsg.section.Contents;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Prefix;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.AbstractListClient;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListObjectClientHandler;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.ServerConstants.ETAG;
import static com.macrosan.constants.ServerConstants.LAST_MODIFY;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_OBJECTS;

@Log4j2
public class ListObjectMergeChannel extends AbstractIndexShardMergeChannel<Tuple3<Boolean, String, MetaData>> {

    private final ListBucketResult listBucketResult;
    private final List<Contents> contentsList;
    private final List<Prefix> prefixList;
    private int resCount = 0;

    public ListObjectMergeChannel(String bucket, ListBucketResult listBucketResult, StoragePool storagePool, List<String> shardNodes, MsHttpRequest request, String currentSnapshotMark,String snapshotLink) {
        super(bucket, storagePool, shardNodes, request, listBucketResult.getMaxKeys(), listBucketResult.getPrefix());
        this.listBucketResult = listBucketResult;
        contentsList = new ArrayList<>(listBucketResult.getMaxKeys());
        prefixList = new ArrayList<>(listBucketResult.getMaxKeys());
        super.currentSnapshotMark = currentSnapshotMark;
        super.snapshotLink = snapshotLink;
    }

    @Override
    protected Comparator<Tuple3<Boolean, String, MetaData>> comparator() {
        return Comparator.comparing(this::getKey);
    }

    private final List<String> commPrefix = new ArrayList<>(1);

    @Override
    public void write(Tuple3<Boolean, String, MetaData> tuple3) {
        synchronized (commPrefix) {
            if (tuple3.var1 && !commPrefix.contains(getKey(tuple3))) {
                queue.add(tuple3);
                commPrefix.add(getKey(tuple3));
            } else if (!tuple3.var1) {
                queue.add(tuple3);
            }
        }
    }

    @Override
    protected ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> send(List<SocketReqMsg> msgs, List<Tuple3<String, String, String>> infoList) {
        return ClientTemplate.oneResponse(msgs, LIST_OBJECTS, new TypeReference<Tuple3<Boolean, String, MetaData>[]>() {
        }, infoList);
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
    protected void publishResult() {
        listBucketResult.setPrefixlist(prefixList);
        listBucketResult.setContents(contentsList);
        if (!listBucketResult.isTruncated()) {
            listBucketResult.setNextMarker("");
        }
        queue.clear();
        res.onNext(true);
    }

    @Override
    protected AbstractListClient<Tuple3<Boolean, String, MetaData>> getListClientHandler(StoragePool pool, ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        ListObjectClientHandler listObjectClientHandler = new ListObjectClientHandler(null, responseInfo, nodeList, request, currentSnapshotMark,snapshotLink );
        listObjectClientHandler.setChannel(this);
        if (!this.getUpdateDirList().isEmpty()) {
            listObjectClientHandler.setUpdateCapDirList(this.getUpdateDirList());
        }
        return listObjectClientHandler;
    }

    protected String getKey(Tuple3<Boolean, String, MetaData> t) {
        return Utils.getMetaDataKey("", t.var3.getBucket(), t.var3.getKey(), null);
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
