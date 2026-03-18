package com.macrosan.storage.client.channel;


import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Prefix;
import com.macrosan.message.xmlmsg.versions.DeleteMarker;
import com.macrosan.message.xmlmsg.versions.ListVersionsResult;
import com.macrosan.message.xmlmsg.versions.Version;
import com.macrosan.message.xmlmsg.versions.VersionBase;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.AbstractListClient;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListVersionsClientHandler;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import io.vertx.core.json.Json;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.ServerConstants.ETAG;
import static com.macrosan.constants.ServerConstants.LAST_MODIFY;

public class ListVersionsMergeChannel extends AbstractIndexShardMergeChannel<Tuple3<Boolean, String, MetaData>> {
    private final ListVersionsResult listVersionsResult;
    private final List<VersionBase> contents;
    private final List<Prefix> prefixes;
    private int resCount = 0;

    public ListVersionsMergeChannel(String bucket, ListVersionsResult listVersionsResult, StoragePool storagePool, List<String> shardNodes, MsHttpRequest request) {
        super(bucket, storagePool, shardNodes, request, listVersionsResult.getMaxKeys(), listVersionsResult.getPrefix());
        contents = new ArrayList<>(listVersionsResult.getMaxKeys());
        prefixes = new ArrayList<>(listVersionsResult.getMaxKeys());
        this.listVersionsResult = listVersionsResult;
    }

    public ListVersionsMergeChannel(String bucket, ListVersionsResult listVersionsResult, StoragePool storagePool, List<String> shardNodes, MsHttpRequest request, String snapshotLink) {
        this(bucket, listVersionsResult, storagePool, shardNodes, request);
        super.snapshotLink = snapshotLink;
    }

    @Override
    protected Comparator<Tuple3<Boolean, String, MetaData>> comparator() {
        return (t1, t2) -> {
            int cmp = getKey(t1).compareTo(getKey(t2));
            if (cmp == 0) {
                return -getStampKey(t1).compareTo(getStampKey(t2));
            }
            return cmp;
        };
    }

    private final List<String> commPrefix = new ArrayList<>(1);

    @Override
    public void write(Tuple3<Boolean, String, MetaData> tuple3) {
        synchronized (commPrefix) {
            // 过滤重复的公共前缀
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
        return ClientTemplate.oneResponse(msgs, ErasureServer.PayloadMetaType.LIST_VERSIONS, new TypeReference<Tuple3<Boolean, String, MetaData>[]>() {
        }, infoList);
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
    protected AbstractListClient<Tuple3<Boolean, String, MetaData>> getListClientHandler(StoragePool pool, ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        ListVersionsClientHandler listVersionsClientHandler = new ListVersionsClientHandler(pool, null, responseInfo, nodeList, snapshotLink);
        listVersionsClientHandler.setChannel(this);
        return listVersionsClientHandler;
    }

    protected String getKey(Tuple3<Boolean, String, MetaData> t) {
        return Utils.getMetaDataKey("", t.var3.getBucket(), t.var3.getKey(), null);
    }

    private String getStampKey(Tuple3<Boolean, String, MetaData> t) {
        return Utils.getMetaDataKey("", t.var3.getBucket(), t.var3.getKey(), t.var3.versionId, t.var3.stamp, null);
    }
}
