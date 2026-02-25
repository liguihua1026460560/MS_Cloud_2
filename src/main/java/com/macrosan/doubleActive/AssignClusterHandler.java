package com.macrosan.doubleActive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.httpserver.MossHttpClient;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.ErrorNo.NO_SUCH_CLUSTER_NAME;
import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.httpserver.MossHttpClient.NAME_INDEX_FLAT_MAP;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;

@Log4j2
public class AssignClusterHandler {
    private static AssignClusterHandler instance;

    public static AssignClusterHandler getInstance() {
        if (instance == null) {
            instance = new AssignClusterHandler();
        }
        return instance;
    }

    public static final Vertx vertx = ServerConfig.getInstance().getVertx();

    public static final String ClUSTER_NAME_HEADER = "moss-cluster-name";
    public static final String SKIP_SIGNAL = "assign_skip_signal";
    public static final String ASSIGN_DELETE_SIGNAL = "assign_delete_signal";

    public Integer checkClusterIndexHeader(MsHttpRequest request) {
        int clusterIndex;
        String clusterName = new String(request.getHeader(ClUSTER_NAME_HEADER).getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
        if (!NAME_INDEX_FLAT_MAP.containsKey(clusterName)) {
            throw new MsException(NO_SUCH_CLUSTER_NAME, "cluster index does not exist. " + clusterName);
        }
        clusterIndex = NAME_INDEX_FLAT_MAP.get(clusterName);
        if (!MossHttpClient.INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)) {
            throw new MsException(NO_SUCH_CLUSTER_NAME, "cluster index does not exist. " + clusterIndex);
        }
        return clusterIndex;
    }

    public Mono<Integer> sendLongRequest(MsHttpRequest request, byte[] requestBody) {
        final AtomicInteger times = new AtomicInteger(0);
        String requestId = getRequestId();
        request.addMember(REQUESTID, requestId);
        final byte[] xmlHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>".getBytes();
        long timerId = vertx.setPeriodic(20000, (idP) -> {
            try {
                if (times.incrementAndGet() == 1) {
                    addPublicHeaders(request, requestId).putHeader(CONTENT_TYPE, "application/xml")
                            .putHeader(TRANSFER_ENCODING, "chunked")
                            .write(Buffer.buffer(xmlHeader));
                }
                request.response().write("\n");
            } catch (Exception e) {
                log.info("still run timer, {}", e.getMessage());
                vertx.cancelTimer(idP);
            }
        });

        MonoProcessor<Integer> res = MonoProcessor.create();
        int clusterIndex = checkClusterIndexHeader(request);
        Map<String, String> headers = new HashMap<>();
        request.headers().entries().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
        MossHttpClient.getInstance().sendRequest(clusterIndex, request.getBucketName(), request.getObjectName(), request.uri(), request.method(), headers, requestBody, null)
                .doFinally(s -> vertx.cancelTimer(timerId))
                .doOnNext(t -> {
                    if (INTERNAL_SERVER_ERROR == t.var1) {
                        throw new MsException(UNKNOWN_ERROR, "delete object internal error");
                    }
                })
                .doOnNext(tuple3 -> {
                    if (times.get() > 0) {
                        MultiMap respHeaders = tuple3.var3;
                        String bodyStr = StringUtils.isNotBlank(respHeaders.get("moss-body")) ? respHeaders.get("moss-body") : "";
                        byte[] data = bodyStr.getBytes();
                        request.response().end(Buffer.buffer(data).slice(xmlHeader.length, data.length));
                    } else {
                        pumpClusterResponse(tuple3, request);
                    }
                    res.onNext(1);
                })
                .doOnError(e -> {
                    log.error("sendRequest err, {} {}", clusterIndex, request.uri(), e);
                    res.onError(e);
                })
                .subscribe();
        return res;
    }

    public Mono<Integer> sendRequest(MsHttpRequest request, byte[] requestBody) {
        MonoProcessor<Integer> res = MonoProcessor.create();
        int clusterIndex = checkClusterIndexHeader(request);
        Map<String, String> headers = new HashMap<>();
        request.headers().entries().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
        MossHttpClient.getInstance().sendRequest(clusterIndex, request.getBucketName(), request.getObjectName(), request.uri(), request.method(), headers, requestBody, null)
                .doOnNext(t -> {
                    if (INTERNAL_SERVER_ERROR == t.var1) {
                        throw new MsException(UNKNOWN_ERROR, "delete object internal error");
                    }
                })
                .doOnNext(tuple3 -> {
                    pumpClusterResponse(tuple3, request);
                    res.onNext(1);
                })
                .doOnError(e -> {
                    log.error("sendRequest err, {} {}", clusterIndex, request.uri(), e);
                    res.onError(e);
                })
                .subscribe();
        return res;
    }

    public void pumpClusterResponse(Tuple3<Integer, String, MultiMap> tuple3, MsHttpRequest request) {
        request.response().setStatusCode(tuple3.var1);
        request.response().setStatusMessage(tuple3.var2);
        MultiMap respHeaders = tuple3.var3;
        String bodyStr = StringUtils.isNotBlank(respHeaders.get("moss-body")) ? respHeaders.get("moss-body") : "";
        respHeaders.remove("moss-body");
        respHeaders.forEach(header -> request.response().putHeader(header.getKey(), header.getValue()));
        if (StringUtils.isNotBlank(bodyStr)) {
            request.response().write(bodyStr);
        }
        request.response().end();
    }

    public Mono<Integer> markForDelete(MetaData oldMeta, int clusterIndex, MsHttpRequest request) {
//        ErasureClient.getObjectMetaVersionResOnlyRead()
        if (oldMeta.equals(ERROR_META)) {
            throw new MsException(UNKNOWN_ERROR, "Get Object Meta Data fail");
        }
        Map<String, String> sysMetaMap = Json.decodeValue(oldMeta.sysMetaData, new TypeReference<Map<String, String>>() {
        });
        String s = sysMetaMap.get(ASSIGN_DELETE_SIGNAL);
        HashSet<Integer> indexSet = new HashSet<>();
        if (StringUtils.isNotBlank(s)) {
            indexSet.addAll(Json.decodeValue(s, new TypeReference<HashSet<Integer>>() {
            }));
        }
        indexSet.add(clusterIndex);
        sysMetaMap.put(ASSIGN_DELETE_SIGNAL, Json.encode(indexSet));
        MetaData metaData = oldMeta.clone();
        metaData.setSysMetaData(Json.encode(sysMetaMap));
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(oldMeta.bucket);
        String bucketVnode = storagePool.getBucketVnodeId(oldMeta.bucket, oldMeta.key);
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ErasureClient.updateMetaDataAcl(Utils.getVersionMetaDataKey(bucketVnode, oldMeta.bucket, oldMeta.key, oldMeta.versionId, oldMeta.snapshotMark),
                        metaData, nodeList, request, null));

    }


}
