package com.macrosan.utils.essearch;

import com.alibaba.fastjson.JSONObject;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.error.ErrorConstant;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.EsMeta;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Entry;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_DEL_ES_META;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.message.jsonmsg.EsMeta.*;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.utils.essearch.EsMetaTaskScanner.SWITCH_BUCKET_KEYS;
import static com.macrosan.utils.essearch.EsMetaTaskScanner.scanInfoMap;


/**
 * @author pangchangya
 */
@Log4j2
public class EsMetaTask {


    public static final String INDEX = "moss";
    public static final String TYPE = "userid";
    private static IndexController indexController;
    public static final int MAX_SEARCH_COUNT = 2000;

    public static void init() {
        indexController = IndexController.getInstance();
        EsMetaTaskScanner.getInstance().init();
    }

    public static Mono<Boolean> putEsMeta(EsMeta esMeta, boolean needCheck) {
        EsMetaOptions options = new EsMetaOptions().setNeedCheck(needCheck);
        return putEsMeta(esMeta, options);
    }

    public static Mono<Boolean> putEsMeta(EsMeta esMeta, EsMetaOptions options) {
        Mono<EsMeta> pMono = Mono.just(esMeta);
        if (StringUtils.isBlank(esMeta.versionNum)) {
            if (esMeta.inode > 0) {
                pMono = pMono.flatMap(e -> Node.getInstance().getInode(esMeta.bucketName, esMeta.getInode())
                        .map(i -> {
                            esMeta.versionNum = !InodeUtils.isError(i) ? i.getVersionNum() : VersionUtil.getVersionNum();
                            return esMeta;
                        }));
            } else {
                esMeta.versionNum = VersionUtil.getVersionNum();
            }
        }

        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(esMeta.bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(esMeta.bucketName);
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
        return pMono.flatMap(g -> storagePool.mapToNodeInfo(bucketVnode))
                .flatMap(nodeList -> {
                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(tuple -> new SocketReqMsg("", 0)
                                    .put("key", esMeta.rocksKey(options.needCheck, false, options.inodeRecord, options.linkRecord))
                                    .put("value", Json.encode(esMeta))
                                    .put("lun", MSRocksDB.getRabbitmqRecordLun(tuple.var2))
                                    .put("poolQueueTag", poolQueueTag)
                                    .put("isReName", options.rename ? "1" : "0")
                                    .put("linkObjNames", options.linkObjNames)
                                    .put("removeLink", options.removeLink ? "1" : "0")
                                    .put("addLink", options.addLink ? "1" : "0")
                                    .put("stamp", String.valueOf(System.currentTimeMillis()))
                                    .put("inodeStamp",StringUtils.isNotBlank(options.inodeStamp) ? options.inodeStamp : ""))
                            .collect(Collectors.toList());
                    return putEsMeta(storagePool, msgs, PUT_ES_META, nodeList);
                });
    }

    private static Mono<Boolean> putEsMeta(StoragePool storagePool, List<SocketReqMsg> msgs, ErasureServer.PayloadMetaType type,
                                           List<Tuple3<String, String, String>> nodeList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, type, String.class, nodeList);
        responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum < storagePool.getK()) {
                if (responseInfo.successNum == 0) {
                    log.error("esMeta is {} ,put es meta fail! ", msgs.get(0).get("value"));
                }
                res.onNext(false);
                return;
            }
            res.onNext(true);
        });
        return res;
    }

    public static void bulkEsMeta(List<EsMeta> esMetaList, List<EsMeta> checkList, Map<String, List<EsMeta>> delMap, String lun, String currPrefix) {
        EsMetaTaskScanner.ScanInfo scanInfo = scanInfoMap.get(lun);
        if (esMetaList.isEmpty() && delMap.isEmpty()) {
            scanInfo.processor.onNext(new Tuple2<>(scanInfo.getVnode(true), ROCKS_ES_KEY));
            return;
        }
        boolean needBulk = !esMetaList.isEmpty();
        if (!needBulk) {
            deleteEsMetas(new ArrayList<>(), delMap, lun, true, false, currPrefix);
        } else {
            indexController.asyncBulkPut(INDEX, TYPE, esMetaList, checkList, lun, 0, currPrefix, delMap);
        }
    }

    public static JSONObject beforeBulk(EsMeta esMeta) {
        JSONObject esTask = new JSONObject();
        try {
            final String objName = esMeta.getObjName();
            final String bucketName = esMeta.getBucketName();
            final String versionId = esMeta.versionId;
            esTask.put("bucketName", bucketName);
            esTask.put("path", objName);
            esTask.put("versionId", versionId);
            if (esMeta.toDelete || esMeta.deleteSource) {
                return esTask;
            }
            long stamp = Long.parseLong(esMeta.getStamp());
            final JsonObject sysMeta = new JsonObject(esMeta.getSysMetaData());
            final JsonObject userMeta = new JsonObject(esMeta.getUserMetaData());
            esTask.put("userId", esMeta.userId);
            esTask.put("size", Long.valueOf(esMeta.objSize));
            esTask.put("last-modify", MsDateUtils.stampToISO8601(stamp));
            esTask.put("date", stamp);
            esTask.put("eTag", sysMeta.getValue("ETag"));
            esTask.put("inode", esMeta.inode);
            //带文件夹对象处理
            if (objName.contains("/")) {
                String[] str = objName.split("/");
                String objectName = str[str.length - 1];
                esTask.put("obj_name", objectName);
            } else {
                esTask.put("obj_name", objName);
            }
            //系统元数据处理
            Optional.ofNullable(sysMeta.getValue("Cache-Control")).ifPresent(cache -> esTask.put("cache-Control", cache));
            Optional.ofNullable(sysMeta.getValue("Content-Encoding")).ifPresent(encoding -> esTask.put("content-Encoding", encoding));
            Optional.ofNullable(sysMeta.getValue("Expires")).ifPresent(expires -> esTask.put("expires", expires));
            Optional.ofNullable(sysMeta.getValue("Content-Disposition")).ifPresent(disposition -> esTask.put("content-Disposition", disposition));
            Optional.ofNullable(sysMeta.getValue("Content-Type")).ifPresent(type -> esTask.put("content-Type", type));
            //用户元数据处理
            userMeta.forEach(key -> {
                String keyName = key.getKey();
                if (keyName.toLowerCase().startsWith("x-amz-meta-int-") && isValidLong(key.getValue().toString())) {
                    esTask.put(keyName, Long.parseLong(key.getValue().toString()));
                } else if (!keyName.toLowerCase().startsWith("x-amz-meta-int-") && keyName.toLowerCase().startsWith("x-amz-meta-")) {
                    esTask.put(keyName, key.getValue().toString());
                }
            });
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        }
        return esTask;
    }

    public static Mono<Boolean> delEsMeta(List<String> esMetaList, String bucketName, String lun, boolean retry) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName);
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> {
                    String curIP = ServerConfig.getInstance().getHeartIp1();
                    boolean isIp = false;

                    for (Tuple3<String, String, String> t : nodeList) {
                        if (t.var1.equals(curIP)) {
                            isIp = true;
                            break;
                        }
                    }
                    if (!isIp && !retry) {
                        nodeList.add(new Tuple3<>(curIP, lun, ""));
                    }
                    boolean finalIsIp = isIp;
                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(tuple -> new SocketReqMsg("", 0)
                                    .put("key", esMetaList.get(0))
                                    .put("value", Json.encode(esMetaList))
                                    .put("bucket", bucketName)
                                    .put("lun", !finalIsIp && !retry ? tuple.var2 : MSRocksDB.getRabbitmqRecordLun(tuple.var2))
                                    .put("poolQueueTag", poolQueueTag))
                            .collect(Collectors.toList());
                    return delEsMeta(storagePool, msgs, DEL_ES_META, ERROR_DEL_ES_META, nodeList, esMetaList.get(0));
                });
    }

    private static Mono<Boolean> delEsMeta(StoragePool storagePool, List<SocketReqMsg> msgs, ErasureServer.PayloadMetaType type,
                                           ErrorConstant.ECErrorType errorType, List<Tuple3<String, String, String>> nodeList, String esMeta) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, type, String.class, nodeList);
        responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            SocketReqMsg msg = msgs.get(0);
            if (responseInfo.successNum < storagePool.getK()) {
                //多个节点写入失败，放入消息队列中
                publishEcError(responseInfo.res, nodeList, msg, errorType);
                res.onNext(false);
                if (responseInfo.successNum == 0) {
                    log.error("esMeta is {} ,del es meta fail! ", esMeta);
                }
                return;
            }
            if (responseInfo.successNum != nodeList.size()) {
                //只要写不成功，均写入消息队列
                publishEcError(responseInfo.res, nodeList, msg, errorType);
            }
            res.onNext(true);
        });
        return res;
    }

    public static void deleteEsMetas(List<EsMeta> esMetaList, Map<String, List<EsMeta>> delMap, String lun, boolean flag, boolean needCheck, String currPrefix) {
        try {
            List<Tuple2<String, String>> delTupleList = new ArrayList<>();
            for (Map.Entry<String, List<EsMeta>> entry : delMap.entrySet()) {
                List<Tuple2<String, String>> tuple2s = entry.getValue().stream().filter(Objects::nonNull).map(esMeta -> {
                    boolean inodeMeta = INODE_SUFFIX.equals(entry.getKey());
                    boolean linkMeta = LINK_SUFFIX.equals(entry.getKey());
                    String key = esMeta.rocksKey(CHECK_SUFFIX.equals(entry.getKey()), ROCKS_ES_KEY.equals(currPrefix), inodeMeta, linkMeta);
                    if (inodeMeta || linkMeta) {
                        key = SPLIT_FLAG + key + SPLIT_FLAG + esMeta.versionNum + SPLIT_FLAG + esMeta.stamp;
                    }
                    return new Tuple2<>(key, esMeta.bucketName);
                }).collect(Collectors.toList());
                delTupleList.addAll(tuple2s);
            }
            List<Tuple2<String, String>> tuple2s = esMetaList.stream().map(esMeta -> new Tuple2<>(esMeta.rocksKey(needCheck, ROCKS_ES_KEY.equals(currPrefix),
                    false, false), esMeta.bucketName)).collect(Collectors.toList());
            delTupleList.addAll(tuple2s);
            Flux.fromIterable(delTupleList).groupBy(delTuple -> delTuple.var2)
                    .flatMap(groupTuple -> groupTuple.map(tuple -> tuple.var1).collectList()
                            .map(delList -> new Entry<>(groupTuple.key(), delList)))
                    .flatMap(esList -> delEsMeta(esList.getValue(), esList.getKey(), lun, false))
                    .doFinally(f -> {
                        if (flag) {
                            EsMetaTaskScanner.ScanInfo scanInfo = scanInfoMap.get(lun);
                            scanInfo.setRetry(false);
                            //调低阈值防止频繁从redis获取桶
                            boolean nextBucket = delTupleList.size() < SWITCH_BUCKET_KEYS;
                            if (scanInfo.needStop.get() && !nextBucket) {
                                scanInfo.needStop.set(false);
                            }
                            scanInfo.processor.onNext(new Tuple2<>(scanInfo.getVnode(nextBucket), ROCKS_ES_KEY));
                        }
                    }).subscribe();
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        }
    }

    public static Mono<Boolean> delEsMetaAsync(EsMeta esMeta) {
        esMeta.toDelete = true;
        esMeta.stamp = String.valueOf(System.currentTimeMillis());
        return putEsMeta(esMeta, false);
    }

    public static Mono<Boolean> delEsMetaAsync(EsMeta esMeta, boolean rename) {
        esMeta.toDelete = true;
        EsMetaOptions options = new EsMetaOptions().setRename(rename);
        return putEsMeta(esMeta, options);
    }

    public static Mono<Boolean> putOrDelS3EsMeta(String userId, String bucketName, String objName, String versionId,
                                                 long size, MsHttpRequest request, boolean needCheck, boolean deleteSource) {
        MonoProcessor<Boolean> esRes = MonoProcessor.create();
        EsMeta esMeta = new EsMeta()
                .setUserId(userId)
                .setBucketName(bucketName)
                .setObjName(objName)
                .setVersionId(versionId)
                .setObjSize(String.valueOf(size))
                .setToDelete(!deleteSource)
                .setDeleteSource(deleteSource);
        if (needCheck || deleteSource) {
            return EsMetaTask.putEsMeta(esMeta, needCheck);
        }
        Disposable subscribe = Mono.just(1).publishOn(DISK_SCHEDULER).subscribe(l -> {
            try {
                EsMetaTask.delEsMetaSync(esMeta, new boolean[]{false});
                esRes.onNext(true);
            } catch (Exception e) {
                esRes.onError(e);
            }
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return esRes;
    }

    public static void delEsMetaSync(EsMeta esMeta, boolean[] needAsync) {
        JsonObject esTask = new JsonObject();
        esTask.put("path", esMeta.getObjName());
        esTask.put("bucketName", esMeta.getBucketName());
        esTask.put("versionId", esMeta.versionId);
        try {
            IndexController.getInstance().delete("moss", "userid", esTask, needAsync);
        } catch (Exception e) {
            log.error("{},del esMeta fail ! esMeta is {}", e.getMessage(), esMeta);
            delEsMetaAsync(esMeta).subscribe();
        }
    }

    public static Mono<Boolean> putEsMeta(Inode inode) {
        if (inode.getLinkN() > 1) {
            return dealLinkInode(inode, NOT_FOUND_ES_META, new EsMetaOptions()).map(b -> true);
        } else {
            return getEsMeta(inode).flatMap(tuple -> {
                if (EsMeta.NOT_FOUND_ES_META.equals(tuple.var1)) {
                    return Mono.just(true);
                }
                return putEsMeta(tuple.var1, tuple.var2).map(f -> true);
            });
        }
    }

    public static Mono<Boolean> dealLinkInode(Inode inode, EsMeta esMeta, EsMetaOptions esOptions) {
        return Mono.just(NOT_FOUND_ES_META.equals(esMeta)).flatMap(b -> b ?
                        getEsMeta(inode) : Mono.just(new Tuple2<>(esMeta, esOptions.needCheck)))
                .flatMap(tuple -> {
                    EsMeta linkMeta = mapLinkMeta(inode);
                    Mono<Tuple2<EsMeta, Boolean>> tupleMono = Mono.just(tuple);
                    //删除后obj为删除对象
                    if (EsMeta.NOT_FOUND_ES_META.equals(tuple.var1) && !esOptions.addLink) {
                        tupleMono = Mono.just(true).flatMap(b -> chooseEsMeta(inode));
                    }
                    return tupleMono.flatMap(tuple2 -> EsMeta.NOT_FOUND_ES_META.equals(tuple2.var1) ?
                            Mono.just(true) : getLinkMeta(inode, tuple2.var1, tuple2.var2, esOptions.addLink)
                            .flatMap(linkTuple -> {
                                Boolean needUpdateLinkMeta = linkTuple.var2;
                                List<Tuple2<EsMeta, Boolean>> esMetasTuple = linkTuple.var1;
                                String[] objs;
                                if (esOptions.linkObj.length > 0) {
                                    objs = esOptions.linkObj;
                                } else {
                                    objs = esMetasTuple.stream().filter(e -> e.var1 != null).map(m -> m.var1.objName).toArray(String[]::new);
                                }
                                String objNames = Json.encode(objs);
                                linkMeta.setObjName(objNames);
                                if (esOptions.addLink) {
                                    tuple2.var1.setVersionNum(tuple2.var1.versionNum + ADD_LINK);
                                    EsMetaOptions options = new EsMetaOptions().setNeedCheck(tuple2.var2).setAddLink(true).setLinkObjNames(objNames);
                                    return putEsMeta(tuple2.var1, options);
                                } else {
                                    linkMeta.setStamp(String.valueOf(inode.getMtime() * 1000 + inode.getMtimensec() / 1_000_000));
                                    return Flux.fromIterable(esMetasTuple)
                                            .index()
                                            .flatMap(esMetaTuple -> {
                                                Long index = esMetaTuple.getT1();
                                                Tuple2<EsMeta, Boolean> t2 = esMetaTuple.getT2();
                                                EsMetaOptions options = new EsMetaOptions().setNeedCheck(t2.var2);
                                                if (index == 0 && needUpdateLinkMeta) {
                                                    options.setLinkObjNames(objNames);
                                                    return putEsMeta(t2.var1, options);
                                                }
                                                return putEsMeta(t2.var1, options);
                                            }).collectList()
                                            .flatMap(b -> putEsMeta(linkMeta, new EsMetaOptions().setLinkRecord(true)
                                                    .setInodeStamp(String.valueOf(inode.getMtime() * 1000 + inode.getMtimensec() / 1_000_000))))
                                            .map(f -> true);
                                }
                            }));

                });
    }

    public static Mono<Tuple2<EsMeta, Boolean>> chooseEsMeta(Inode inode) {
        EsMeta linkMeta = mapLinkMeta(inode);
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(inode.getBucket());
        return getEsMeta(linkMeta, false, true).flatMap(res -> {
            EsMeta meta = res.var1;
            int maxCount = Math.min(inode.getLinkN() + 1, MAX_SEARCH_COUNT);
            //查一遍es防止后续getMeta时为error meta
            EsMeta esMeta = inodeMapEsMeta(inode);
            //返回除inode obj之外的所有esMeta
            List<EsMeta> esMetaList = search(inode, esMeta, maxCount, true, true);
            if (NOT_FOUND_ES_META.equals(meta) || ERROR_ES_META.equals(meta)) {
                return Mono.just(esMetaList);
            } else {
                String[] objNames = Json.decodeValue(meta.objName, String[].class);
                List<EsMeta> esList = IntStream.range(0, objNames.length)
                        .mapToObj(i -> i == 0 ? inodeMapEsMeta(inode).setObjName(objNames[i]) : new EsMeta().setObjName(objNames[i]))
                        .collect(Collectors.toList());
                return Mono.just(esList);
            }
        }).flatMap(esMetaList -> Flux.fromIterable(esMetaList)
                .flatMap(esMeta -> storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(inode.getBucket(), esMeta.objName))
                        .flatMap(nodeList -> ErasureClient.getObjectMetaVersionUnlimitedNotRecover(inode.getBucket(), esMeta.objName, inode.getVersionId(), nodeList, null, null, null))
                        .flatMap(metaData -> {
                            if (metaData.isAvailable() && metaData.inode == inode.getNodeId()) {
                                return Mono.just(new Tuple2<>(inodeMetaMapEsMeta(metaData, inode), false));
                            } else {
                                return Mono.empty();
                            }
                        }))
                .next()
                .switchIfEmpty(Mono.just(new Tuple2<>(esMetaList.isEmpty() ? NOT_FOUND_ES_META : esMetaList.get(0),
                        !esMetaList.isEmpty() && StringUtils.isBlank(esMetaList.get(0).sysMetaData)))));
    }

    public static Mono<Tuple2<List<Tuple2<EsMeta, Boolean>>, Boolean>> getLinkMeta(Inode inode, EsMeta currEsMeta, boolean needCheck, boolean addLink) {
        EsMeta linkMeta = mapLinkMeta(inode);
        return getEsMeta(linkMeta, false, true).flatMap(res -> {
            EsMeta meta = res.var1;
            int maxCount = Math.min(inode.getLinkN() + 1, MAX_SEARCH_COUNT);
            if (NOT_FOUND_ES_META.equals(meta) || ERROR_ES_META.equals(meta)) {
                List<EsMeta> search = search(inode, currEsMeta, maxCount, needCheck, false);
                List<Tuple2<EsMeta, Boolean>> result = search.stream().map(esMeta -> new Tuple2<>(esMeta, false)).collect(Collectors.toList());
                return Mono.just(new Tuple2<>(result, true));
            } else {
                String[] objNames = Json.decodeValue(meta.objName, String[].class);
                if (!addLink) {
                    StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(inode.getBucket());
                    boolean[] exist = new boolean[]{false};
                    return Flux.fromArray(objNames)
                            .doOnNext(objName -> {
                                if (objName.equals(currEsMeta.getObjName())) {
                                    exist[0] = true;
                                }
                            })
                            .flatMap(objName -> storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(inode.getBucket(), objName))
                                    .flatMap(nodeList -> ErasureClient.getObjectMetaVersionUnlimitedNotRecover(inode.getBucket(), objName, inode.getVersionId(), nodeList, null, null, null))
                                    .flatMap(metaData -> {
                                        if (NOT_FOUND_META.equals(metaData) || metaData.deleteMark || metaData.deleteMarker) {
                                            return Mono.empty();
                                        }
                                        if (ERROR_META.equals(metaData)) {
                                            return Mono.just(new Tuple2<>(inodeMapEsMeta(inode).setObjName(objName), true));
                                        }
                                        if (inode.getNodeId() == metaData.inode) {
                                            return Mono.just(new Tuple2<>(inodeMetaMapEsMeta(metaData, inode), false));
                                        }
                                        return Mono.empty();
                                    })).collectList()
                            .map(l -> {
                                if (!exist[0]) {
                                    l.add(new Tuple2<>(currEsMeta, false));
                                }
                                return new Tuple2<>(l, false);
                            });
                }
                List<Tuple2<EsMeta, Boolean>> esMetaList = Arrays.stream(objNames)
                        .filter(objName -> !objName.equals(currEsMeta.getObjName()))
                        .map(objName -> new Tuple2<>(new EsMeta().setObjName(objName), false)).collect(Collectors.toList());
                esMetaList.add(new Tuple2<>(currEsMeta, false));
                return Mono.just(new Tuple2<>(esMetaList, false));
            }
        });
    }

    public static Mono<Boolean> delEsMeta(Inode inode) {
        EsMetaOptions options = new EsMetaOptions().setAsync(true).setUpdate(true);
        return delEsMeta(inode, options);
    }

    public static Mono<Boolean> delEsMeta(Inode inode, String bucket, String objName, long nodeId) {
        Inode cInode = inode.clone().setObjName(objName);
        Inode res = cInode.getLinkN() >= 1 ? cInode : EsMeta.delInode(bucket, objName, "null", nodeId);
        EsMetaOptions options = new EsMetaOptions().setAsync(true).setUpdate(cInode.getLinkN() >= 1);
        return delEsMeta(res, options);
    }

    public static Mono<Boolean> delEsMeta(Inode resInode, MetaData metaData, boolean deleteSource) {
        Inode res = resInode.getLinkN() >= 1 ? resInode : EsMeta.metaMapDelInode(metaData);
        EsMetaOptions options = new EsMetaOptions().setAsync(true).setUpdate(resInode.getLinkN() >= 1).setDeleteSource(deleteSource);
        return delEsMeta(res, options);
    }

    public static Mono<Boolean> delEsMeta(Inode resInode, Inode oldInode, String bucket, String objName, long nodeId, boolean async) {
        Inode res = resInode.getLinkN() >= 1 ? resInode : oldInode.getLinkN() >= 1 ? oldInode : delInode(bucket, objName, "null", nodeId);
        EsMetaOptions options = new EsMetaOptions().setAsync(async).setUpdate(resInode.getLinkN() >= 1);
        return delEsMeta(res, options);
    }

    public static Mono<Boolean> delEsMeta(Inode inode, EsMetaOptions delOptions) {
        delOptions.async = delOptions.async && delOptions.update;
        EsMeta esMeta = inodeMapEsMeta(inode);
        if (delOptions.rename) {
            esMeta.setVersionNum(inode.getVersionNum() + "0");
        }
        MonoProcessor<Boolean> esRes = MonoProcessor.create();
        boolean[] needAsync = new boolean[]{false};
        if (delOptions.deleteSource) {
            delOptions.async = false;
            esMeta.deleteSource = true;
            esRes.onNext(true);
        } else {
            Mono.just(true).publishOn(DISK_SCHEDULER).subscribe(l -> {
                try {
                    EsMetaTask.delEsMetaSync(esMeta, needAsync);
                    esRes.onNext(true);
                } catch (Exception e) {
                    esRes.onNext(false);
                }
            });
        }
        boolean finalAsync = delOptions.async;
        return esRes.flatMap(v -> esMeta.deleteSource ? EsMetaTask.putEsMeta(esMeta, false) : Mono.just(true))
                .flatMap(f -> {
                    EsMeta linkMeta = mapLinkMeta(inode);
                    EsMeta inodeMeta = mapInodeMeta(inode);
                    Mono<Boolean> needUpdate = Mono.just(false);
                    if (inode.getLinkN() == 1 && delOptions.update) {
                        needUpdate = Node.getInstance().getInode(inode.getBucket(), inode.getNodeId())
                                .map(i -> !InodeUtils.isError(i));
                    } else if (inode.getLinkN() > 1) {
                        needUpdate = Mono.just(true);
                    }
                    EsMetaOptions putOptions = new EsMetaOptions();
                    return needUpdate.flatMap(b -> {
                                if (delOptions.update) {
                                    if (b) {
                                        return getEsMeta(linkMeta, false, true).flatMap(res -> {
                                            EsMeta meta = res.var1;
                                            if (NOT_FOUND_ES_META.equals(meta) || ERROR_ES_META.equals(meta)) {
                                                putOptions.setInodeRecord(true);
                                                return putEsMeta(inodeMeta, putOptions);
                                            }
                                            String[] objs = new String[]{esMeta.objName};
                                            String objStr = Json.encode(objs);
                                            linkMeta.setObjName(objStr);
                                            putOptions.setRemoveLink(true).setLinkRecord(true)
                                                    .setInodeStamp(String.valueOf(inode.getMtime() * 1000 + inode.getMtimensec() / 1_000_000));
                                            return putEsMeta(linkMeta, putOptions);
                                        });
                                    }
                                    putOptions.setInodeRecord(true);
                                    return putEsMeta(inodeMeta, putOptions);
                                }
                                return Mono.just(true);
                                //再删一次
                            })
                            .flatMap(b -> finalAsync && needAsync[0] ? delEsMetaAsync(esMeta, delOptions.rename) : Mono.just(true));
                }).map(b -> true);
    }

    public static Mono<Tuple2<EsMeta, Tuple2<ErasureServer.PayloadMetaType, EsMeta>[]>> getEsMeta(EsMeta esMeta, boolean inodeRecord, boolean linkRecord) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(esMeta.bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(esMeta.bucketName);
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ECUtils.getRocksKey(storagePool, esMeta.rocksKey(false, false, inodeRecord, linkRecord)
                        , EsMeta.class, GET_ES_META, NOT_FOUND_ES_META, ERROR_ES_META
                        , null, EsMeta::getVersionNum, Comparator.comparing(a -> a.versionNum),
                        (a, b, c, d) -> Mono.just(1), nodeList, null));
    }

    public static Mono<Boolean> mvEsMeta(Inode newInode, String bucket, String oldObj, String newObj, long nodeId) {
        Inode oldInode = newInode.clone().setObjName(oldObj);
        boolean deleteAsync = true;
        if (newInode.getLinkN() < 1) {
            newInode = EsMeta.delInode(bucket, newObj, "null", nodeId);
            oldInode = EsMeta.delInode(bucket, oldObj, "null", nodeId);
            //没有versionNum删除不异步
            deleteAsync = false;
        }
        Inode finalOldInode = oldInode;
        boolean finalDeleteAsync = deleteAsync;
        EsMetaOptions delOptions = new EsMetaOptions().setAsync(finalDeleteAsync).setUpdate(finalDeleteAsync).setRename(true);
        if (newInode.getLinkN() > 1) {
            EsMetaOptions options = new EsMetaOptions().setAddLink(true);
            return dealLinkInode(newInode, NOT_FOUND_ES_META, options)
                    .flatMap(b -> delEsMeta(finalOldInode, delOptions)).map(b -> true);
        }
        return putEsMeta(newInode).flatMap(b -> delEsMeta(finalOldInode, delOptions)).map(b -> true);
    }

    //inode创建第一个硬链接(防止s3转换成inode后es没有inode)
    public static Mono<Boolean> putLinkEsMeta(Inode newInode, Inode oldInode) {
        if (newInode.getLinkN() > 1) {
            EsMetaOptions options = new EsMetaOptions().setAddLink(true);
            return dealLinkInode(newInode, NOT_FOUND_ES_META, options)
                    .flatMap(b -> {
                        if (newInode.getLinkN() == 2) {
                            EsMetaOptions options0 = new EsMetaOptions().setAddLink(true);
                            String[] linkObj = new String[]{newInode.getObjName(), oldInode.getObjName()};
                            options0.setLinkObj(linkObj);
                            return dealLinkInode(oldInode, NOT_FOUND_ES_META, options0);
                        }
                        return Mono.just(true);
                    });
        }
        return Mono.just(true);
    }


    public static Mono<Boolean> overWriteEsMeta(EsMeta esMeta, Inode inode, boolean needCheck) {
        Mono<Inode> inodeMono = Mono.just(inode);
        if (InodeUtils.isError(inode)) {
            inodeMono = Node.getInstance().getInode(esMeta.bucketName, esMeta.getInode())
                    .doOnNext(i -> {
                        if (!InodeUtils.isError(i)) {
                            esMeta.setVersionNum(i.getVersionNum()).setObjSize(String.valueOf(i.getSize()));
                        } else {
                            esMeta.setVersionNum(VersionUtil.getVersionNum());
                        }
                    });
        } else {
            esMeta.versionNum = inode.getVersionNum();
        }
        if (inode.getLinkN() > 1) {
            EsMetaOptions options = new EsMetaOptions().setNeedCheck(needCheck).setAddLink(needCheck);
            return inodeMono.flatMap(i -> dealLinkInode(i, esMeta, options));
        } else {
            return inodeMono.flatMap(i -> putEsMeta(esMeta, needCheck));
        }
    }

    public static Mono<Tuple2<EsMeta, Boolean>> getEsMeta(Inode inode) {
        return Mono.just(StoragePoolFactory.getMetaStoragePool(inode.getBucket()))
                .flatMap(bucketPool -> bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(inode.getBucket(), inode.getObjName())))
                .flatMap(nodeList -> ErasureClient.getObjectMetaVersionUnlimitedNotRecover(inode.getBucket(), inode.getObjName(), inode.getVersionId(), nodeList, null, null, null))
                .flatMap(metaData -> {
                    EsMeta esMeta = inodeMapEsMeta(inode);

                    if (NOT_FOUND_META.equals(metaData) || metaData.deleteMark) {
                        return Mono.just(new Tuple2<>(EsMeta.NOT_FOUND_ES_META, false));
                    } else if (ERROR_META.equals(metaData)) {
                        return Mono.just(new Tuple2<>(esMeta, true));
                    }
                    JsonObject sysMeta = new JsonObject(metaData.sysMetaData);
                    String userId = sysMeta.getString("owner");
                    esMeta.setUserId(userId)
                            .setSysMetaData(metaData.sysMetaData)
                            .setUserMetaData(metaData.userMetaData);

                    return Mono.just(new Tuple2<>(esMeta, false));
                });
    }

    /**
     * 根据inode从es中查所有的esMeta,用于硬链接以及getMeta为error时
     */
    public static List<EsMeta> search(Inode inode, EsMeta esMeta, int maxCount, boolean needCheck, boolean choose) {
        List<String> results;
        int start = 0;
        boolean first = true;
        List<EsMeta> esMetaList = new ArrayList<>();
        do {
            Map<String, String> queryMap = new HashMap<>();
            queryMap.put(ES_BUCKET, inode.getBucket());
            queryMap.put(ES_NUMBER, String.valueOf(start));
            queryMap.put(ES_MAX_NUMBER, String.valueOf(maxCount));
            queryMap.put(ES_LINK_TYPE, ES_TYPE_AND);
            queryMap.put(ES_INODE, String.valueOf(inode.getNodeId()));
            results = indexController.searchEs(queryMap, false);
            for (String result : results) {
                JsonObject obj = new JsonObject(result);
                if (!esMeta.objName.equals(obj.getString("path"))) {
                    EsMeta convertEsMeta = resMapToEsMeta(obj, inode, esMeta, needCheck);
                    esMetaList.add(convertEsMeta);
                }
                if (first && !choose) {
                    esMetaList.add(esMeta);
                    first = false;
                }
            }

            start += results.size();
        } while (results.size() == maxCount);
        return esMetaList;
    }

    public static EsMeta resMapToEsMeta(JsonObject obj, Inode inode, EsMeta esMeta, boolean needCheck) {
        JsonObject userMetaJson = new JsonObject();
        JsonObject sysMetaJson = new JsonObject();
        for (String str : obj.fieldNames()) {
            if (str.startsWith(USER_META)) {
                userMetaJson.put(str, obj.getValue(str));
            }
            if ("cache-Control".equals(str)) {
                sysMetaJson.put("Cache-Control", obj.getValue(str));
            }
            if ("content-Encoding".equals(str)) {
                sysMetaJson.put("Content-Encoding", obj.getValue(str));
            }
            if ("expires".equals(str)) {
                sysMetaJson.put("Expires", obj.getValue(str));
            }
            if ("content-Disposition".equals(str)) {
                sysMetaJson.put("Content-Disposition", obj.getValue(str));
            }
            if ("content-Type".equals(str)) {
                sysMetaJson.put("Content-Type", obj.getValue(str));
            }
        }

        EsMeta linkEsMeta = new EsMeta()
                .setInode(inode.getNodeId())
                .setObjSize(String.valueOf(inode.getSize()))
                .setBucketName(inode.getBucket())
                .setUserId(obj.getString("userId"))
                .setVersionId(obj.getString("versionId"))
                .setObjName(obj.getString("path"))
                .setVersionNum(inode.getVersionNum())
                .setStamp(String.valueOf(inode.getMtime() * 1000 + inode.getMtimensec() / 1_000_000))
                .setDeleteSource(obj.getBoolean(ES_DELETE_SOURCE) != null && obj.getBoolean(ES_DELETE_SOURCE));
        if (!needCheck) {
            JsonObject newSysMeta = new JsonObject(esMeta.sysMetaData);
            sysMetaJson.put(ETAG, newSysMeta.getValue(ETAG));
            String userMeta = userMetaJson.encode();
            String sysMeta = sysMetaJson.encode();
            linkEsMeta.setUserMetaData(userMeta)
                    .setSysMetaData(sysMeta);
        }
        return linkEsMeta;
    }

    public static void isExit() {
        try {
            IndexController.getInstance().isLink();
        } catch (Exception e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "es connection error, " + e.getMessage());
        }
    }

    private static boolean isValidLong(String str) {
        try {
            Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Accessors(chain = true)
    @Data
    public static class EsMetaOptions {
        private boolean needCheck = false;
        private boolean linkRecord = false;
        private boolean inodeRecord = false;
        private boolean rename = false;
        private boolean removeLink = false;
        private String linkObjNames = "";
        private boolean addLink = false;
        private String[] linkObj = new String[0];
        private boolean async = false;
        private boolean update = false;
        private boolean deleteSource = false;
        private String inodeStamp = "";
    }
}
