package com.macrosan.filesystem;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.*;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.utils.ChunkFileUtils;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.ChunkFile;
import com.macrosan.message.jsonmsg.EsMeta;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.channel.Channel;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.macrosan.utils.params.DelMarkParams;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.Utils.DEFAULT_META_HASH;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.filesystem.nfs.call.ReNameCall.RENAME_NOT_FOUND;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.message.jsonmsg.ChunkFile.ERROR_CHUNK;
import static com.macrosan.message.jsonmsg.Inode.*;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rabbitmq.RabbitMqUtils.getDiskName;
import static com.macrosan.storage.move.CacheMove.isEnableCacheOrderFlush;

@Log4j2
public class FsUtils {
    public static MsExecutor fsExecutor = new MsExecutor(48, 8, new MsThreadFactory("filesystem"));

    public static final byte[] EMPTY_BYTE = new byte[0];

    public static Mono<Boolean> putObj(StoragePool pool, Encoder encoder, String fileName,
                                       List<Tuple3<String, String, String>> nodeList, Inode inode, long curOffset, MonoProcessor<Boolean> rollBackProcessor) {

        String bucketVnode = pool.getBucketVnodeId(inode.getBucket());
        // versionNum 必须为"null字符串"，null时metakey=*22490/test/2.txt\u0000null，非null时22490/test/2.txt
        String[] metaKey = new String[1];
        if (pool.getVnodePrefix().startsWith("cache")) {
            StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(inode.getBucket());
            String metaVnode = metaPool.getBucketVnodeId(inode.getBucket());
            metaKey[0] = Inode.getKey(metaVnode, inode.getBucket(), inode.getNodeId());
        } else {
            metaKey[0] = Utils.getVersionMetaDataKey(bucketVnode, inode.getBucket(), inode.getObjName(), inode.getVersionId() == null ? "null" : inode.getVersionId());
        }

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("noGet", "1")
                .put("fileName", fileName)
                .put("compression", pool.getCompression())
                .put("metaKey", metaKey[0])
                .put("fileOffset", String.valueOf(curOffset));

        // 判断是否开启缓冲池 按序下刷
        if (isEnableCacheOrderFlush(pool)) {
            long timeMs =  inode.getCreateTime() * 1000L;
            msg.put("flushStamp", String.valueOf(timeMs));
        }

        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> msg.copy()
                        .put("lun", tuple.var2)
                        .put("vnode", tuple.var3))
                .collect(Collectors.toList());

        List<UnicastProcessor<Payload>> publisher = nodeList.stream()
                .map(t -> UnicastProcessor.<Payload>create())
                .collect(Collectors.toList());

        boolean[] first = new boolean[publisher.size()];
        Arrays.fill(first, true);

        UnicastProcessor<byte[]>[] dataFlux = encoder.data();
        for (int i = 0; i < publisher.size(); i++) {
            int index = i;
            dataFlux[index].subscribe(bytes -> {
                        if (first[index]) {
                            if (bytes.length < pool.getPackageSize()) {
                                Payload payload;
                                if (CURRENT_IP.equals(nodeList.get(index).var1)) {
                                    payload = new LocalPayload<>(PUT_OBJECT_ONCE, new Tuple2<>(msgs.get(index), bytes));
                                } else {
                                    byte[] socketBytes = msgs.get(index).toBytes().array();
                                    payload = DefaultPayload.create(Unpooled.wrappedBuffer(socketBytes, bytes), Unpooled.wrappedBuffer(PUT_OBJECT_ONCE.name().getBytes()));
                                }

                                publisher.get(index).onNext(payload);
                                publisher.get(index).onComplete();
                                log.debug("PUT_OBJECT_ONCE: {}, {}", index, first[index]);
                            } else {
                                publisher.get(index).onNext(DefaultPayload.create(Json.encode(msgs.get(index)), START_PUT_OBJECT.name()));
                                publisher.get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                                log.debug("START_PUT_OBJECT and PUT_OBJECT: {}, {}", index, first[index]);
                            }
                            first[index] = false;
                        } else {
                            publisher.get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                            log.debug("PUT_OBJECT: {}, {}", index, first[index]);
                        }
                    },
                    e -> {
                        log.error("", e);
                        publisher.get(index).onNext(DefaultPayload.create("put file error", ERROR.name()));
                        publisher.get(index).onComplete();
                    },
                    () -> {
                        publisher.get(index).onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                        publisher.get(index).onComplete();
                    }
            );
        }

        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, nodeList);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<Integer> errorChunksList = new ArrayList<>(pool.getM());
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());

        responseInfo.responses
                .subscribe(tuple -> {
                    if (tuple.var2.equals(ERROR)) {
                        errorChunksList.add(tuple.var1);
                    }
                }, e -> log.error("", e), () -> {
                    String recoverObj = pool.getVnodePrefix().startsWith("cache") ? metaKey[0] : inode.getObjName();
                    if (responseInfo.successNum == pool.getK() + pool.getM()) {
                        res.onNext(true);
                    } else if (responseInfo.successNum >= pool.getK()) {
                        res.onNext(true);
                        // 缺失数据块在冗余范围内，发布消息修复，未发现stamp在ERROR_PUT_OBJECT_FILE中使用，暂不进行添加
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("errorChunksList", Json.encode(errorChunksList))
                                .put("bucket", inode.getBucket())
                                .put("object", recoverObj)
                                .put("fileName", fileName)
                                .put("versionId", inode.getVersionId())
                                .put("storage", pool.getVnodePrefix())
                                .put("nodeId", String.valueOf(inode.getNodeId()))
                                .put("fileSize", String.valueOf(encoder.size()))
                                .put("poolQueueTag", poolQueueTag)
                                .put("fileOffset", String.valueOf(curOffset));
                        log.debug("【publishFix】 res:{} >>>>>> errorMsg:{}", responseInfo.res, errorMsg);
                        publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_FS_PUT_FILE);
                    } else {
                        res.onNext(false);
                        //响应成功数量达不到k，发布回退消息，删掉成功的节点上的文件
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("bucket", inode.getBucket())
                                .put("object", recoverObj)
                                .put("fileName", fileName)
                                .put("storage", pool.getVnodePrefix())
                                .put("poolQueueTag", poolQueueTag);
                        log.debug("【publishRoll】 res:{} >>>>>> errorMsg:{}", responseInfo.res, errorMsg);
                        ECUtils.publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                    }

                    rollBackProcessor.subscribe(noExceedRedundancy -> {
                        if (noExceedRedundancy) {
                            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                    .put("bucket", inode.getBucket())
                                    .put("object", recoverObj)
                                    .put("fileName", fileName)
                                    .put("storage", pool.getVnodePrefix())
                                    .put("poolQueueTag", poolQueueTag)
                                    .put("lun", getDiskName(nodeList.get(0).var1, nodeList.get(0).var2));
                            ObjectPublisher.publish(CURRENT_IP, errorMsg, ERROR_ROLL_BACK_FILE);
                            log.debug("rollBack ino: {}, obj: {}, fileName: {}", inode.getNodeId(), inode.getObjName(), fileName);
                        }
                    });
                });

        return res;
    }

    public static Mono<Inode> lookup(String bucket, String objName, ReqInfo reqHeader, boolean isReName, long dirInode, List<Inode.ACE> dirACEs) {
        return lookup(bucket, objName, reqHeader, true, isReName, dirInode, dirACEs);
    }

    public static Mono<Inode> lookup(String bucket, String objName, ReqInfo reqHeader, boolean caseSensitive, boolean isReName, long dirInode, List<Inode.ACE> dirACEs) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();
        String finalObj;
        if (objName.endsWith("/")) {
            finalObj = objName.substring(0, objName.length() - 1);
        } else {
            finalObj = objName;
        }
        String key = Utils.getLatestMetaKey(bucketVnode, bucket, finalObj);

        Map<String, String> extraMap = new HashMap<>();
        if (isReName) {
            extraMap.put("isReName", "1");
        }

        //默认区分大小写
        if (!caseSensitive) {
            extraMap.put("caseSensitive", "0");
        }

        extraMap.put("dirInode", String.valueOf(dirInode));

        return BucketSyncSwitchCache.isSyncSwitchOffMono(bucket)
                .map(isSyncSwitchOff -> NOT_FOUND_INODE.clone()
                        .setVersionNum(VersionUtil.getLastVersionNum("", isSyncSwitchOff)))
                .flatMap(deleteMark -> ECUtils.getRocksKey(pool, key, Inode.class, LOOK_UP, NOT_FOUND_INODE, ERROR_INODE, deleteMark,
                        Inode::getVersionNum, Comparator.comparing(a -> a.getVersionNum()), FsUtils::repairInode, nodeList, null, extraMap, null))
                .map(t -> t.var1)
                .flatMap(inode -> {
                    if (!NOT_FOUND_INODE.equals(inode) && !ERROR_INODE.equals(inode) && inode.getNodeId() == 0 && reqHeader != null) {
                        // nfs端覆盖moss同名对象时，先lookup，此时moss对象无inode，会创建inode，因此重新生成cookie
                        if (dirInode == -1) {
                            inode.setReference(inode.getObjName());
                            return Mono.just(inode);
                        } else {
                            String metaHash = inode.getReference();
                            if (DEFAULT_META_HASH.equals(inode.getReference())) {
                                return FsUtils.checkDir(bucket, finalObj)
                                        .flatMap(checkRes -> {
                                            //NotFound
                                            if (checkRes == 0) {
                                                return Mono.just(NOT_FOUND_INODE);
                                            } else if (checkRes == 1) {
                                                //found
                                                return Node.getInstance().createS3Inode(dirInode, bucket, inode.getObjName(), inode.getVersionId(), metaHash, dirACEs);
                                            } else {
                                                return Mono.just(ERROR_INODE);
                                            }
                                        });
                            }
                            return Node.getInstance().createS3Inode(dirInode, bucket, inode.getObjName(), inode.getVersionId(), metaHash, dirACEs);
                        }
                    } else {
                        if (!InodeUtils.isError(inode) && !DEFAULT_META_HASH.equals(inode.getReference())) {
                            return Node.getInstance().getInode(bucket, inode.getNodeId())
                                    .flatMap(i -> {
                                        return i.getLinkN() == NOT_FOUND_INODE.getLinkN() ? Mono.just(NOT_FOUND_INODE) : Mono.just(inode);
                                    });
                        }
                        return Mono.just(inode);
                    }
                });
    }

    public static Mono<Integer> rename(StoragePool pool, SocketReqMsg msg, List<Tuple3<String, String, String>> nodeList, long stamp, int stampNano, long nodeId, String key) {
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> msg.copy()
                        .put("lun", tuple.var2)
                        .put("stamp", String.valueOf(stamp))
                        .put("stampNano", String.valueOf(stampNano)))
                .collect(Collectors.toList());

        MonoProcessor<Integer> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, RENAME, String.class, nodeList);

        AtomicInteger notFoundNum = new AtomicInteger(0);

        Disposable subscribe = responseInfo.responses.subscribe(s -> {
                    if (RENAME_NOT_FOUND.equals(s.var3)) {
                        notFoundNum.incrementAndGet();
                    }
                },
                e -> {
                    log.error("", e);
                    res.onNext(-1);
                }, () -> {
                    if (responseInfo.successNum == 0) {
                        res.onNext(-1);
                    } else {
                        if (responseInfo.successNum == pool.getK() + pool.getM()) {
                            if (notFoundNum.get() > 0) {
                                res.onNext(0);
                            } else {
                                DelDeleteMark.putDeleteKey(key, msg.get("deleteMark"));
                                res.onNext(1);
                            }
                        } else if (responseInfo.successNum >= pool.getK()) {
                            String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
//                            if (StringUtils.isEmpty(poolName)) {
//                                String strategyName = "storage_" + pool.getVnodePrefix();
//                                poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                            }
//                            String poolQueueTag = poolName;

                            // 发布消息进行修复；修复inode与其余元数据
                            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                    .put("bucket", msg.get("bucket"))
                                    .put("nodeId", String.valueOf(nodeId))
                                    .put("oldObjName", msg.get("oldObj"))
                                    .put("key", key)
                                    .put("poolQueueTag", poolQueueTag);
                            log.debug("【publishFix】 rename res:{} >>>>>> errorMsg:{}", responseInfo.res, errorMsg);
                            publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_FS_RENAME);
                            res.onNext(1);
                        } else {
                            //TODO
                            res.onNext(-1);
                        }
                    }
                });

        return res;
    }

    /**
     * 在索引盘中存入Inode记录，用于flush后
     *
     * @param key      索引盘中形如 (vnode/bucket/nodeId 的文件系统记录
     * @param pool     存入key的索引池
     * @param value    inode的Json格式
     * @param type     完成putRocksKey的RSocket消息类型
     * @param nodeList <ip, lunName, vnode>
     **/
    public static Mono<Boolean> putRocksKey(StoragePool pool, String key, String value, PayloadMetaType type, boolean isRecover,
                                            boolean repairCookieAndInode, List<Tuple3<String, String, String>> nodeList, String mda, EsMeta esMeta) {

        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", key)
                        .put("value", value)
                        .put("isRecover", isRecover ? "1" : "0")
                        .put("repairCookieAndInode", repairCookieAndInode ? "1" : "0")
                        .put("lun", tuple.var2))
                .collect(Collectors.toList());

        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, type, String.class, nodeList);

        Disposable subscribe = responseInfo.responses.subscribe(s -> {

                },
                e -> {
                    log.error("", e);
                    res.onNext(false);
                }, () -> {
                    if (responseInfo.successNum == 0) {
                        res.onNext(false);
                    } else {
                        if (responseInfo.successNum == pool.getK() + pool.getM()) {
                            res.onNext(true);
                        } else if (responseInfo.successNum >= pool.getK()) {
                            String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
                            msgs.get(0).put("poolQueueTag", poolQueueTag);
                            ECUtils.publishEcError(responseInfo.res, nodeList, msgs.get(0), ERROR_PUT_INODE);
                            res.onNext(true);
                        } else {
                            if (null != esMeta && ES_ON.equals(mda)) {
                                log.error("es meta minor success  ! esMeta is {}", esMeta);
                                Inode inode = Json.decodeValue(value, Inode.class);
                                Mono.just(1).publishOn(DISK_SCHEDULER).subscribe(l -> EsMetaTask.overWriteEsMeta(esMeta, inode, true).subscribe());
                            }
                            //TODO
                            res.onNext(false);
                        }
                    }
                });

        return res;
    }

    static TypeReference<Tuple2<String, Inode>[]> reference = new TypeReference<Tuple2<String, Inode>[]>() {
    };

    //smb
    public static Mono<List<Inode>> listObject(String bucket, String prefix, String marker, long maxSize, String pattern, int queryClass) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);

        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();
        List<SocketReqMsg> msg = nodeList.stream()
                .map(t -> new SocketReqMsg("", 0)
                        .put("prefix", prefix)
                        .put("bucket", bucket)
                        .put("marker", marker)
                        .put("vnode", bucketVnode)
                        .put("maxSize", String.valueOf(maxSize))
                        .put("pattern", pattern)
                        .put("queryClass", String.valueOf(queryClass))
                        .put("lun", t.var2))
                .collect(Collectors.toList());

        ClientTemplate.ResponseInfo<Tuple2<String, Inode>[]> responseInfo =
                ClientTemplate.oneResponse(msg, LIST_DIR_PLUS, reference, nodeList);

        ListDirPlus clientHandler = new ListDirPlus(pool, responseInfo, nodeList);

        responseInfo.responses
                .subscribe(clientHandler::handleResponse, e -> {
                    log.error("", e);
                    clientHandler.res.onNext(false);
                }, clientHandler::handleComplete);

        return clientHandler.res
                .doOnNext(b -> {
                    if (!b) {
                        log.error("list {}: {} from {} fail.", bucket, prefix, marker);
                    }
                })
                .map(b -> clientHandler.list);
    }

    //【生命周期】检查父目录是否为空
    public static Mono<List<Inode>> listObjectNoRepair(String bucket, String prefix, String marker, long maxSize, String pattern, int queryClass) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);

        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();
        List<SocketReqMsg> msg = nodeList.stream()
                .map(t -> new SocketReqMsg("", 0)
                        .put("prefix", prefix)
                        .put("bucket", bucket)
                        .put("marker", marker)
                        .put("vnode", bucketVnode)
                        .put("maxSize", String.valueOf(maxSize))
                        .put("pattern", pattern)
                        .put("queryClass", String.valueOf(queryClass))
                        .put("lun", t.var2))
                .collect(Collectors.toList());

        ClientTemplate.ResponseInfo<Tuple2<String, Inode>[]> responseInfo =
                ClientTemplate.oneResponse(msg, LIST_DIR_PLUS, reference, nodeList);

        ListDirPlus clientHandler = new ListDirPlus(pool, responseInfo, nodeList);

        responseInfo.responses
                .subscribe(clientHandler::handleResponse, e -> {
                    log.error("", e);
                    clientHandler.res.onNext(false);
                }, clientHandler::handleCompleteNoRepair);

        return clientHandler.res.map(b -> clientHandler.list);
    }

    public static Mono<List<Inode>> listObject(String bucket, String prefix, String marker, long maxSize) {
        return listObject(bucket, prefix, marker, maxSize, "*", -1);
    }

    public static Mono<Boolean> setFsDelMark(Inode inode, List<Tuple3<String, String, String>> nodeList, String[] versionNum, boolean needDeleteInode, AtomicBoolean isFsDelMark) {
        return ErasureClient.getFsMetaVerUnlimited(inode.getBucket(), inode.getObjName(), inode.getVersionId(), nodeList, null, null, null, inode.getXAttrMap().get(QUOTA_KEY))
                .zipWith(MsObjVersionUtils.getObjVersionIdReactive(inode.getBucket()))
                .flatMap(tuple2 -> {
                    MetaData meta = tuple2.getT1();
                    if (meta.isAvailable() && meta.inode == inode.getNodeId()) {
                        //对于s3上传的对象，创建inode元数据之后，再进行删除，不更新用户/组配额的容量已经文件数信息
                        int[] uid = new int[]{inode.getUid()};
                        int[] gid = new int[]{inode.getGid()};
                        return FSQuotaUtils.existQuotaInfoHasScanComplete(inode.getBucket(), meta.key, Long.parseLong(meta.stamp), Collections.singletonList(uid[0]), Collections.singletonList(gid[0]))
                                .map(t2 -> {
                                    if (t2.var1) {
                                        inode.getXAttrMap().put(QUOTA_KEY, Json.encode(t2.var2));
                                    }
                                    return tuple2;
                                });
                    }
                    return Mono.just(tuple2);
                })
                .flatMap(tuple2 -> {
                    MetaData meta = tuple2.getT1();
                    // 将 stamp 从创建时间改成最后一次修改时间 MTime
                    meta.stamp = String.valueOf(inode.getMtime() * 1000L + inode.getMtimensec() / 1_000_000L);
                    if (meta.isAvailable() && meta.inode == inode.getNodeId()) {
                        return BucketSyncSwitchCache.isSyncSwitchOffMono(inode.getBucket())
                                .flatMap(isSync -> Mono.just(VersionUtil.getVersionNumMaybeUpdate(isSync, inode.getNodeId())))
                                .flatMap(newVersionNum -> {
                                    //处理getFsMetaVerUnlimited 恢复流程之后后，versionNum比较小的情况，需再次获取最新的versionNum,记录下用于硬链接的更新
                                    versionNum[0] = newVersionNum;
                                    return ErasureClient
                                            .setDelMark(new DelMarkParams(inode.getBucket(), inode.getObjName(), inode.getVersionId(), meta, nodeList, tuple2.getT2(), newVersionNum)
                                                    .type(ERROR_DEL_INODE_META)
                                                    .needDeleteInode(needDeleteInode)
                                                    .updateQuotaKeyStr(inode.getXAttrMap().get(QUOTA_KEY))
                                                    .fileCookie(meta.cookie)
                                                    .nodeId(meta.inode))
                                            .map(r -> r > 0);
                                });
                    } else {
                        if (meta.deleteMarker || meta.deleteMark || (meta.isAvailable() && meta.inode != inode.getNodeId())) {
                            isFsDelMark.set(true);
                            return Mono.just(true);
                        }
                        return Mono.just(meta.isAvailable());
                    }
                });
    }

    public static Mono<Boolean> setFsDelMark(Inode inode, List<Tuple3<String, String, String>> nodeList) {
        return ErasureClient.getObjectMetaVersionUnlimited(inode.getBucket(), inode.getObjName(), inode.getVersionId(), nodeList, null)
                .zipWith(BucketSyncSwitchCache.isSyncSwitchOffMono(inode.getBucket()))
                .zipWith(MsObjVersionUtils.getObjVersionIdReactive(inode.getBucket()))
                .flatMap(tuple2 -> {
                    MetaData meta = tuple2.getT1().getT1();
                    Boolean isSyncSwitchOff = tuple2.getT1().getT2();
                    String versionstatus = tuple2.getT2();
                    if (meta.isAvailable() && meta.inode == inode.getNodeId()) {
                        meta.stamp = String.valueOf(inode.getMtime() * 1000L + inode.getMtimensec() / 1_000_000L);
                        return ErasureClient
                                .setDelMark(new DelMarkParams(inode.getBucket(), inode.getObjName(), inode.getVersionId(), meta, nodeList, versionstatus, VersionUtil.getVersionNum(isSyncSwitchOff)))
                                .map(r -> r > 0);
                    } else {
                        return Mono.just(meta.isAvailable());
                    }
                });
    }

    public static boolean instanceOf(Class cl, Class superClass) {
        Class tmp = cl;
        while (tmp != null) {
            if (tmp == superClass) {
                return true;
            }

            tmp = tmp.getSuperclass();
        }

        return false;
    }

    private static class ListDirPlus {
        private List<Inode> list = new LinkedList<>();
        protected List<Tuple3<String, String, String>> nodeList;
        public ClientTemplate.ResponseInfo<Tuple2<String, Inode>[]> responseInfo;
        public MonoProcessor<Boolean> res = MonoProcessor.create();
        protected UnicastProcessor<Integer> repairFlux = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        private boolean repairEnd = false;
        //repairNum最后要为0，表示异常数据的repair已执行
        private int repairNum = 0;
        protected String vnode;
        //保存list结果
        protected LinkedList<Counter> linkedList = new LinkedList<>();
        public StoragePool pool;
        protected MsHttpRequest request;
        protected Channel<Tuple2<String, Inode>> channel;
        private AtomicReference<String> maxNormalOrderKey = new AtomicReference<>("");

        @Data
        protected class Counter {
            //这条记录有多少个节点返回了
            int c;
            //记录对应的key
            String key;
            //某条记录
            Tuple2<String, Inode> t;
            //是否一致
            boolean identical = true;
            //记录当前文件不含createTime inode元数据的数量
            int errorNum;
            //记录待更新的createTime值
            long createTime;

            Counter(int c, String key, Tuple2<String, Inode> t) {
                this.c = c;
                this.t = t;
                this.key = key;
            }

            public Tuple2<String, Inode> getT() {
                return this.t;
            }
        }

        protected ListDirPlus(StoragePool pool, ClientTemplate.ResponseInfo<Tuple2<String, Inode>[]> responseInfo, List<Tuple3<String, String, String>> nodeList) {
            this.pool = pool;
            this.responseInfo = responseInfo;
            this.nodeList = nodeList;
            this.vnode = nodeList.get(0).var3;

            Disposable subscribe = repairFlux.subscribe(i -> {
                if (i == 0) {
                    repairEnd = true;
                } else {
                    repairNum += i;
                }

                if (repairEnd && repairNum == 0) {
                    if (channel == null) {
                        for (Counter counter : linkedList) {
                            handleResult(counter);
                        }
                        publishResult();
                    } else {
                        Iterator<Counter> iterator = linkedList.iterator();
                        while (iterator.hasNext()) {
                            channel.write(iterator.next().t);
                            iterator.remove();
                        }
                        res.onNext(true);
                    }
                }
            });
            Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> {
                subscribe.dispose();
                repairFlux.dispose();
            }));
        }

        protected void publishResult() {
            res.onNext(true);
        }

        protected String getKey(Tuple2<String, Inode> tuple2) {
            return tuple2.var1;
        }

        /**
         * 如果key相等则比较版本号是否相等;如果key不相等直接比较key
         *
         * @param t1 当前节点返回的新的响应结果
         * @param t2 之前节点返回的已经保存在linkedList中的响应结果
         **/
        protected int compareTo(Tuple2<String, Inode> t1, Tuple2<String, Inode> t2) {
            if (t2.var1.equals(t1.var1)) {
                return t1.var2.getVersionNum().compareTo(t2.var2.getVersionNum());
            } else {
                return t1.var1.compareTo(t2.var1);
            }
        }

        /**
         * 该函数会处理每个节点返回的响应，例如三节点环境，第一次传入的结果会全部放入linkedList中：在放入时会筛选createTime=0但nodeId>0的
         * inode，这些inode代表CIFS合入前版本的旧数据；第二次传入的结果会被遍历，每个inode会逐一与加入linkedList的数据比较，首先比较key的
         * 字典顺序，小的放前面大的放后面，再比较 versionNum 的先后，取最新版本为返回结果；在更新 counter 结果时，会根据情况更新 errorNum
         * <p>
         * 对于某些情况下，有节点元数据刚被 mq消息修复，list回来时其顺序相较其它两个节点均靠后，产生 marker 跳跃的问题，对此，对最终的 linkedList
         * 遍历，找到 counter.c >= 冗余副本k 数量的 key 字典顺序最大的条目，由此往后的 n个 counter.c < k的条目仅修复，但不添加至列举缓存
         * 如果这 n 个条目确实有缺失且数量 < k，且列举顺序延续上一次列举，则可以通过当前次列举修复，并在下一次 list 时填充至缓存；如果不延续上
         * 一次列举，则应当在后续的list中填充至缓存，但同样在当前次修复；如果这 n 个条目未缺失，则同样在下一次 list 时填充至缓存
         * <p>
         * 如果某次列举的结果全部都是 counter.c < k，也就是找不到 counter.c >= k，那就全部填充至列举缓存
         *
         * @param tuple var1: Integer 节点索引序号
         *              var2: ErasureServer.PayloadMetaType 对应节点返回的响应成功或失败的标志
         *              var3: Tuple2<String, Inode>[] 保存<key, inode>的数组
         **/
        public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, Tuple2<String, Inode>[]> tuple) {
            //如果这个节点响应的是error，直接返回
            if (ERROR.equals(tuple.var2)) {
                return;
            }
            //如果链表为空，也就是第一个节点响应结果，直接将第一个节点的响应结果放入链表中
            if (linkedList.isEmpty()) {
                for (Tuple2<String, Inode> t : tuple.var3) {
                    linkedList.add(updateCounter(t.var2, new Counter(1, getKey(t), t)));
                }
            } else {
                ListIterator<Counter> iterator = linkedList.listIterator();
                Counter curCounter;
                //如果不是第一个响应的节点，遍历这个节点的响应结果
                for (Tuple2<String, Inode> t : tuple.var3) {
                    //如果链表已经遍历结束，但当前节点返回的结果还没遍历结束，就将这条记录加到链表中
                    //例如：第一个节点返回3条记录，第二个节点返回4条记录的情况
                    if (!iterator.hasNext()) {
                        iterator.add(updateCounter(t.var2, new Counter(1, getKey(t), t)));
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
                        curCounter.c++;
                        updateCounter(t.var2, curCounter);
                        int j = compareTo(t, curCounter.t);
                        // 版本号不一样
                        if (j != 0) {
                            curCounter.identical = false;
                            if (j > 0) {
                                curCounter.t = t;
                            }
                        }
                    } else if (i > 0) {
                        //如果新来的key比链表中当前的key小，将新来的记录加到当前key前面
                        iterator.previous();
                        iterator.add(updateCounter(t.var2, new Counter(1, key, t)));
                    } else {
                        //如果新来的key比链表中当前的key大，遍历链表：找到某个已存在的排序比当前新来key刚刚大一位的key，把新来的key放在前面
                        //如果有key和当前一致的，对应的counter.c++
                        //如果找不到与当前对应的，遇到比当前大的就在他前面插入这条记录
                        //如果到最后一直是比当前大的，就在最后插入这条记录
                        int j = -1;
                        while (iterator.hasNext()) {
                            curCounter = iterator.next();
                            j = curCounter.key.compareTo(getKey(t));
                            if (j == 0) {
                                curCounter.c++;
                                updateCounter(t.var2, curCounter);
                                break;
                            }

                            if (j > 0) {
                                iterator.previous();
                                iterator.add(updateCounter(t.var2, curCounter));
                                break;
                            }
                        }

                        if (j < 0) {
                            iterator.add(updateCounter(t.var2, new Counter(1, key, t)));
                        }
                    }
                }
            }
        }

        /**
         * 每当遍历到的inode为旧数据，则errorNum+1；
         * 每当遍历到的inode的createTime大于0，则记录于counter中，以便在需要处理旧数据时使用
         *
         * @param inode   遍历的inode
         * @param counter 对应当前inode在所有节点响应的统计情况
         **/
        private Counter updateCounter(Inode inode, Counter counter) {
            if (inode.getNodeId() > 0 && inode.getCreateTime() == 0) {
                counter.errorNum++;
            }

            if (inode.getCreateTime() > 0) {
                counter.createTime = inode.getCreateTime();
            }

            if (counter.c >= pool.getK()) {
                maxNormalOrderKey.set(counter.key);
            }

            return counter;
        }

        /**
         * 把counter中的统计情况放入最后返回的list中，筛选出deleteMark，并且在inode中更新待更新的旧版本数据
         * 所有节点如果都是旧数据，则取atime、mtime、ctime中最小值为createTime；
         * 如果有节点是新数据，有节点是旧数据，但筛选出的inode中createTime=0，则将其置为新数据的createTime
         *
         * @param counter 当前inode所有节点响应的统计情况
         **/
        protected void handleResult(Counter counter) {
            Tuple2<String, Inode> tuple2 = counter.t;
            Inode inode = tuple2.var2;
            if (inode.getLinkN() != Inode.DELETEMARK_INODE.getLinkN()) {
                inode.setCounter(counter.errorNum);
                if (inode.getCreateTime() == 0 && counter.errorNum > 0) {
                    if (counter.createTime > 0) {
                        inode.setCreateTime(counter.createTime);
                    } else {
                        inode.setCreateTime(getMinTime(inode));
                    }
                }

                // 仅将字典顺序小于或等于maxNormalOrderKey的条目放入列举缓存
                if (maxNormalOrderKey.get().compareTo("") == 0 || (maxNormalOrderKey.get().compareTo("") > 0 && counter.key.compareTo(maxNormalOrderKey.get()) <= 0)) {
                    list.add(inode);
                }
            }
        }

        private Mono<Integer> markDelete(MetaData metadata, Inode inode) {
            if (metadata.deleteMark || NOT_FOUND_META.equals(metadata)) {
                if (inode.getObjName().endsWith("/") && DEFAULT_META_HASH.equals(inode.getReference())) {
                    return FsUtils.checkDir(inode.getBucket(), inode.getObjName())
                            .flatMap(checkRes -> {
                                //ERROR
                                if (checkRes < 0) {
                                    return Mono.just(-1);
                                } else if (checkRes == 0) {
                                    return Mono.just(1);
                                } else {
                                    return Mono.just(0);
                                }
                            });
                } else {
                    return Mono.just(1);
                }
            } else {
                return Mono.just(0);
            }
        }

        protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList) {
            Tuple2<String, Inode> counterTuple = counter.getT();
            Inode inode = counterTuple.var2;
            return ErasureClient.getObjectMetaVersionUnlimited(inode.getBucket(), inode.getObjName(), inode.getVersionId(), nodeList, null)
                    .flatMap(metaData -> {
                        boolean res = !metaData.equals(MetaData.ERROR_META);
                        if (res) {
                            Inode tmpInode;
                            if (StringUtils.isNotBlank(metaData.getTmpInodeStr())) {
                                tmpInode = Json.decodeValue(metaData.tmpInodeStr, Inode.class);
                            } else {
                                tmpInode = null;
                            }

                            return markDelete(metaData, inode)
                                    .flatMap(deleteRes -> {
                                        if (deleteRes == 0) {
                                            counter.getT().var2 = new Inode()
                                                    .setVersionNum(metaData.getVersionNum())
                                                    .setBucket(metaData.getBucket())
                                                    .setVersionId(metaData.getVersionId())
                                                    .setCookie(metaData.cookie)
                                                    .setNodeId(metaData.inode)
                                                    .setObjName(metaData.getKey());
                                            copyTempInode(tmpInode, counter.getT().var2);
                                            // list触发修复后用于createS3Inode
                                            if (null == tmpInode && metaData.syncStamp != null && null == counter.getT().var2.getReference()) {
                                                counter.getT().var2.setReference(Utils.metaHash(metaData));
                                            }

                                            return Mono.just(res);
                                        } else if (deleteRes == 1) {
                                            counter.getT().var2 = new Inode()
                                                    .setVersionNum(metaData.getVersionNum())
                                                    .setBucket(metaData.getBucket())
                                                    .setVersionId(metaData.getVersionId())
                                                    .setCookie(metaData.cookie)
                                                    .setNodeId(metaData.inode)
                                                    .setObjName(metaData.getKey())
                                                    .setLinkN(Inode.DELETEMARK_INODE.getLinkN());
                                            copyTempInode(tmpInode, counter.getT().var2);

                                            return Mono.just(res);
                                        } else {
                                            return Mono.just(false);
                                        }
                                    });
                        }
                        return Mono.just(res);
                    });
        }

        protected void putErrorList(Counter counter) {

        }

        protected void publishErrorList() {

        }

        public void handleComplete() {
            if (responseInfo.successNum < pool.getK()) {
                res.onNext(false);
                return;
            }

            //存放没有得到k+m份返回的元数据
            for (Counter counter : linkedList) {
                //返回响应不及k个的元数据需要马上进行repair，若失败则lsfile请求返500
                //else，没有得到k+m份返回或有不一致的元数据在异常处理中修复，可延后。
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

        public void handleCompleteNoRepair() {
            if (responseInfo.successNum < pool.getK()) {
                res.onNext(false);
                return;
            }
            //存放没有得到k+m份返回的元数据
            for (Counter counter : linkedList) {
                //返回响应不及k个的元数据需要马上进行repair，若失败则lsfile请求返500
                //else，没有得到k+m份返回或有不一致的元数据在异常处理中修复，可延后。
                if (counter.c < pool.getK()) {
                    res.onNext(false);
                } else {
                    if (counter.c != pool.getK() + pool.getM() || !counter.identical) {
                        putErrorList(counter);
                    }
                }
            }
            publishErrorList();
            repairFlux.onNext(0);
        }

        private void copyTempInode(Inode tmpInode, Inode inode) {
            if (tmpInode != null) {
                inode.setSize(tmpInode.getSize())
                        .setMode(tmpInode.getMode())
                        .setCifsMode(CifsUtils.changeToHiddenCifsMode(inode.getObjName(), tmpInode.getCifsMode(), true))
                        .setUid(tmpInode.getUid())
                        .setAtime(tmpInode.getAtime())
                        .setAtimensec(tmpInode.getAtimensec())
                        .setMtime(tmpInode.getMtime())
                        .setMtimensec(tmpInode.getMtimensec())
                        .setCtime(tmpInode.getCtime())
                        .setCtimensec(tmpInode.getCtimensec())
                        .setReference(tmpInode.getReference())
                        .setLinkN(tmpInode.getLinkN())
                        .setGid(tmpInode.getGid());
                CifsUtils.setDefaultCifsMode(inode);
            }
        }
    }

    public static Flux<Tuple2<Integer, byte[]>> readObj(int readN, String pool, String bucket, String fileName, long offset, int size, long fileSize) {
        //hole file
        if (StringUtils.isBlank(fileName)) {
            return Flux.just(new Tuple2<>(readN, new byte[size]));
        }

        StoragePool storagePool = StoragePoolFactory.getStoragePool(pool, bucket);
        String vnode = storagePool.getObjectVnodeId(fileName);
        List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(vnode).block();

        UnicastProcessor<Long> streamController = UnicastProcessor.create();

        UnicastProcessor<Tuple2<Integer, byte[]>> res = UnicastProcessor.create();
        AtomicInteger n = new AtomicInteger(readN);

        ECUtils.getObject(storagePool, fileName, false, offset, offset + size - 1, fileSize, nodeList, streamController, null, null)
                .doOnNext(b -> streamController.onNext(1L))
                .doOnError(e -> {
                    log.error("", e);
                    res.onError(e);
                })
                .doOnComplete(res::onComplete)
                .subscribe(b -> {
                    res.onNext(new Tuple2<>(n.get(), b));
                    n.addAndGet(b.length);
                });

        return res;
    }


    /**
     * 删除文件Inode及数据块；先删"*-+(空"五种类型的元数据，如果删除时 Inode 的linkN是大于1，再重新添加"("元数据
     * 删除元数据前检查look_up返回inode的objName是否为本次删除请求的objName；若不是，则表明本次删除的为同nodeId
     * 的其它硬链接，将inode的objName重置为请求的文件名
     *
     * @param inode   待删除文件的inode
     * @param objName 本次请求删除的文件名
     **/
    public static Mono<Boolean> deleteFile(Inode inode, List<Tuple3<String, String, String>> nodeList, String objName) {
        if (!inode.getObjName().equals(objName)) {
            inode.setObjName(objName);
        }
        return setFsDelMark(inode, nodeList)
                .flatMap(b -> {
                    if (!b) {
                        log.error("delete object or link internal error!");
                        return Mono.just(false);
                    } else {
                        int linkN = inode.getLinkN();
                        if (linkN > 1) {
                            inode.setLinkN(linkN - 1);
                            return InodeUtils.updateInode(inode.getBucket(), inode, false, false);
                        }
                        return deleteFile0(inode.getInodeData(), inode.getBucket());
                    }
                });
    }

    public static Mono<Boolean> deleteChunkFile(List<Inode.InodeData> inodeData, String bucket, boolean... needCheck) {
        Map<String, List<String>> needDeleteMap = new HashMap<>();

        for (Inode.InodeData data : inodeData) {
            needDeleteMap.computeIfAbsent(data.getStorage(), k -> new LinkedList<>());
            if (StringUtils.isNotEmpty(data.getFileName())) {
                needDeleteMap.get(data.getStorage()).add(data.getFileName());
            }
        }
        boolean needCheck0 = needCheck.length > 0 && needCheck[0];
        return Flux.fromStream(needDeleteMap.entrySet().stream())
                .flatMap(e -> {
                    String storage = e.getKey();
                    List<String> needDeleteFile = e.getValue();
                    StoragePool dataPool = StoragePoolFactory.getStoragePool(storage, bucket);
                    String[] fileNames = needDeleteFile.toArray(new String[0]);
                    return Flux.fromArray(fileNames).flatMap(f -> Mono.just(f).zipWith(Mono.just(dataPool)));
                })
                .flatMap(zip -> {
                    StoragePool dataPool = zip.getT2();
                    String f = zip.getT1();
                    return deleteChunkFile0(dataPool, bucket, new String[]{f}, needCheck0);
                }, 24)
                .collectList()
                .map(l -> true)
                .timeout(Duration.ofSeconds(20), Mono.just(false))
                .onErrorResume(e -> Mono.just(false));
    }

    /**
     * 递归删除chunk中的数据块，适配chunk中存在chunk的情况，
     * 防止删除时通过inode一致性去查询chunk（会与删除操作互斥导致死锁） 导致出现超时的情况
     *
     * @param dataPool  数据池
     * @param bucket    存储桶
     * @param fileNames chunk文件名
     **/
    public static Mono<Boolean> deleteChunkFile0(StoragePool dataPool, String bucket, String[] fileNames, boolean needCheck) {
        return Flux.fromArray(fileNames)
                .flatMap(f -> {
                    if (StringUtils.isBlank(f)) {
                        return Mono.just(true);
                    } else if (f.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                        Tuple3<Long, String, String> chunk0 = ChunkFile.getChunkFromFileName(f);
                        return ChunkFileUtils.getChunk(bucket, chunk0.var3)
                                .flatMap(chunk -> {
                                    // 处理不同池（缓存池、数据池）文件
                                    Map<String, List<String>> map = new HashMap<>();
                                    chunk.getChunkList().stream().forEach(inodeData0 ->
                                            map.computeIfAbsent(inodeData0.storage, k -> new ArrayList<>()).add(inodeData0.fileName)
                                    );

                                    Flux<Boolean> deleteResults = Flux.fromIterable(map.entrySet())
                                            .flatMap(entry -> {
                                                StoragePool storagePool = StoragePoolFactory.getStoragePool(entry.getKey(), bucket);
                                                String[] fs = entry.getValue().toArray(new String[0]);
                                                if (needCheck) {
                                                    Flux<Boolean> resFlux = Flux.empty();
                                                    List<String> notCheckList = new ArrayList<>();
                                                    for (String file : fs) {
                                                        if (StringUtils.isNotBlank(file) && file.endsWith("/split/")) {
                                                            resFlux = resFlux.mergeWith(
                                                                    FsUtils.checkFileInInode(bucket, chunk0.var1, file, "", "")
                                                                            .flatMap(t -> {
                                                                                if (t.var1 <= 0) {
                                                                                    return ErasureClient.deleteObjectFile(storagePool, new String[]{file}, null);
                                                                                } else {
                                                                                    return Mono.just(true);
                                                                                }
                                                                            }));
                                                        } else {
                                                            notCheckList.add(file);
                                                        }
                                                    }
                                                    if (!notCheckList.isEmpty()) {
                                                        resFlux = resFlux.mergeWith(
                                                                ErasureClient.deleteObjectFile(storagePool, notCheckList.toArray(new String[0]), null));
                                                    }
                                                    return resFlux
                                                            .collectList()
                                                            .map(results -> results.stream().allMatch(Boolean::booleanValue));
                                                }
                                                return deleteChunkFile0(storagePool, bucket, fs, false);
                                            });

                                    return deleteResults
                                            .collectList()
                                            .map(results -> results.stream().allMatch(Boolean::booleanValue));
                                });
                    } else {
                        return dataPool.mapToNodeInfo(dataPool.getObjectVnodeId(f))
                                .flatMap(list -> {
                                    Tuple2<Mono<Boolean>, Disposable> tuple = ErasureClient.deleteObjectFile(dataPool, f, list);
                                    return tuple.var1;
                                });
                    }
                }, 16)
                .collectList()
                .map(l -> true);
    }

    public static Mono<Boolean> deleteFile0(List<Inode.InodeData> inodeData, String bucket) {
        return deleteChunkFile(inodeData, bucket, true)
                .flatMap(res -> {
                    //删除chunk 元数据
                    String[] chunkFiles = inodeData
                            .stream()
                            .filter(inodeData0 -> {
                                return StringUtils.isNotEmpty(inodeData0.getFileName())
                                        && inodeData0.fileName.startsWith(ROCKS_CHUNK_FILE_KEY);
                            })
                            .map(l -> l.fileName)
                            .toArray(String[]::new);
                    return Flux.fromArray(chunkFiles)
                            .flatMap(fileName -> FsUtils.deleteChunkMeta(fileName, bucket))
                            .collectList()
                            .map(l -> true);
                });
    }

    public static Mono<Integer> repairInode(String key, Inode inode, List<Tuple3<String, String, String>> nodeList, Tuple2<PayloadMetaType, Inode>[] res) {
        //ERROR or NOT FOUND；以及down节点时查找父目录返回的nodeId=0的结果无需修复，避免影响s3上传流量
        if (inode.getLinkN() < 0 || (!inode.isDeleteMark() && inode.getNodeId() == 0)) {
            return Mono.just(1);
        }

        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, Inode> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.getVersionNum());
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(inode.getBucket());
        //inode.setVersionNum(getLastVersionNum(inode.getVersionNum(), inode.getBucket()));
        String bucketVnode = pool.getBucketVnodeId(inode.getBucket());
        String inodeKey = Inode.getKey(bucketVnode, inode.getBucket(), inode.getNodeId());

        return ErasureClient.getObjectMetaVersionUnlimitedNotRecover(inode.getBucket(), inode.getObjName(), inode.getVersionId(), nodeList, null, null, null)
                .flatMap(meta -> {
                    //如果meta已经不存在则不再修复inode
                    if (meta != null && meta.equals(MetaData.NOT_FOUND_META)) {
                        return Mono.just(1);
                    }

                    return ECUtils.updateRocksKey(pool, oldVersionNum, inodeKey, Json.encode(inode), PUT_INODE, ERROR_PUT_INODE, nodeList, null);
                });

    }


    public static Mono<Boolean> deleteChunkMeta(String chunkFileName) {
        return deleteChunkMeta(chunkFileName, ChunkFile.getChunkFromFileName(chunkFileName).var2);
    }

    public static Mono<Boolean> deleteChunkMeta(String chunkFileName, String bucket) {
        String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(bucket, chunkFileName);
        return ChunkFileUtils.getChunk(bucket, chunkKey)
                .flatMap(chunkFile -> {
                    if (chunkFile.equals(ERROR_CHUNK)) {
                        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
                        String v = storagePool.getBucketVnodeId(bucket);
                        List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(v).block();
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("key", chunkFileName)
                                .put("value", String.valueOf(bucket))
                                .put("lun", getDiskName(nodeList.get(0).var1, nodeList.get(0).var2))
                                .put("poolQueueTag", poolQueueTag);
                        ObjectPublisher.publish(CURRENT_IP, errorMsg, ERROR_DEL_CHUNK);
                        return Mono.just(false);
                    }
                    String[] chunkFiles = chunkFile.getChunkList()
                            .stream()
                            .filter(inodeData -> StringUtils.isNotEmpty(inodeData.getFileName())
                                    && inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY))
                            .map(l -> l.fileName)
                            .toArray(String[]::new);
                    return Flux.fromArray(chunkFiles)
                            .flatMap(fileName -> {
                                return deleteChunkMeta(fileName, bucket);
                            }, 16)
                            .collectList()
                            .flatMap(l -> deleteChunkMeta0(chunkFileName, bucket));
                });
    }

    public static Mono<Boolean> deleteChunkMeta0(String chunkFileName, String bucket) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
        String v = storagePool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(v).block();
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", chunkFileName)
                        .put("value", String.valueOf(bucket))
                        .put("lun", tuple.var2)
                        .put("poolQueueTag", poolQueueTag)
                )
                .collect(Collectors.toList());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DEL_CHUNK, String.class, nodeList);
        responseInfo.responses.subscribe(r -> {

                },
                e -> {
                    log.error("", e);
                    res.onNext(false);
                },
                () -> {
                    if (responseInfo.successNum == 0) {
                        res.onNext(false);
                    } else {
                        if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                            res.onNext(true);
                        } else if (responseInfo.successNum >= storagePool.getK()) {
                            publishEcError(responseInfo.res, nodeList, msgs.get(0), ERROR_DEL_CHUNK);
                            res.onNext(true);
                        } else {
                            //TODO
                            res.onNext(false);
                        }
                    }
                });
        return res;
    }

    /**
     * @param fileName   待检查的fileName
     * @param needRemove 是否要要移除待检查fileName的"/split/"再比较
     **/
    public static String replaceSplit(String fileName, boolean needRemove) {
        if (needRemove) {
            return fileName.replace("/split/", "");
        } else {
            return fileName;
        }
    }

    private static Mono<Tuple2<Integer, InodeData>> loopTotalInodeData(List<InodeData> list, String fileName, long fileOffset, long fileSize) {
        boolean[] removeSplitStr = {true};
        //fileName带"/split/"，不移除inode中字符串；未携带"/split/"，则需要移除
        if (StringUtils.isNotBlank(fileName) && fileName.endsWith("/split/")) {
            removeSplitStr[0] = false;
        }

        if (fileOffset == 0) {
            List<Mono<Tuple2<Integer, InodeData>>> chunkRes = new LinkedList<>();
            long curOffset = 0L;

            for (InodeData inodeData : list) {
                if (replaceSplit(inodeData.fileName, removeSplitStr[0]).equals(fileName)) {
                    return Mono.just(new Tuple2<>(1, inodeData));
                } else if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                    Mono<Tuple2<Integer, InodeData>> res = Node.getInstance().getChunk(inodeData.fileName)
                            .flatMap(chunk -> loopTotalInodeData(chunk.getChunkList(), fileName, 0L, 0L));

                    chunkRes.add(res);
                }
                curOffset += inodeData.size;
            }

            if (chunkRes.isEmpty()) {
                return Mono.just(new Tuple2<>(0, null)); // 未找到
            } else {
                return Flux.merge(chunkRes)
                        .filter(tuple -> tuple.var1 == 1 && replaceSplit(tuple.var2.fileName, removeSplitStr[0]).equals(fileName))
                        .next()
                        .switchIfEmpty(Mono.just(new Tuple2<>(0, null)));
            }
        } else {
            long curOffset = 0L;
            List<Mono<Tuple2<Integer, InodeData>>> chunkRes = new LinkedList<>();

            for (InodeData inodeData : list) {
                long curEnd = curOffset + inodeData.getSize();

                if (curOffset >= fileOffset + fileSize) {
                    break;
                } else if (fileOffset < curEnd) {
                    if (replaceSplit(inodeData.fileName, removeSplitStr[0]).equals(fileName)) {
                        return Mono.just(new Tuple2<>(1, inodeData));
                    } else if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                        long chunkOff = fileOffset - curOffset + inodeData.getOffset();
                        long chunkSize = fileSize;
                        Mono<Tuple2<Integer, InodeData>> res = Node.getInstance().getChunk(inodeData.fileName)
                                .flatMap(chunk -> loopTotalInodeData(chunk.getChunkList(), fileName, chunkOff, chunkSize));

                        chunkRes.add(res);
                    }
                }
                curOffset = curEnd;
            }

            if (chunkRes.isEmpty()) {
                return Mono.just(new Tuple2<>(0, null));
            } else {
                return Flux.merge(chunkRes)
                        .filter(tuple -> tuple.var1 == 1 && replaceSplit(tuple.var2.fileName, removeSplitStr[0]).equals(fileName))
                        .next()
                        .switchIfEmpty(Mono.just(new Tuple2<>(0, null)));
            }
        }
    }

    /**
     * 判断fileName是否还在inode中
     *
     * @param fileName 1) 入参fileName在inodeOperator needDeleteMap中调用时，fileName带有"/spilt/"后缀，此时与inode中fileName匹配
     *                 2) 而在缓存池下刷时，fileName不带有"/split/"后缀，此时需对loopTotalInodeData做处理，使inode中fileName能与入参匹配
     * @return <0,"">:fileName已经不在inode中  <1,storage-x>:fileName在inode中  <-1, "">:错误
     */
    public static Mono<Tuple2<Integer, String>> checkFileInInode(String bucket, long nodeId, String fileName, String fileSize, String fileOffset) {
        if (Node.getInstance() == null) {
            return Mono.just(new Tuple2<>(-1, ""));
        }
        long offset = 0L;
        if (StringUtils.isNotBlank(fileOffset)) {
            offset = Long.parseLong(fileOffset);
        }
        long size = 0L;
        if (StringUtils.isNotBlank(fileSize)) {
            size = Long.parseLong(fileSize);
        }
        long finalOffset = offset;
        long finalSize = size;
        return Node.getInstance().getInode(bucket, nodeId)
                .timeout(Duration.ofMillis(30_000))
                .flatMap(inode -> loopTotalInodeData(inode.getInodeData(), fileName, finalOffset, finalSize))
                .map(res -> new Tuple2<>(res.var1, res.var2 == null ? null : res.var2.storage))
                .onErrorReturn(new Tuple2<>(-1, ""));
    }

    public static Mono<String> findCookie(String bucket, long cookie) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();
        String cookieKey = Inode.getCookieKey(bucketVnode, bucket, cookie);

        return BucketSyncSwitchCache.isSyncSwitchOffMono(bucket)
                .map(isSyncSwitchOff -> NOT_FOUND_INODE.clone()
                        .setVersionNum(VersionUtil.getLastVersionNum("", isSyncSwitchOff)))
                .flatMap(deleteMark -> ECUtils.getRocksKey(pool, cookieKey, Inode.class, GET_COOKIE, NOT_FOUND_INODE, ERROR_INODE, deleteMark,
                        Inode::getVersionNum, Comparator.comparing(a -> a.getVersionNum()), FsUtils::repairCookie, nodeList, null))
                .map(t -> t.var1)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        return Mono.just("");
                    } else {
                        return Mono.just(inode.getObjName());
                    }
                });
    }

    public static Mono<Integer> repairCookie(String key, Inode cookieInode, List<Tuple3<String, String, String>> nodeList, Tuple2<PayloadMetaType, Inode>[] res) {
        if (cookieInode.getCookie() == 0 || cookieInode.getNodeId() <= 0) {
            return Mono.just(1);
        }

        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, Inode> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.getVersionNum());
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(cookieInode.getBucket());
        String bucketVnode = pool.getBucketVnodeId(cookieInode.getBucket());
        String cookieKey = Inode.getCookieKey(bucketVnode, cookieInode.getBucket(), cookieInode.getCookie());
        return ECUtils.updateRocksKey(pool, oldVersionNum, cookieKey, Json.encode(cookieInode), PUT_COOKIE, ERROR_PUT_COOKIE, nodeList, null);
    }

    //检查目录下是否还存在对象
    public static Mono<Integer> checkDir(String bucket, String obj) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();

        String checkKey = Utils.getMetaDataKey(bucketVnode, bucket, obj, "");
        List<SocketReqMsg> msgs = nodeList.stream().map(t -> new SocketReqMsg("", 1)
                        .put("lun", t.var2)
                        .put("key", checkKey)
                )
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, CHECK_DIR, String.class, nodeList);

        MonoProcessor<Integer> res = MonoProcessor.create();
        responseInfo.responses.subscribe(s -> {
        }, e -> {
            log.error("", e);
            res.onNext(-1);
        }, () -> {
            if (responseInfo.successNum >= pool.getK()) {
                res.onNext(1);
            } else {
                int notFoundNum = 0;
                for (Tuple2<PayloadMetaType, String> result : responseInfo.res) {
                    if (result.var1 == NOT_FOUND) {
                        notFoundNum++;
                    }
                }

                if (notFoundNum >= pool.getK()) {
                    res.onNext(0);
                } else if (responseInfo.successNum > 0) {
                    res.onNext(1);
                } else if (notFoundNum > 0) {
                    res.onNext(0);
                } else {
                    res.onNext(-1);
                }
            }
        });

        return res;

    }

    public static Flux<byte[]> getHoleFileBytes(long curPartStart, long curPartEnd, StoragePool pool0, UnicastProcessor<Long> streamController) {
        long last = curPartEnd - curPartStart + 1;
        UnicastProcessor<Long> holeSize = UnicastProcessor.create();
        holeSize.onNext(last);
        AtomicLong lastSize = new AtomicLong(last);
        int defaultSize = pool0.getPackageSize() * pool0.getK();
        byte[] buf = new byte[defaultSize];
        streamController.subscribe(holeSize::onNext);
        UnicastProcessor<byte[]> resFlux = UnicastProcessor.create(Queues.<byte[]>unboundedMultiproducer().get());

        holeSize.subscribe(size -> {
            long nowSize = lastSize.get();
            if (nowSize > 0) {
                long cur = Math.min(nowSize, defaultSize);
                lastSize.addAndGet(-cur);
                if (cur == defaultSize) {
                    resFlux.onNext(buf);
                } else {
                    resFlux.onNext(new byte[(int) cur]);
                }
                for (int i = 1; i < pool0.getK(); i++) {
                    resFlux.onNext(EMPTY_BYTE);
                }
            } else {
                resFlux.onComplete();
                holeSize.onComplete();
            }
        });

        return resFlux;
    }

    /**
     * 从redis表2获取端口号
     *
     * @param portName 端口的名称
     * @param defaultPort 端口的默认值
     **/
    public static int getFsPort(String portName, int defaultPort) {
        int port = defaultPort;

        try {
            if (StringUtils.isBlank(portName)) {
                return port;
            }

            String portValue = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get(portName);
            if (StringUtils.isNotBlank(portValue)) {
                port = Integer.parseInt(portValue);
            }

        } catch (Exception e) {
            log.error("get port value error, name: {}, ", portName, e);
        }

        return port;
    }
}
