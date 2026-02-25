package com.macrosan.snapshot;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.Utils;
import com.macrosan.ec.part.PartClient;
import com.macrosan.lifecycle.LifecycleService;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ListMultipartUploadsResult;
import com.macrosan.message.xmlmsg.ListPartsResult;
import com.macrosan.message.xmlmsg.section.Part;
import com.macrosan.message.xmlmsg.versions.DeleteMarker;
import com.macrosan.message.xmlmsg.versions.ListVersionsResult;
import com.macrosan.message.xmlmsg.versions.Version;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.LifecycleClientHandler;
import com.macrosan.storage.client.ListAllLifecycleClientHandler;
import com.macrosan.storage.client.channel.ListMultiPartClientMergeChannel;
import com.macrosan.storage.client.channel.ListVersionsMergeChannel;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.REDIS_SNAPSHOT_INDEX;
import static com.macrosan.constants.SysConstants.ROCKS_LIFE_CYCLE_PREFIX;
import static com.macrosan.ec.Utils.getLifeCycleStamp;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_LIFE_OBJECT;
import static com.macrosan.snapshot.BucketSnapshotStarter.SNAP_SCHEDULER;
import static com.macrosan.snapshot.utils.SnapshotMergeUtil.getMergeProgressRedisKey;


/**
 * @author zhaoyang
 * @date 2024/08/05
 **/
@Log4j2
public class SnapshotListTemplate {
    public static Mono<Boolean> listVersionsTemplate(String bucketName, StoragePool storagePool, Function<Tuple2<String, String>, Mono<Boolean>> versionConsumer, String snapshotMark) {
        UnicastProcessor<String> listController = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("bucket", bucketName)
                .put("maxKeys", "1000")
                .put("prefix", "")
                .put("marker", "")
                .put("versionIdMarker", "")
                .put("delimiter", "")
                .put("currentSnapshotMark", snapshotMark);
        ListVersionsResult listVersionsRes = new ListVersionsResult()
                .setDelimiter("")
                .setKeyMarker("")
                .setPrefix("")
                .setName(bucketName)
                .setMaxKeys(1000);
        MonoProcessor<Boolean> result = MonoProcessor.create();
        try {
            List<String> infoList = storagePool.getBucketVnodeList(bucketName);
            Disposable[] disposables = new Disposable[]{null};
            disposables[0] = listController
                    .publishOn(SNAP_SCHEDULER)
                    .doFinally(s -> Optional.ofNullable(disposables[0]).ifPresent(Disposable::dispose))
                    .filter(b -> {
                        if (!"master".equals(Utils.getRoleState())) {
                            result.onError(new RuntimeException("salve node cannot list operation"));
                            return false;
                        }
                        return true;
                    })
                    .subscribe(b -> {
                        ListVersionsMergeChannel channel = (ListVersionsMergeChannel) new ListVersionsMergeChannel(bucketName, listVersionsRes, storagePool, infoList, null)
                                .withBeginPrefix("", "", "");
                        channel.request(msg);
                        channel.response()
                                .doOnNext(res -> {
                                    if (!res) {
                                        listController.onError(new RuntimeException("list version error"));
                                    }
                                })
                                .flatMapMany(res -> Flux.fromIterable(listVersionsRes.getVersion()))
                                .flatMap(versionBase -> {
                                    String key = null;
                                    String versionId = null;
                                    if (versionBase instanceof Version) {
                                        Version version = (Version) versionBase;
                                        key = version.getKey();
                                        versionId = version.getVersionId();
                                    }
                                    if (versionBase instanceof DeleteMarker) {
                                        DeleteMarker deleteMarker = (DeleteMarker) versionBase;
                                        key = deleteMarker.getKey();
                                        versionId = deleteMarker.getVersionId();
                                    }
                                    return Mono.just(new Tuple2<>(key, versionId));
                                })
                                .flatMap(versionConsumer)
                                .subscribe(res -> {
                                }, listController::onError, () -> {
                                    if (listVersionsRes.getVersion().isEmpty()) {
                                        result.onNext(true);
                                        listController.onComplete();
                                    } else {
                                        listController.onNext("1");
                                    }
                                });

                    }, result::onError);
            listController.onNext("1");
        } catch (Exception e) {
            result.onError(e);
        }
        return result;
    }

    public static Mono<Boolean> listMultiPartTemplate(String bucketName, String beginKeyMarker, String beginUploadIdMarker, StoragePool storagePool, Function<Tuple2<String, String>, Mono<Boolean>> uploadConsumer, String snapshotMark, String saveProgressKey) {
        MonoProcessor<Boolean> result = MonoProcessor.create();
        UnicastProcessor<Tuple2<String, String>> listController = UnicastProcessor.create(Queues.<Tuple2<String, String>>unboundedMultiproducer().get());
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("bucket", bucketName)
                .put("maxUploads", "1000")
                .put("prefix", "")
                .put("marker", "")
                .put("delimiter", "")
                .put("uploadIdMarker", "")
                .put("currentSnapshotMark", snapshotMark);
        ListMultipartUploadsResult listMultiUploadsRes = new ListMultipartUploadsResult()
                .setDelimiter("")
                .setKeyMarker("")
                .setPrefix("")
                .setBucket(bucketName)
                .setMaxUploads(1000);
        try {
            List<String> infoList = storagePool.getBucketVnodeList(bucketName);
            Disposable[] disposables = new Disposable[]{null};
            AtomicInteger findNum = new AtomicInteger(0);
            disposables[0] = listController
                    .publishOn(SNAP_SCHEDULER)
                    .doFinally(s -> Optional.ofNullable(disposables[0]).ifPresent(Disposable::dispose))
                    .filter(b -> {
                        if (!"master".equals(Utils.getRoleState())) {
                            result.onError(new RuntimeException("salve node cannot list operation"));
                            return false;
                        }
                        return true;
                    })
                    .subscribe(tuple2 -> {
                        msg.put("marker", tuple2.var1);
                        msg.put("uploadIdMarker", tuple2.var2);
                        ListMultiPartClientMergeChannel channel = (ListMultiPartClientMergeChannel) new ListMultiPartClientMergeChannel(null, listMultiUploadsRes, storagePool, infoList, null, snapshotMark)
                                .withBeginPrefix("", tuple2.var1, "");
                        AtomicBoolean hasFail = new AtomicBoolean();
                        channel.request(msg);
                        channel.response()
                                .doOnNext(res -> {
                                    if (!res) {
                                        listController.onError(new RuntimeException("list version error"));
                                    }
                                })
                                .flatMapMany(res -> Flux.fromIterable(listMultiUploadsRes.getUploads()))
                                .map(upload -> new Tuple2<>(upload.getKey(), upload.getUploadId()))
                                .flatMap(uploadConsumer)
                                .subscribe(res -> {
                                    if (!res && !hasFail.get()) {
                                        hasFail.set(true);
                                    }
                                }, result::onError, () -> {
                                    if (hasFail.get()) {
                                        // 存在错误，则重新扫描本轮
                                        listMultiUploadsRes.setTruncated(false);
                                        listController.onNext(tuple2);
                                    } else if (listMultiUploadsRes.isTruncated()) {
                                        // 保存进度
                                        Mono.just(findNum.addAndGet(listMultiUploadsRes.getUploads().size()) >= 10000)
                                                .flatMap(needSave -> {
                                                    if (needSave && saveProgressKey != null) {
                                                        return Mono.just(true)
                                                                .publishOn(SNAP_SCHEDULER)
                                                                .handle((b, sink) -> {
                                                                    Map<String, String> map = new HashMap<>(2);
                                                                    map.put("beginKeyMarker", listMultiUploadsRes.getNextKeyMarker());
                                                                    map.put("beginUploadIdMarker", listMultiUploadsRes.getNextUploadIdMarker());
                                                                    RedisConnPool.getInstance().getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hmset(saveProgressKey, map);
                                                                    findNum.set(0);
                                                                    sink.next(true);
                                                                });
                                                    }
                                                    return Mono.just(true);
                                                }).subscribe(b -> {
                                                    listMultiUploadsRes.setTruncated(false);
                                                    listController.onNext(new Tuple2<>(listMultiUploadsRes.getNextKeyMarker(), listMultiUploadsRes.getNextUploadIdMarker()));
                                                });
                                    } else {
                                        result.onNext(true);
                                        listController.onComplete();
                                    }
                                });
                    }, result::onError);
            listController.onNext(new Tuple2<>(beginKeyMarker == null ? "" : beginKeyMarker, beginUploadIdMarker == null ? "" : beginUploadIdMarker));
        } catch (Exception e) {
            result.onError(e);
        }
        return result;
    }

    public static Mono<Boolean> listPartTemplate(String bucketName, String object, String uploadId, StoragePool storagePool, Function<Integer, Mono<Boolean>> partConsumer, String snapshotMark) {
        UnicastProcessor<Integer> listController = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        ListPartsResult listPartsResult = new ListPartsResult()
                .setBucket(bucketName)
                .setMaxParts(1000)
                .setKey(object)
                .setUploadId(uploadId);
        MonoProcessor<Boolean> result = MonoProcessor.create();
        Disposable[] disposables = new Disposable[]{null};
        disposables[0] = listController
                .publishOn(SNAP_SCHEDULER)
                .doFinally(s -> Optional.ofNullable(disposables[0]).ifPresent(Disposable::dispose))
                .filter(b -> {
                    if (!"master".equals(Utils.getRoleState())) {
                        result.onError(new RuntimeException("salve node cannot list operation"));
                        return false;
                    }
                    return true;
                })
                .subscribe(mark -> {
                    listPartsResult.setPartNumberMarker(mark);
                    String bucketVnode = storagePool.getBucketVnodeId(bucketName, object);
                    AtomicBoolean hasFail = new AtomicBoolean(false);
                    storagePool.mapToNodeInfo(bucketVnode)
                            .flatMap(nodeList -> PartClient.listParts(listPartsResult, nodeList, null, snapshotMark, null))
                            .doOnNext(res -> {
                                if (!res) {
                                    listController.onError(new RuntimeException("list version error"));
                                }
                            })
                            .flatMapMany(res -> Flux.fromIterable(listPartsResult.getParts()))
                            .map(Part::getPartNumber)
                            .flatMap(partConsumer)
                            .subscribe(res -> {
                                if (!res && !hasFail.get()) {
                                    hasFail.set(true);
                                }
                            }, result::onError, () -> {
                                if (hasFail.get()) {
                                    listPartsResult.setTruncated(false);
                                    listController.onNext(mark);
                                } else if (listPartsResult.isTruncated()) {
                                    listPartsResult.setTruncated(false);
                                    listController.onNext(listPartsResult.getNextPartNumberMarker());
                                } else {
                                    result.onNext(true);
                                    listController.onComplete();
                                }
                            });

                }, result::onError);
        listController.onNext(0);
        return result;
    }

    public static Mono<Boolean> listObjectTemplate(String bucketName, String beginPrefix, String endStamp, StoragePool storagePool, String snapshotMark, Function<MetaData, Mono<Boolean>> objectConsumer, String saveProgressKey) {
        MonoProcessor<Boolean> result = MonoProcessor.create();
        List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucketName);
        AtomicInteger startIndex = new AtomicInteger(0);
        UnicastProcessor<String> bucketVnodeProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        SocketReqMsg reqMsg = new SocketReqMsg("", 0)
                .put("bucket", bucketName)
                .put("maxKeys", "1000")
                .put("stamp", endStamp)
                .put("currentSnapshotMark", snapshotMark);
        AtomicInteger findNum = new AtomicInteger();
        bucketVnodeProcessor.subscribe(vnode -> {
            String curBeginPrefix = beginPrefix == null ? getLifeCycleStamp(vnode, bucketName, "0") :
                    ROCKS_LIFE_CYCLE_PREFIX + vnode + beginPrefix.substring(beginPrefix.indexOf(File.separator));
            UnicastProcessor<String> listController = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
            listController.subscribe(beginPrefixMark -> {
                reqMsg.put("beginPrefix", beginPrefixMark);
                storagePool.mapToNodeInfo(vnode)
                        .subscribe(infoList -> {
                            String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                            reqMsg.put("vnode", nodeArr[0]);
                            List<SocketReqMsg> msgs = infoList.stream().map(info -> reqMsg.copy().put("lun", info.var2)).collect(Collectors.toList());
                            AtomicBoolean hasFail = new AtomicBoolean(false);
                            AtomicReference<String> nextBeginPrefix = new AtomicReference<>();
                            AtomicInteger size = new AtomicInteger();
                            ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_LIFE_OBJECT, new TypeReference<Tuple3<Boolean,
                                    String, MetaData>[]>() {
                            }, infoList);
                            AtomicBoolean listComplete = new AtomicBoolean(false);
                            ListAllLifecycleClientHandler lifecycleClientHandler = new ListAllLifecycleClientHandler(responseInfo, nodeArr[0], reqMsg, bucketName);
                            responseInfo.responses.publishOn(LifecycleService.getScheduler()).subscribe(lifecycleClientHandler::handleResponse, e -> log.error("", e),
                                    lifecycleClientHandler::completeResponse);
                            lifecycleClientHandler.res
                                    .map(metas -> {
                                        if (metas.size() > 1000) {
                                            nextBeginPrefix.set(metas.get(1000).getKey());
                                            size.set(1000);
                                        } else {
                                            listComplete.set(true);
                                            size.set(metas.size());
                                        }
                                        return metas.stream().limit(1000).collect(Collectors.toList());
                                    })
                                    .flatMapMany(Flux::fromIterable)
                                    .map(LifecycleClientHandler.Counter::getMetaData)
                                    .flatMap(objectConsumer)
                                    .subscribe(res -> {
                                        if (!res && !hasFail.get()) {
                                            hasFail.set(true);
                                        }
                                    }, listController::onError, () -> {
                                        if (hasFail.get()) {
                                            listController.onNext(beginPrefixMark);
                                        } else if (listComplete.get()) {
                                            listController.onComplete();
                                        } else {
                                            // 保存进度
                                            Mono.just(findNum.addAndGet(size.get()) >= 10000 && StringUtils.isNotBlank(saveProgressKey))
                                                    .publishOn(SNAP_SCHEDULER)
                                                    .flatMap(needSave -> {
                                                        if (needSave) {
                                                            return Mono.create(sink -> {
                                                                RedisConnPool.getInstance().getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hset(saveProgressKey, "beginPrefix", nextBeginPrefix.get());
                                                                findNum.set(0);
                                                                sink.success(true);
                                                            });
                                                        }
                                                        return Mono.just(true);
                                                    }).subscribe(b -> {
                                                        listController.onNext(nextBeginPrefix.get());
                                                    });
                                        }
                                    });
                        }, listController::onError);
            }, bucketVnodeProcessor::onError, () -> {
                if (startIndex.incrementAndGet() < bucketVnodeList.size()) {
                    bucketVnodeProcessor.onNext(bucketVnodeList.get(startIndex.get()));
                } else {
                    bucketVnodeProcessor.onComplete();
                }
            });
            listController.onNext(curBeginPrefix);
        }, result::onError, () -> {
            result.onNext(true);
        });
        bucketVnodeProcessor.onNext(bucketVnodeList.get(startIndex.get()));
        return result;
    }
}
