package com.macrosan.storage.move;

import com.macrosan.ec.ECUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.Inode.InodeData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.storage.coder.Limiter;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.quota.QuotaRecorder;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_OBJECT_FILE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;

@Log4j2
@AllArgsConstructor
public class FileSystemRunner {
    private static final Logger delLogger = LogManager.getLogger("DeleteObjLog.FileSystemRunner");
    StoragePool pool;
    TaskRunner runner;

    private Mono<Tuple2<Long, Long>> searchInodeData(String value, List<InodeData> list, long fileOffset, long fileSize) {
        return searchInodeData(value, list, fileOffset, fileSize, pool);
    }

    /**
     * return Tuple2<fileOffset, fileSize>
     */
    public static Mono<Tuple2<Long, Long>> searchInodeData(String value, List<InodeData> list, long fileOffset, long fileSize, StoragePool pool) {
        //没有offset信息 从所有元数据中搜索fileOffset和fileSize返回
        if (fileOffset == 0 && fileSize == 0) {
            List<Mono<Tuple2<Long, Long>>> chunkRes = new LinkedList<>();
            long curOffset = 0L;

            for (InodeData inodeData : list) {
                if (inodeData.fileName.replace("/split/", "").equals(value) && pool.getVnodePrefix().equals(inodeData.getStorage())) {
                    return Mono.just(new Tuple2<>(curOffset, inodeData.size + inodeData.size0));
                } else if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                    long chunkOff = curOffset - inodeData.getOffset();
                    Mono<Tuple2<Long, Long>> res = Mono.just(true)
                            .flatMap(b -> Node.getInstance().getChunk(inodeData.fileName))
                            .flatMap(chunk -> searchInodeData(value, chunk.getChunkList(), 0L, 0L, pool))
                            .map(t -> {
                                if (t.var2 != -1) {
                                    t.var1 += chunkOff;
                                    return t;
                                } else {
                                    return t;
                                }
                            });

                    chunkRes.add(res);
                }

                curOffset += inodeData.size;
            }

            if (chunkRes.isEmpty()) {
                return Mono.just(new Tuple2<>(-1L, -1L));
            } else {
                return Flux.merge(Flux.fromStream(chunkRes.stream()), 1, 1)
                        .collectList()
                        .map(l -> {
                            long minStart = -1;
                            long maxEnd = -1;
                            for (Tuple2<Long, Long> t : l) {
                                if (t.var2 != -1) {
                                    long start = t.var1;
                                    long end = t.var1 + t.var2;

                                    if (minStart == -1 || start < minStart) {
                                        minStart = start;
                                    }

                                    if (maxEnd == -1 || end > maxEnd) {
                                        maxEnd = end;
                                    }
                                }
                            }

                            if (maxEnd == -1) {
                                return new Tuple2<>(-1L, -1L);
                            } else {
                                return new Tuple2<>(minStart, maxEnd - minStart);
                            }
                        });
            }
        } else {
            //有fileOffset和fileSize，只要找到一个元数据就可以返回，开始下刷
            long curOffset = 0L;

            List<Mono<Tuple2<Long, Long>>> chunkRes = new LinkedList<>();

            for (InodeData inodeData : list) {
                long curEnd = curOffset + inodeData.getSize();

                if (curOffset >= fileOffset + fileSize) {
                    break;
                } else if (fileOffset < curEnd) {
                    if (inodeData.fileName.replace("/split/", "").equals(value) && pool.getVnodePrefix().equals(inodeData.getStorage())) {
                        return Mono.just(new Tuple2<>(fileOffset, fileSize));
                    } else if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                        long chunkOff = fileOffset - curOffset + inodeData.getOffset();
                        long chunkSize = fileSize;
                        Mono<Tuple2<Long, Long>> res = Mono.just(true)
                                .flatMap(b -> Node.getInstance().getChunk(inodeData.fileName))
                                .flatMap(chunk -> searchInodeData(value, chunk.getChunkList(), chunkOff, chunkSize, pool));

                        chunkRes.add(res);
                    }
                }

                curOffset = curEnd;
            }

            if (chunkRes.isEmpty()) {
                return Mono.just(new Tuple2<>(-1L, -1L));
            } else {
                return Flux.merge(Flux.fromStream(chunkRes.stream()), 1, 1)
                        .collectList()
                        .map(l -> {
                            if (l.stream().anyMatch(t -> t.var2 != -1)) {
                                return new Tuple2<>(fileOffset, fileSize);
                            }

                            return new Tuple2<>(-1L, -1L);
                        });
            }
        }
    }

    public Mono<Boolean> fileMove(String taskKey, String value, StoragePool[] dataPool, boolean[] isMoveData,
                                  String bucekt, int retryNum, String vnode, long nodeId, long fileOffset, long fileSize) {
        Node node = Node.getInstance();
        if (node == null) {
            return Mono.just(false);
        }

        AtomicBoolean isErrorInode = new AtomicBoolean(false);
        AtomicBoolean fileExist = new AtomicBoolean(false);
        AtomicBoolean updateProcess = new AtomicBoolean(false);

        return node.getInode(bucekt, nodeId)
                .flatMap(inode -> {
                    if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                        isErrorInode.set(true);
                        return Mono.just(false);
                    }

                    return searchInodeData(value, inode.getInodeData(), fileOffset, fileSize)
                            .flatMap(t -> {
                                if (t.var2 == -1) {
                                    return Mono.just(false);
                                }

                                fileExist.set(true);
                                Mono<Boolean> moveRes;
                                if (isMoveData[0]) {
                                    moveRes = moveFile(inode, value, t.var2, dataPool[0], vnode, fileOffset);
                                } else {
                                    moveRes = Mono.just(true);
                                }

                                return moveRes.flatMap(b -> {
                                    if (b) {
                                        return updateInodeData(inode, value, dataPool[0], t.var1, t.var2, updateProcess)
                                                .doOnNext(b0 -> {
                                                    if (b0) {
                                                        runner.putMq(taskKey, ROCKS_OBJ_META_DELETE_MARKER + value);
                                                    }
                                                });
                                    } else {
                                        return Mono.just(false);
                                    }
                                });

                            });
                })
                .doOnNext(b -> {
                    if (!fileExist.get() && !b && !isErrorInode.get()) {
                        if (!updateProcess.get()) {
                            if (retryNum <= 0) {
                                delLogger.info("{} not in inode. next delete in {},nodeId:{},offset:{},size:{}", value, pool.getVnodePrefix(),nodeId,fileOffset,fileSize);
                                runner.putMq(taskKey, ROCKS_OBJ_META_DELETE_MARKER + value);
                            } else {
                                String prefix = ROCKS_LATEST_KEY + (retryNum - 1);
                                runner.putMq(taskKey, prefix + value + "#" + fileOffset + "#" + fileSize);
                            }
                        } else {
                            //处理升级，升级期间不删除
                            String prefix = ROCKS_LATEST_KEY + (retryNum);
                            runner.putMq(taskKey, prefix + value + "#" + fileOffset + "#" + fileSize);
                        }
                    }
                });
    }

    private Mono<Boolean> moveFile(Inode inode, String fileName, long fileSize, StoragePool targetPool, String vnode, long fileOffset) {
        return moveFile(inode, fileName, fileSize, targetPool, vnode, fileOffset, pool);
    }

    public static Mono<Boolean> moveFile(Inode inode, String fileName, long fileSize, StoragePool targetPool, String vnode, long fileOffset, StoragePool pool) {
        List<Tuple3<String, String, String>> getNodeList = pool.mapToNodeInfo(pool.getObjectVnodeId(fileName)).block();

        Encoder ecEncodeHandler = targetPool.getEncoder();
        UnicastProcessor<Long> streamController = UnicastProcessor.create();


        ECUtils.getObject(pool, fileName, false, 0, fileSize - 1, fileSize,
                        getNodeList, streamController, null, null)
                .doOnError(e -> {
                    for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                        ecEncodeHandler.data()[i].onError(e);
                    }
                })
                .doOnComplete(ecEncodeHandler::complete)
                .subscribe(bytes -> {
                    ecEncodeHandler.put(bytes);
                    streamController.onNext(1L);
                });


        List<Tuple3<String, String, String>> putNodeList = targetPool.mapToNodeInfo(targetPool.getObjectVnodeId(fileName)).block();
        String metaKey = Inode.getKey(vnode, inode.getBucket(), inode.getNodeId());


        List<UnicastProcessor<Payload>> publisher = putNodeList.stream()
                .map(t -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("fileName", fileName)
                            .put("lun", t.var2)
                            .put("vnode", t.var3)
                            .put("compression", targetPool.getCompression())
                            .put("metaKey", metaKey)
                            .put("fileOffset", String.valueOf(fileOffset));

                    return msg;
                })
                .map(msg0 -> {
                    UnicastProcessor<Payload> processor = UnicastProcessor.create();
                    processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PUT_OBJECT.name()));
                    return processor;
                })
                .collect(Collectors.toList());

        for (int i = 0; i < publisher.size(); i++) {
            int index = i;
            ecEncodeHandler.data()[index].subscribe(bytes -> {
                        publisher.get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                    },
                    e -> {
                        log.error("", e);
                        publisher.get(index).onNext(DefaultPayload.create("put file error", ERROR.name()));
                        publisher.get(index).onComplete();
                    },
                    () -> {
                        publisher.get(index).onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                        publisher.get(index).onComplete();
                    });
        }

        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, putNodeList);

        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<Integer> errorChunksList = new ArrayList<>();
        Limiter limiter = new Limiter(new TaskRunner.MoveMsRequest(), putNodeList.size(), targetPool.getK());

        responseInfo.responses.doOnNext(s -> {
            if (s.var2.equals(ERROR)) {
                errorChunksList.add(s.var1);
                limiter.request(s.var1, Long.MAX_VALUE);
            } else {
                limiter.request(s.var1, Long.MAX_VALUE);
            }

        }).doOnComplete(() -> {
            if (responseInfo.successNum == targetPool.getK() + targetPool.getM()) {
                QuotaRecorder.addCheckBucket(inode.getBucket());
                res.onNext(true);
            } else if (responseInfo.successNum >= targetPool.getK()) {
                QuotaRecorder.addCheckBucket(inode.getBucket());
                res.onNext(true);
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(targetPool.getVnodePrefix());
                //订阅数据修复消息的发出。b表示k+m个元数据是否至少写上了一个。
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("errorChunksList", Json.encode(errorChunksList))
                        .put("storage", targetPool.getVnodePrefix())
                        .put("bucket", inode.getBucket())
                        .put("object", metaKey)
                        .put("fileName", fileName)
                        .put("versionId", inode.getVersionId())
                        .put("fileSize", String.valueOf(ecEncodeHandler.size()))
                        .put("poolQueueTag", poolQueueTag)
                        .put("fileOffset", String.valueOf(fileOffset));


                for (int index : errorChunksList) {
                    if (targetPool.getK() + targetPool.getM() <= index) {
                        log.error("publish error {} {}", targetPool, errorChunksList);
                    }
                }
                publishEcError(responseInfo.res, putNodeList, errorMsg, ERROR_PUT_OBJECT_FILE);
            } else {
                res.onNext(false);
            }
        }).doOnError(e -> log.error("", e)).subscribe();

        return res;
    }

    public static Mono<Boolean> updateInodeData(Inode inode, String fileName, StoragePool targetPool, long fileOffset, long fileSize, AtomicBoolean updateProcess) {
        InodeData tmp = new InodeData()
                .setFileName(fileName)
                .setStorage(targetPool.getVnodePrefix())
                .setSize(fileSize);

        return Node.getInstance().updateInodeData(inode.getBucket(), inode.getNodeId(), fileOffset, tmp, "", "oldInodeData")
                .flatMap(resInode -> {
                    if (resInode.getLinkN() == Inode.UPDATE_PROCESS_INODE.getLinkN()) {
                        updateProcess.set(true);
                        return Mono.just(false);
                    }

                    if (!InodeUtils.isError(resInode) && null != resInode.getUpdateInodeDataStatus() && resInode.getUpdateInodeDataStatus().get(fileName)) {
                        return Mono.just(true);
                    }
                    return Mono.just(false);
                });
    }
}
