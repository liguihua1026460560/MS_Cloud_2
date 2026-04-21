package com.macrosan.filesystem.tier;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.ECUtils;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.utils.FsTierUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.NO_SUCH_BUCKET;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_OBJECT_FILE;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_ROLL_BACK_FILE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.filesystem.tier.FileTierMove.ONCE;
import static com.macrosan.filesystem.utils.FsTierUtils.FS_TIER_DEBUG;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.storage.StorageOperate.PoolType.CACHE;
import static com.macrosan.storage.aggregation.AggregateFileClient.putMq;
import static com.macrosan.storage.move.CacheMove.isEnableCacheAccessTimeFlush;

/**
 * @author DaiFengtao
 * @date 2026年03月17日 11:04
 */
@Log4j2
public class FileTierRunner {
    private static final Logger delLogger = LogManager.getLogger("DeleteObjLog.FileSystemTierRunner");
    MonoProcessor<Boolean> res = MonoProcessor.create();
    Queue<Tuple2<String, String>> queue = new ConcurrentLinkedQueue<>();
    AtomicBoolean scanEnd = new AtomicBoolean(false);
    AtomicLong queueSize = new AtomicLong();
    FileTierMove runner;
    MSRocksDB mqDB;
    final MSRocksIterator iterator;

    private final AtomicLong total = new AtomicLong();
    private final AtomicLong error = new AtomicLong();
    private final AtomicLong normal = new AtomicLong();


    public FileTierRunner(MSRocksDB mqDB, FileTierMove runner) {
        this.runner = runner;
        this.mqDB = mqDB;
        this.iterator = mqDB.newIterator();
        iterator.seek(ROCKS_CACHE_BACK_STORE_KEY.getBytes());
        getSomeTask();
    }

    public long getTotal() {
        return total.get();
    }

    public long getError() {
        return error.get();
    }

    void run() {
        AtomicBoolean decrement = new AtomicBoolean(false);
        try {
            Tuple2<String, String> task = queue.poll();
            if (task != null && FS_TIER_DEBUG) {
                log.info("task = {}", task);
            }
            if (null == task) {
                if (scanEnd.get()) {
                    if (!tryEnd()) {
                        FsUtils.fsExecutor.schedule(this::run, 100, TimeUnit.MILLISECONDS);
                    }
                } else {
                    getSomeTask();
                    FsUtils.fsExecutor.submit(this::run);
                }
            } else {
                decrement.set(true);
                runTask(task.var1, task.var2)
                        .timeout(Duration.ofMinutes(15))
                        .doFinally(s -> {
                            normal.incrementAndGet();
                            queueSize.decrementAndGet();
                            decrement.set(false);
                            FsUtils.fsExecutor.submit(this::run);
                        })
                        .subscribe(t -> {
                        }, e -> {
                            error.incrementAndGet();
                            log.error("", e);
                        });
            }
        } catch (Exception e) {
            error.incrementAndGet();
            if (decrement.get()) {
                queueSize.decrementAndGet();
            }
            if (e instanceof MsException && ((MsException) e).getErrCode() == NO_SUCH_BUCKET) {

            } else {
                log.error("run task error", e);
            }
            FsUtils.fsExecutor.submit(this::run);
        }

    }

    public Mono<Boolean> runTask(String taskKey, String v) {
        try {
            if (FS_TIER_DEBUG) {
                log.info("start run task key: {},v:{}", taskKey, v);
            }
            int retryNum;
            String value;
            StoragePool[] cachePool = new StoragePool[]{null};
            boolean[] isMoveData = new boolean[]{true};
            long[] fileOffset = new long[1];
            long[] fileSize = new long[1];
            String[] stamp = new String[1];
            v = handleTaskValue(v, fileOffset, fileSize, stamp);

            String[] split = taskKey.split("_");
            String storage = split[1];
            long nodeId = Long.parseLong(split[2]);
            String bucket = split[3];
            if (v.startsWith(ROCKS_LATEST_KEY)) {
                retryNum = Integer.parseInt(v.substring(ROCKS_LATEST_KEY.length(), ROCKS_LATEST_KEY.length() + 1));
                value = v.substring(ROCKS_LATEST_KEY.length() + 1);
            } else if (v.startsWith(ROCKS_FILE_META_PREFIX)) {
                int end = v.substring(ROCKS_FILE_META_PREFIX.length()).indexOf(ROCKS_FILE_META_PREFIX)
                        + ROCKS_FILE_META_PREFIX.length();
                String pool = v.substring(ROCKS_FILE_META_PREFIX.length(), end);
                cachePool[0] = StoragePoolFactory.getStoragePool(pool, bucket);
                value = v.substring(end + ROCKS_FILE_META_PREFIX.length());
                retryNum = 10;
            } else {
                retryNum = 10;
                value = v;
            }

            StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucket);
            String metaVnode = metaPool.getBucketVnodeId(bucket);
            if (cachePool[0] == null) {
                StorageOperate operate = new StorageOperate(CACHE, "", fileSize[0]);
                cachePool[0] = StoragePoolFactory.getStoragePool(operate, bucket);
            } else {
                isMoveData[0] = false;
            }
            return move(taskKey, value, cachePool, isMoveData, bucket, retryNum, metaVnode, storage, nodeId, fileOffset[0], fileSize[0], stamp[0]);
        } catch (Exception e) {
            log.error("runTask error key: {},v:{}", taskKey, v, e);
            return Mono.just(false);
        }
    }

    private static String handleTaskValue(String v, long[] fileOffset, long[] fileSize, String[] stamp) {
        if (v.contains(ROCKS_FILE_META_PREFIX) && !v.startsWith(ROCKS_FILE_META_PREFIX)) {
            String[] split = v.split(ROCKS_FILE_META_PREFIX);
            v = split[0];
            fileOffset[0] = Long.parseLong(split[1]);
            fileSize[0] = Long.parseLong(split[2]);
            stamp[0] = split[3];
        }
        return v;
    }

    private synchronized void getSomeTask() {
        synchronized (iterator) {
            while (!scanEnd.get() && iterator.isValid()) {
                String key = new String(iterator.key());
                if (FS_TIER_DEBUG) {
                    log.info("getSomeTask key: {},v:{}", key, iterator.value());
                }
                if (!key.startsWith(ROCKS_CACHE_BACK_STORE_KEY)) {
                    scanEnd.set(true);
                } else {
                    String value = new String(iterator.value());
                    iterator.next();
                    if (!value.startsWith(ROCKS_OBJ_META_DELETE_MARKER)) {
                        queue.offer(new Tuple2<>(key, value));
                        total.incrementAndGet();
                        if (queueSize.incrementAndGet() > 2000) {
                            return;
                        }
                        if (total.get() >= ONCE) {
                            scanEnd.set(true);
                            return;
                        }
                    }
                }
            }

            if (!scanEnd.get() && !iterator.isValid()) {
                scanEnd.set(true);
            }
        }
    }

    private synchronized boolean tryEnd() {
        synchronized (iterator) {
            if (scanEnd.get() && queue.isEmpty() && queueSize.get() == 0L) {
                iterator.close();
                res.onNext(true);
                return true;
            }
            return false;
        }
    }

    /**
     * return Tuple2<fileOffset, fileSize>
     */
    public static Mono<Tuple2<Long, Long>> searchInodeData(String value, List<Inode.InodeData> list, long fileOffset, long fileSize, String storage) {
        //没有offset信息 从所有元数据中搜索fileOffset和fileSize返回
        if ((fileOffset == 0 && fileSize == 0)) {
            List<Mono<Tuple2<Long, Long>>> chunkRes = new LinkedList<>();
            long curOffset = 0L;

            for (Inode.InodeData inodeData : list) {
                if (inodeData.fileName.replace("/split/", "").equals(value) && storage.equals(inodeData.getStorage())) {
                    return Mono.just(new Tuple2<>(curOffset, inodeData.size + inodeData.size0));
                } else if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                    long chunkOff = curOffset - inodeData.getOffset();
                    Mono<Tuple2<Long, Long>> res = Mono.just(true)
                            .flatMap(b -> Node.getInstance().getChunk(inodeData.fileName))
                            .flatMap(chunk -> searchInodeData(value, chunk.getChunkList(), 0L, 0L, storage))
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

            for (Inode.InodeData inodeData : list) {
                long curEnd = curOffset + inodeData.getSize();
                if (curOffset >= fileOffset + fileSize) {
                    break;
                } else if (fileOffset < curEnd) {
                    if (inodeData.fileName.replace("/split/", "").equals(value) && storage.equals(inodeData.getStorage())) {
                        return Mono.just(new Tuple2<>(fileOffset, fileSize));
                    } else if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                        long chunkOff = fileOffset - curOffset + inodeData.getOffset();
                        long chunkSize = fileSize;
                        Mono<Tuple2<Long, Long>> res = Mono.just(true)
                                .flatMap(b -> Node.getInstance().getChunk(inodeData.fileName))
                                .flatMap(chunk -> searchInodeData(value, chunk.getChunkList(), chunkOff, chunkSize, storage));

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

    public Mono<Boolean> move(String taskKey, String value, StoragePool[] cachePool, boolean[] isMoveData,
                              String bucekt, int retryNum, String vnode, String storage, long nodeId, long fileOffset, long fileSize, String stamp) {
        Node node = Node.getInstance();
        if (node == null) {
            return Mono.just(false);
        }

        AtomicBoolean isErrorInode = new AtomicBoolean(false);
        AtomicBoolean fileExist = new AtomicBoolean(false);
        AtomicBoolean updateProcess = new AtomicBoolean(false);

        StoragePool oldPool = StoragePoolFactory.getStoragePool(storage, bucekt);
        if (oldPool == null) {
            return Mono.just(true);
        }

        return node.getInode(bucekt, nodeId)
                .flatMap(inode -> {
                    if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                        isErrorInode.set(true);
                        return Mono.just(false);
                    }


                    return searchInodeData(value, inode.getInodeData(), fileOffset, fileSize, storage)
                            .flatMap(t -> {
                                if (t.var2 == -1) {
                                    return Mono.just(false);
                                }

                                fileExist.set(true);
                                Mono<Boolean> moveRes;
                                if (isMoveData[0]) {
                                    moveRes = moveFile(oldPool, cachePool[0], inode, value, vnode, fileSize, fileOffset, stamp);
                                } else {
                                    moveRes = Mono.just(true);
                                }

                                return moveRes
                                        .flatMap(b -> {
                                            if (b) {
                                                putMq(taskKey, ROCKS_FILE_META_PREFIX + cachePool[0].getVnodePrefix() + ROCKS_FILE_META_PREFIX + value);
                                                return updateInodeData(inode, value, cachePool[0], t.var1, t.var2, updateProcess)
                                                        .doOnNext(b0 -> {
                                                            if (b0) {
                                                                //文件不处理删除配置，默认是直接删除
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
                                delLogger.info("{} not in inode. next delete in {},nodeId:{},offset:{},size:{}", value, storage, nodeId, fileOffset, fileSize);
                                runner.putMq(taskKey, ROCKS_OBJ_META_DELETE_MARKER + value);
                            } else {
                                String prefix = ROCKS_LATEST_KEY + (retryNum - 1);
                                String v = "";
                                if (retryNum - 1 == 1) {
                                    v = FsTierUtils.buildTierRecordValue(prefix + value, 0, 0, stamp );
                                } else {
                                    v = FsTierUtils.buildTierRecordValue(prefix + value, fileOffset, fileSize, stamp );
                                }
                                runner.putMq(taskKey, v);
                            }
                        } else {
                            //处理升级，升级期间不删除
                            String prefix = ROCKS_LATEST_KEY + (retryNum);
                            String v = FsTierUtils.buildTierRecordValue(prefix + value, fileOffset, fileSize, stamp);
                            runner.putMq(taskKey, v);
                        }
                    }
                });
    }

    public static Mono<Boolean> moveFile(StoragePool oldPool, StoragePool cachePool, Inode inode, String fileName, String vnode, long fileSize, long fileOffset, String stamp) {
        List<Tuple3<String, String, String>> getNodeList = oldPool.mapToNodeInfo(oldPool.getObjectVnodeId(fileName)).block();

        Encoder ecEncodeHandler = cachePool.getEncoder();
        UnicastProcessor<Long> streamController = UnicastProcessor.create();

        int ontPutBytes = cachePool.getK() * cachePool.getPackageSize();
        UnicastProcessor<Tuple2<Integer, Integer>> next = UnicastProcessor.create(Queues.<Tuple2<Integer, Integer>>unboundedMultiproducer().get());
        AtomicInteger exceptGetNum = new AtomicInteger(oldPool.getK());
        AtomicInteger waitEncodeBytes = new AtomicInteger(0);
        AtomicInteger exceptPutNum = new AtomicInteger(0);
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(cachePool.getVnodePrefix());

        next.subscribe(t -> {
            if (t.var1 > 0) {
                exceptGetNum.decrementAndGet();
                waitEncodeBytes.addAndGet(t.var2);
                int n = waitEncodeBytes.get() / ontPutBytes; //原始数据均分至新盘存k个
                exceptPutNum.addAndGet(n);
                waitEncodeBytes.addAndGet(-n * ontPutBytes);
            } else {
                exceptPutNum.decrementAndGet();//一个块上传完成减1
            }

            while (exceptPutNum.get() == 0 && exceptGetNum.get() <= 0) {
                for (int j = 0; j < oldPool.getK(); j++) {
                    streamController.onNext(-1L);
                }
                exceptGetNum.addAndGet(oldPool.getK());//继续get数据
            }
        });

        ECUtils.getObject(oldPool, fileName, false, 0, fileSize - 1, fileSize,
                        getNodeList, streamController, null, null)
                .doOnError(e -> {
                    for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                        ecEncodeHandler.data()[i].onError(e);
                    }
                })
                .doOnComplete(ecEncodeHandler::complete)
                .subscribe(bytes -> {
                    next.onNext(new Tuple2<>(1, bytes.length));
                    ecEncodeHandler.put(bytes);
//                    streamController.onNext(1L);
                });

        List<Tuple3<String, String, String>> putNodeList = cachePool.mapToNodeInfo(cachePool.getObjectVnodeId(fileName)).block();
        String metaKey = Inode.getKey(vnode, inode.getBucket(), inode.getNodeId());
        List<UnicastProcessor<Payload>> publisher = putNodeList.stream()
                .map(t -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("fileName", fileName)
                            .put("lun", t.var2)
                            .put("vnode", t.var3)
                            .put("compression", cachePool.getCompression())
                            .put("fileOffset", String.valueOf(fileOffset))
                            .put("metaKey", metaKey)
                            .put("bucket", inode.getBucket())
                            .put("object", inode.getObjName())
                            .put("versionId", inode.getVersionId())
                            .put("storage", cachePool.getVnodePrefix())
                            .put("fileSize", String .valueOf(fileSize));


                    if (isEnableCacheAccessTimeFlush(cachePool)) {
                        msg.put("lastAccessStamp", stamp);//将对象的get访问时间作为缓存池中该数据的访问记录
                    }
                    //这里需要补充上传文件需要的属性参数，比如增加访问记录属性并在缓存盘上条件访问记录
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

        AtomicInteger putNum = new AtomicInteger();

        responseInfo.responses.doOnNext(s -> {
            if (ERROR.equals(s.var2)) {
                errorChunksList.add(s.var1);
            }

            if (putNum.incrementAndGet() == putNodeList.size()) {
                next.onNext(new Tuple2<>(-1, 0));
                putNum.set(errorChunksList.size());
            }

        }).doOnComplete(() -> {
            if (responseInfo.successNum == cachePool.getK() + cachePool.getM()) {
                res.onNext(true);
            } else if (responseInfo.successNum >= cachePool.getK()) {
                res.onNext(true);

                //订阅数据修复消息的发出。b表示k+m个元数据是否至少写上了一个。
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("errorChunksList", Json.encode(new ArrayList<>(errorChunksList)))
                        .put("storage", cachePool.getVnodePrefix())
                        .put("bucket", inode.getBucket())
                        .put("object", metaKey)
                        .put("fileName", fileName)
                        .put("versionId", inode.getVersionId())
                        .put("fileSize", String.valueOf(ecEncodeHandler.size()))
                        .put("poolQueueTag", poolQueueTag)
                        .put("stamp", stamp)
                        .put("lastAccessStamp", stamp)//写回缓存池的数据需要增加写入访问记录
                        .put("fileOffset", String.valueOf(fileOffset));
                publishEcError(responseInfo.res, putNodeList, errorMsg, ERROR_PUT_OBJECT_FILE);
            } else {
                res.onNext(false);
                //响应成功数量达不到k,发布回退消息，删掉成功的节点上的文件
                SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                errorMsg.put("bucket", inode.getBucket());
                errorMsg.put("object", inode.getObjName());
                errorMsg.put("fileName", fileName);
                errorMsg.put("storage", cachePool.getVnodePrefix());
                errorMsg.put("poolQueueTag", poolQueueTag);
                publishEcError(responseInfo.res, putNodeList, errorMsg, ERROR_ROLL_BACK_FILE);
            }
        }).doOnError(e -> log.error("", e)).subscribe();

        return res;
    }


    public static Mono<Boolean> updateInodeData(Inode inode, String fileName, StoragePool targetPool, long fileOffset, long fileSize, AtomicBoolean updateProcess) {
        Inode.InodeData tmp = new Inode.InodeData()
                .setFileName(fileName)
                .setStorage(targetPool.getVnodePrefix())
                .setSize(fileSize);

        return Node.getInstance().updateInodeData2(inode.getBucket(), inode.getNodeId(), fileOffset, tmp, "", "oldInodeData")
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