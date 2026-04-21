package com.macrosan.filesystem.cache;

import com.macrosan.ec.Utils;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.md5.Digest;
import com.macrosan.utils.msutils.md5.Md5Digest;
import io.vertx.core.json.Json;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomStringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.File;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.ES_ON;
import static com.macrosan.constants.SysConstants.ES_SWITCH;
import static com.macrosan.filesystem.cache.BytesPoolNode.MAX_BYTE_POOL_SIZE;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.filesystem.utils.InodeUtils.isError;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;

@Slf4j
@ToString(exclude = "dataPool")
public class WriteCacheNode {
    public static final int MAX_CACHE_SIZE = (128 << 20);
    public static final int MAX_TRUNK_CACHE_SIZE = (16 << 20);
    public static AtomicInteger AVAILABLE_CACHE_NUM = new AtomicInteger(MAX_BYTE_POOL_SIZE);
    private static final Map<Long, WriteCacheNode> map = new ConcurrentHashMap<>();
    private static final AtomicBoolean FLUSHING = new AtomicBoolean(false);

    private AtomicInteger cacheSize = new AtomicInteger(0);
    private String bucket;
    private long nodeId;
    private boolean sync;
    private StoragePool dataPool;
    private Map<Long, ByteCacheNode> cacheData = new ConcurrentHashMap<>();
    // 引用计数：当get该NFSCache时增加1，当该次NFSCache完成数据的write时减1
    private AtomicInteger writeNum = new AtomicInteger(0);

    public static void printWriteCache() {
        log.info("writeCacheNode\n{}", map.toString());
    }

    public WriteCacheNode(String bucket, long nodeId, boolean sync, String storage) {
        this.sync = sync;
        this.bucket = bucket;
        this.nodeId = nodeId;
        dataPool = StoragePoolFactory.getStoragePool(storage, bucket);
    }

    public static int getAvailableCacheNum() {
        return AVAILABLE_CACHE_NUM.get();
    }

    public static Mono<WriteCacheNode> getCache(String bucket, long nodeId, int flags, String storage) {
        boolean sync = flags != 0;
        return Mono.just(1L)
                .map(l -> {
                    if (sync) {
                        return new WriteCacheNode(bucket, nodeId, sync, storage);
                    }
                    return map.compute(nodeId, (k, v) -> {
                        if (v == null) {
                            v = new WriteCacheNode(bucket, nodeId, sync, storage);
                        }
                        v.sync = sync;
                        v.writeNum.incrementAndGet();
                        return v;
                    });
                });
    }

    public long getWriteNum() {
        return writeNum.get();
    }

    public static void removeCache(long nodeId) {
        map.compute(nodeId, (k, v) -> {
            if (v != null) {
                v.cacheData.forEach((key, byteCacheNode) -> {
                    BytesPoolNode.release(byteCacheNode.data);
                });
            }
            return null;
        });
    }

    /**
     *
     * @param bytes
     * @param curOffset
     * @return 1为成功，0为一块写满16mb需要下刷，-1为分配失败
     */
    public Mono<Integer> writeCache(byte[] bytes, long curOffset) {
        try {
            long key = curOffset / MAX_TRUNK_CACHE_SIZE;
            AtomicBoolean needFlush = new AtomicBoolean(false);

            byte[] firstBlock;
            byte[] secondBlock;

            int firstOffset = (int) (curOffset % MAX_TRUNK_CACHE_SIZE);
            if ((firstOffset + bytes.length) > MAX_TRUNK_CACHE_SIZE) {
                int firstBlockSize = MAX_TRUNK_CACHE_SIZE - firstOffset;
                firstBlock = new byte[firstBlockSize];
                System.arraycopy(bytes, 0, firstBlock, 0, firstBlockSize);
                secondBlock = new byte[bytes.length - firstBlockSize];
                System.arraycopy(bytes, firstBlockSize, secondBlock, 0, secondBlock.length);
            } else {
                firstBlock = bytes;
                secondBlock = null;
            }

            try {
                AtomicBoolean directWrite = new AtomicBoolean(false);

                cacheData.compute(key, (k, v) -> {
                    if (v == null) {
                        v = ByteCacheNode.alloc(curOffset, 0);
                        if (v == null) {
                            directWrite.set(true);
                            return null;
                        }
                    }
                    int lastSize = v.size;
                    v.updateOffset(curOffset);
                    v.put(curOffset, firstBlock);
                    int updateSize = v.size - lastSize;
                    cacheSize.addAndGet(updateSize);


                    if (v.size == MAX_TRUNK_CACHE_SIZE) {
                        needFlush.set(true);
                    }
                    return v;
                });

                if (directWrite.get()) {
                    //第一块分配失败
                    return Mono.just(-1);
                }

                //上一个trunk写完了
                if (secondBlock != null) {
                    cacheData.compute(key + 1, (k, v) -> {
                        long trunkOffset = k * MAX_TRUNK_CACHE_SIZE;
                        if (v == null) {
                            v = ByteCacheNode.alloc(trunkOffset, 0);
                            if (v == null) {
                                directWrite.set(true);
                                return null;
                            }
                        }
                        int lastSize = v.size;
                        v.updateOffset(trunkOffset);
                        v.put(trunkOffset, secondBlock);
                        int updateSize = v.size - lastSize;
                        cacheSize.addAndGet(updateSize);
                        if (v.size == MAX_TRUNK_CACHE_SIZE) {
                            needFlush.set(true);
                        }
                        return v;
                    });

                    if (directWrite.get()) {
                        //第一块成功，第二块分配失败
                        return Mono.just(-1);
                    }
                }
            } catch (Exception e) {
                log.error("", e);
                return Mono.just(-1);
            }

            // write 不需要等待flush
            if (needFlush.get()) {
                return Mono.just(0);
            }

            return Mono.just(1);

        } catch (Exception e) {
            log.error("", e);
            return Mono.just(-1);
        } finally {
            writeNum.decrementAndGet();
        }
    }

    public Mono<Boolean> commit(Inode inode, long offset, int count, long end, boolean writeFlush, boolean write) {
        if (write) {
            return FSQuotaUtils.existQuotaInfo(inode.getBucket(), inode.getObjName(), System.currentTimeMillis(), inode.getUid(), inode.getGid())
                    .flatMap(t2 -> {
                        if (t2.var1) {
                            inode.getXAttrMap().put(QUOTA_KEY, Json.encode(t2.var2));
                        }
                        return commit0(inode, offset, count, end, writeFlush, true);
                    });
        } else {
            return commit0(inode, offset, count, end, writeFlush, false);
        }
    }

    private static class CommitRes {
        AtomicBoolean prepare = new AtomicBoolean(false);
        Throwable error;
        boolean value;
        MonoProcessor<Boolean> res = MonoProcessor.create();


        private void complete(boolean value, Throwable error) {
            this.value = value;
            this.error = error;
            prepare.set(true);
        }
    }

    private final LinkedList<WriteCacheNode.CommitRes> commitQueue = new LinkedList<>();

    private Mono<Boolean> commit0(Inode inode, long offset, int count, long end, boolean writeFlush, boolean write) {
        WriteCacheNode.CommitRes res = new WriteCacheNode.CommitRes();
        List<ByteCacheNode> needPut;

        //保证 返回响应的顺序为 进入对列的顺序
        synchronized (commitQueue) {
            if (writeFlush) {
                needPut = getWriteFlushCaches();
            } else if (offset == 0L && count == 0) {//下刷全部缓存
                needPut = getAllCaches();
            } else {
                needPut = getRangeCaches(offset, count);
            }

            commitQueue.add(res);
        }

        if (needPut.isEmpty()) {
            res.complete(true, null);
            commitEnd();
        } else {
            FsUtils.fsExecutor.submit(() -> {
                try {
                    flushCaches(inode, needPut, end, write).subscribe(b -> {
                        res.complete(b, null);
                        commitEnd();
                    });
                } catch (Exception e) {
                    log.error("", e);
                    res.complete(false, e);
                    commitEnd();
                }
            });
        }

        return res.res.doFinally(s -> {
            map.computeIfPresent(inode.getNodeId(), (k, v) -> {
                if (writeNum.decrementAndGet() == 0 && cacheSize.get() == 0) {
                    return null;
                } else {
                    return v;
                }
            });
        });
    }

    private void commitEnd() {
        synchronized (commitQueue) {
            while (!commitQueue.isEmpty() && commitQueue.peekFirst().prepare.get()) {
                WriteCacheNode.CommitRes res = commitQueue.pollFirst();
                if (res.error != null) {
                    try {
                        res.res.onError(res.error);
                    } catch (Exception e) {

                    }
                } else {
                    res.res.onNext(res.value);
                }
            }
        }
    }

    //只获得特定range的ByteCacheNode
    private List<ByteCacheNode> getRangeCaches(long offset, int count) {
        List<ByteCacheNode> needPut = new LinkedList<>();
        long end = offset + count;
        //可以优化key的搜索条件不用遍历整个缓存
        for (long key : cacheData.keySet()) {
            cacheData.computeIfPresent(key, (k0, byteCacheNode) -> {
                long byteEnd = byteCacheNode.offset + byteCacheNode.size;
                if (byteCacheNode.offset <= offset) {
                    if (byteEnd > offset) {
                        needPut.add(byteCacheNode);
                        return null;
                    } else {
                        return byteCacheNode;
                    }
                } else if (byteCacheNode.offset < end) {
                    needPut.add(byteCacheNode);
                    return null;
                } else {
                    return byteCacheNode;
                }
            });
        }

        return needPut;
    }

    //write 触发的flush
    private List<ByteCacheNode> getWriteFlushCaches() {
        LinkedList<ByteCacheNode> needPut = new LinkedList<>();

        for (long key : cacheData.keySet()) {
            cacheData.computeIfPresent(key, (k, v) -> {
                //下刷已满的数据
                if (v.size >= MAX_TRUNK_CACHE_SIZE) {
                    needPut.add(v);
                    cacheSize.addAndGet(-v.size);
                    return null;
                } else if (v.size > (MAX_TRUNK_CACHE_SIZE / 2)) {
                    //超过MAX_CACHE_SIZE 的部分数据
                    if (cacheSize.get() > MAX_CACHE_SIZE) {
                        needPut.add(v);
                        cacheSize.addAndGet(-v.size);
                        return null;
                    }
                }

                return v;
            });
        }

        return needPut;
    }

    //获得尽可能多的ByteCacheNode
    private List<ByteCacheNode> getAllCaches() {
        LinkedList<ByteCacheNode> needPut = new LinkedList<>();

        for (long key : cacheData.keySet()) {
            cacheData.computeIfPresent(key, (k, v) -> {
                needPut.add(v);
                cacheSize.addAndGet(-v.size);
                return null;
            });
        }

        return needPut;
    }

    private Mono<Boolean> flushCaches(Inode inode, List<ByteCacheNode> needPut, long end, boolean isMaster) {
        return Flux.fromStream(needPut.stream())
                .flatMap(byteCacheNode -> flush(inode, byteCacheNode, end, isMaster))
                .doOnError(e -> log.error("", e))
                .collectList()
                .map(resList -> {
                    needPut.clear();
                    return !resList.contains(false);
                });
    }

    private Mono<Boolean> flush(Inode inode, ByteCacheNode byteCacheNode, long end, boolean write) {
        return Flux.fromArray(byteCacheNode.pages.toArray(new Tuple2[0]))
                .flatMap(t -> {
                    if (write) {
                        int tmpOffset = (int) ((long) t.var1 % MAX_TRUNK_CACHE_SIZE);

                        long offset = (long) t.var1;
                        int size = (int) t.var2;
                        if (offset >= end) {
                            return Mono.just(true);
                        }
                        if (offset + size > end) {
                            size = (int) (end - offset);
                        }
                        // 根据文件大小决定写入数据池还是缓存池
                        StorageOperate dataOperate = new StorageOperate(DATA, inode.getObjName(), size);
                        StoragePool dataPool = StoragePoolFactory.getStoragePool(dataOperate, inode.getBucket());
                        Encoder encoder = dataPool.getEncoder(size);
                        Digest digest = new Md5Digest();
                        if (size == MAX_TRUNK_CACHE_SIZE) {
                            encoder.put(byteCacheNode.data);
                            digest.update(byteCacheNode.data);
//                        byteCacheNode.data = null;
                        } else {
                            byte[] tmp = new byte[size];
                            System.arraycopy(byteCacheNode.data, tmpOffset, tmp, 0, size);
                            encoder.put(tmp);
                            digest.update(tmp);
                            tmp = null;
                        }
                        String md5 = Hex.encodeHexString(digest.digest());
                        return putObj(encoder, inode, (long) t.var1, size, md5, 1, dataPool)
                                .map(inode1 -> !isError(inode1));
                    } else {
                        return Mono.just(true);
                    }
                }).collectList().map(resList -> !resList.contains(false))
                .doOnNext(r -> {
                    BytesPoolNode.release(byteCacheNode.data);
                });
    }

    public static Mono<Inode> putObj(Encoder encoder0, Inode inode, long curOffset, int curSize, String md5, int updateTime, StoragePool dataPool) {
        String fileName = Utils.getObjFileName(dataPool, inode.getBucket(), inode.getObjName() + RandomStringUtils.randomAlphanumeric(4),
                RandomStringUtils.randomAlphanumeric(32)) + '/';
        String vnode = fileName.split(File.separator)[1].split("_")[0];
        List<Tuple3<String, String, String>> nodeList = dataPool.mapToNodeInfo(vnode).block();
        encoder0.complete();

        MonoProcessor<Boolean> rollBackProcessor = MonoProcessor.create();
        return FsUtils.putObj(dataPool, encoder0, fileName, nodeList, inode, curOffset, rollBackProcessor)
                .flatMap(b -> {
                    if (!b) {
                        log.error("put obj {} fail.", fileName);
                        return Mono.just(ERROR_INODE);
                    } else {
                        Inode.InodeData inodeData = new Inode.InodeData()
                                .setSize(curSize)
                                .setStorage(dataPool.getVnodePrefix())
                                .setOffset(0L)
                                .setEtag(md5)
                                .setFileName(fileName);
                        return Node.getInstance().updateInodeData(inode.getBucket(), inode.getNodeId(), curOffset, inodeData, "", "", updateTime, inode.getXAttrMap().get(QUOTA_KEY), inode.getObjName())
                                .flatMap(i -> {
                                    if (!isError(i) && !i.isDeleteMark()) {
                                        if (ES_ON.equals(NFSBucketInfo.getBucketInfo(i.getBucket()).get(ES_SWITCH))) {
                                            return EsMetaTask.putEsMeta(i).map(f -> i);
                                        }
                                    } else if (i.isDeleteMark()) {
                                        rollBackProcessor.onNext(b);
                                    }

                                    return Mono.just(i);
                                });
                    }
                });
    }

    private static final LinkedList<WriteCacheNode.CommitRes> flushAllQueue = new LinkedList<>();
    public Mono<Boolean> flushByteCache() {
        if (SMBHandler.runningDebug) {
            log.info("start flushByteCache");
        }
        WriteCacheNode.CommitRes res = new WriteCacheNode.CommitRes();

        boolean flush;
        synchronized (flushAllQueue) {
            flushAllQueue.add(res);
            flush = FLUSHING.compareAndSet(false, true);
        }

        if (flush) {
            FsUtils.fsExecutor.submit(() -> {
                try {
                    Flux.fromStream(map.keySet().stream())
                            .flatMap(inodeId -> {
                                WriteCacheNode writeCacheNode = map.get(inodeId);
                                if (writeCacheNode != null) {
                                    long cacheSize = writeCacheNode.cacheSize.get();
                                    if (SMBHandler.runningDebug) {
                                        log.info("flushByteCache inode {} writeNum:{}", inodeId, writeCacheNode.writeNum.get());
                                    }
                                    if (writeCacheNode.writeNum.get() <= 0
                                            && writeCacheNode.commitQueue.isEmpty()
                                            && writeCacheNode.cacheSize.get() > 0) {
                                        return Node.getInstance().getInode(writeCacheNode.bucket, inodeId)
                                                .flatMap(inode -> {
                                                    WriteCacheNode wc = map.get(inodeId);
                                                    if (SMBHandler.runningDebug && InodeUtils.isError(inode)) {
                                                        log.info("flushByteCache error inode, {}, cacheSize: {}", nodeId, cacheSize);
                                                    }
                                                    if (wc != null && wc.cacheSize.get() == cacheSize && !InodeUtils.isError(inode)) {
                                                        return Node.getInstance().flushWriteCache(inode, 0, 0, 1);
                                                    }
                                                    return Mono.just(true);
                                                });
                                    }
                                }
                                return Mono.just(false);
                            })
                            .collectList()
                            .map(ignore -> true)
                            .timeout(Duration.ofSeconds(360))
                            .onErrorResume(e -> {
                                log.error("", e);
                                return Mono.just(false);
                            })
                            .doFinally(l -> {
                                FLUSHING.compareAndSet(true, false);
                            })
                            .subscribe(b -> {
                                res.complete(b, null);
                                flushAllEnd();
                            });
                } catch (Exception e) {
                    log.error("", e);
                    res.complete(false, e);
                    commitEnd();
                }
            });
        } else {
            res.complete(true, null);
            flushAllEnd();
        }

        return res.res;
    }

    private static void flushAllEnd() {
        synchronized (flushAllQueue) {
            while (!flushAllQueue.isEmpty() && flushAllQueue.peekFirst().prepare.get()) {
                WriteCacheNode.CommitRes res = flushAllQueue.pollFirst();
                if (res.error != null) {
                    try {
                        res.res.onError(res.error);
                    } catch (Exception e) {

                    }
                } else {
                    res.res.onNext(res.value);
                }
            }
        }
    }

    public static boolean getFlushing() {
        return FLUSHING.get();
    }
}
