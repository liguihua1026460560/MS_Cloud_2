package com.macrosan.filesystem.cache;

import com.macrosan.ec.Utils;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cifs.CIFS;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.utils.FSQuotaUtils;
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
import lombok.extern.log4j.Log4j2;
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
import static com.macrosan.filesystem.cache.BytesPool.MAX_BYTE_POOL_SIZE;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.filesystem.utils.InodeUtils.isError;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;

@Log4j2
public class WriteCache {

    public static final int MAX_CACHE_SIZE = (128 << 20);
    public static final int MAX_TRUNK_CACHE_SIZE = (16 << 20);
    public static AtomicInteger AVAILABLE_CACHE_NUM = new AtomicInteger(MAX_BYTE_POOL_SIZE);
    private static final Map<Long, WriteCache> map = new ConcurrentHashMap<>();
    private static final AtomicBoolean FLUSHING = new AtomicBoolean(false);

    private AtomicInteger cacheSize = new AtomicInteger(0);
    private String bucket;
    private long nodeId;
    private boolean sync;
    private StoragePool dataPool;
    private Map<Long, ByteCache> cacheData = new ConcurrentHashMap<>();
    // 引用计数：当get该NFSCache时增加1，当该次NFSCache完成数据的write时减1
    private AtomicInteger writeNum = new AtomicInteger(0);
    private boolean isCIFS = false;

    public WriteCache(String bucket, long nodeId, boolean sync, String storage) {
        this.sync = sync;
        this.bucket = bucket;
        this.nodeId = nodeId;
        dataPool = StoragePoolFactory.getStoragePool(storage, bucket);
    }

    public static int getAvailableCacheNum() {
        return AVAILABLE_CACHE_NUM.get();
    }

    public static Mono<WriteCache> getCache(String bucket, long nodeId, int flags, String storage, boolean... isCIFS) {
        boolean sync = flags != 0;
        return Mono.just(1L)
                .map(l -> {
                    if (sync) {
                        return new WriteCache(bucket, nodeId, sync, storage);
                    }
                    return map.compute(nodeId, (k, v) -> {
                        if (v == null) {
                            v = new WriteCache(bucket, nodeId, sync, storage);
                        }
                        v.sync = sync;
                        v.writeNum.incrementAndGet();
                        if (isCIFS.length > 0) {
                            v.isCIFS = isCIFS[0];
                        }
                        return v;
                    });
                });
    }

    public long getWriteNum() {
        return writeNum.get();
    }

    public static Mono<Boolean> isExistCache(long nodeId, boolean isSMB3, SMB2FileId smb2FileId) {
        if (isSMB3) {
            return Mono.just(WriteCacheClient.isExistCache(smb2FileId));
        }
        return Mono.just(map.get(nodeId) != null);
    }

    public static void removeCache(long nodeId) {
        map.compute(nodeId, (k, v) -> {
            if (v != null) {
                v.cacheData.forEach((key, byteCache) -> {
                    BytesPool.release(byteCache.data);
                });
            }
            return null;
        });
    }

    public Mono<Boolean> nfsWrite(long offset, byte[] bytes, Inode inode, int flag) {
        long key = offset / MAX_TRUNK_CACHE_SIZE;
        boolean canWrite = cacheData.get(key) != null;
        if (flag != 0 || (AVAILABLE_CACHE_NUM.get() <= 0 && !canWrite)) {
            //同步写直接进行put
            try {
                return directWrite(offset, bytes, inode);
            } finally {
                if (CIFS.cifsDebug) {
                    log.info("【write num】write num:{},flag:{},can write:{}", writeNum.get(), flag, canWrite);
                }
                writeNum.decrementAndGet();
            }
        }
        return nfsWriteCache(bytes, inode, offset);
    }

    private Mono<Boolean> directWrite(long offset, byte[] bytes, Inode inode) {
        // 根据文件大小决定写入数据池还是缓存池
        StorageOperate dataOperate = new StorageOperate(DATA, inode.getObjName(), bytes.length);
        StoragePool dataPool = StoragePoolFactory.getStoragePool(dataOperate, inode.getBucket());
        Encoder encoder = dataPool.getEncoder(bytes.length);
        encoder.put(bytes);
        Digest digest = new Md5Digest();
        digest.update(bytes);
        String md5 = Hex.encodeHexString(digest.digest());
        return nfsPutObj(encoder, inode, offset, bytes.length, md5, dataPool)
                .doOnNext(res -> {
                    if (AVAILABLE_CACHE_NUM.get() <= 0 && this.isCIFS && FLUSHING.compareAndSet(false, true)) {
                        FsUtils.fsExecutor.submit(this::flushByteCache);
                    }
                });
    }

    private Mono<Boolean> nfsWriteCache(byte[] bytes, Inode inode, long curOffset) {
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
                        v = ByteCache.alloc(curOffset, 0);
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
                    //第一块分配失败，直接落盘所有bytes
                    return directWrite(curOffset, bytes, inode);
                }

                //上一个trunk写完了
                if (secondBlock != null) {
                    cacheData.compute(key + 1, (k, v) -> {
                        long trunkOffset = k * MAX_TRUNK_CACHE_SIZE;
                        if (v == null) {
                            v = ByteCache.alloc(trunkOffset, 0);
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
                        //第一块成功，第二块分配失败，也把所有数据落盘
                        return directWrite(curOffset, bytes, inode);
                    }
                }
            } catch (Exception e) {
                log.error("", e);
                return Mono.just(false);
            }

            //write 不需要等待flush
            if (needFlush.get()) {
                FsUtils.fsExecutor.submit(() -> nfsCommit(inode, 0, 0, true).subscribe());
            }

            return Mono.just(true);

        } finally {
            writeNum.decrementAndGet();
        }
    }

    public Mono<Boolean> nfsCommit(Inode inode, long offset, int count) {
        return FSQuotaUtils.existQuotaInfo(inode.getBucket(), inode.getObjName(), System.currentTimeMillis(), inode.getUid(), inode.getGid())
                .flatMap(t2 -> {
                    if (t2.var1) {
                        inode.getXAttrMap().put(QUOTA_KEY, Json.encode(t2.var2));
                    }
                    return nfsCommit(inode, offset, count, false);
                });
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

    private final LinkedList<CommitRes> commitQueue = new LinkedList<>();

    public Mono<Boolean> nfsCommit(Inode inode, long offset, int count, boolean writeFlush) {
        CommitRes res = new CommitRes();
        List<ByteCache> needPut;

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
                    nfsFlushCaches(inode, needPut).subscribe(b -> {
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
                if (!writeFlush) {
                    if (writeNum.decrementAndGet() == 0 && cacheSize.get() == 0) {
                        return null;
                    } else {
                        return v;
                    }
                } else {
                    return v;
                }
            });
        });
    }

    private void commitEnd() {
        synchronized (commitQueue) {
            while (!commitQueue.isEmpty() && commitQueue.peekFirst().prepare.get()) {
                CommitRes res = commitQueue.pollFirst();
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

    //只获得特定range的ByteCache
    private List<ByteCache> getRangeCaches(long offset, int count) {
        List<ByteCache> needPut = new LinkedList<>();
        long end = offset + count;
        //可以优化key的搜索条件不用遍历整个缓存
        for (long key : cacheData.keySet()) {
            cacheData.computeIfPresent(key, (k0, byteCache) -> {
                long byteEnd = byteCache.offset + byteCache.size;
                if (byteCache.offset <= offset) {
                    if (byteEnd > offset) {
                        needPut.add(byteCache);
                        return null;
                    } else {
                        return byteCache;
                    }
                } else if (byteCache.offset < end) {
                    needPut.add(byteCache);
                    return null;
                } else {
                    return byteCache;
                }
            });
        }

        return needPut;
    }

    //write 触发的flush
    private List<ByteCache> getWriteFlushCaches() {
        LinkedList<ByteCache> needPut = new LinkedList<>();

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

    //获得尽可能多的ByteCache
    private List<ByteCache> getAllCaches() {
        LinkedList<ByteCache> needPut = new LinkedList<>();

        for (long key : cacheData.keySet()) {
            cacheData.computeIfPresent(key, (k, v) -> {
                needPut.add(v);
                cacheSize.addAndGet(-v.size);
                return null;
            });
        }

        return needPut;
    }

    private Mono<Boolean> nfsFlushCaches(Inode inode, List<ByteCache> needPut) {
        return Flux.fromStream(needPut.stream())
                .flatMap(byteCache -> nfsFlush(inode, byteCache))
                .doOnError(e -> log.error("", e))
                .collectList()
                .map(resList -> {
                    needPut.clear();
                    return !resList.contains(false);
                });
    }

    private Mono<Boolean> nfsFlush(Inode inode, ByteCache byteCache) {
        return Flux.fromArray(byteCache.pages.toArray(new Tuple2[0]))
                .flatMap(t -> {
                    int offset = (int) ((long) t.var1 % MAX_TRUNK_CACHE_SIZE);
                    int size = (int) t.var2;
                    // 根据文件大小决定写入数据池还是缓存池
                    StorageOperate dataOperate = new StorageOperate(DATA, inode.getObjName(), size);
                    StoragePool dataPool = StoragePoolFactory.getStoragePool(dataOperate, inode.getBucket());
                    Encoder encoder = dataPool.getEncoder(size);
                    Digest digest = new Md5Digest();
                    if (size == MAX_TRUNK_CACHE_SIZE) {
                        encoder.put(byteCache.data);
                        digest.update(byteCache.data);
//                        byteCache.data = null;
                    } else {
                        byte[] tmp = new byte[size];
                        System.arraycopy(byteCache.data, offset, tmp, 0, size);
                        encoder.put(tmp);
                        digest.update(tmp);
                        tmp = null;
                    }
                    String md5 = Hex.encodeHexString(digest.digest());
                    return nfsPutObj(encoder, inode, (long) t.var1, size, md5, dataPool);
                }).collectList().map(resList -> !resList.contains(false))
                .doOnNext(r -> {
                    BytesPool.release(byteCache.data);
                });
    }

    private Mono<Inode> putObj(Encoder encoder0, Inode inode, long curOffset, int curSize, String md5, int updateTime, StoragePool dataPool) {
        encoder0.complete();
        FsUtils.PutObjectFunction function = (p, list, msgs) -> FsUtils.sendEncode(p, encoder0, list, msgs);
        return FsUtils.putObj(inode, function, curOffset, curSize, md5, updateTime, dataPool);
    }

    private Mono<Boolean> nfsPutObj(Encoder encoder0, Inode inode, long curOffset, int curSize, String md5, StoragePool dataPool) {
        return putObj(encoder0, inode, curOffset, curSize, md5, 1, dataPool)
                .map(inode1 -> !isError(inode1));
    }

    private void flushByteCache() {
        if (SMBHandler.runningDebug) {
            log.info("start flushByteCache");
        }
        Flux.fromStream(map.keySet().stream())
                .flatMap(inodeId -> {
                    WriteCache writeCache = map.get(inodeId);
                    if (writeCache != null && writeCache.isCIFS) {
                        long cacheSize = writeCache.cacheSize.get();
                        if (SMBHandler.runningDebug) {
                            log.info("flushByteCache inode {} writeNum:{}", inodeId, writeCache.writeNum.get());
                        }
                        if (writeCache.writeNum.get() <= 0
                                && writeCache.commitQueue.isEmpty()
                                && writeCache.cacheSize.get() > 0) {
                            return Node.getInstance().getInode(writeCache.bucket, inodeId)
                                    .flatMap(inode -> WriteCache.getCache(inode.getBucket(), inodeId, 0, inode.getStorage(), true)
                                            .flatMap(wc -> {
                                                if (wc.cacheSize.get() == cacheSize) {
                                                    return wc.nfsCommit(inode, 0, 0);
                                                }
                                                return Mono.just(true);
                                            }));
                        }
                    }
                    return Mono.just(false);
                })
                .collectList()
                .timeout(Duration.ofSeconds(360))
                .doFinally(l -> {
                    FLUSHING.compareAndSet(true, false);
                })
                .subscribe();
    }

    public static boolean getFlushing() {
        return FLUSHING.get();
    }
}
