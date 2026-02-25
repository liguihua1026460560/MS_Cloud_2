package com.macrosan.filesystem.utils;

import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.reply.ReadDirPlusReply;
import com.macrosan.filesystem.nfs.reply.ReadDirReply;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_BAD_COOKIE;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_I0;
import static com.macrosan.filesystem.FsConstants.ONE_SECOND_NANO;

@Log4j2
public class ReadDirCache {
    private static final Map<String, List<ReadDirCache>> cacheMap = new ConcurrentHashMap<>();

    private static final long MAX_CACHE_MIL = 120_000;
    private static final long READ_AHEAD_SIZE = 1 << 20;
    public static boolean debug = false;

    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("ReadDirOffsetCache-"));

    static {
        executor.submit(ReadDirCache::tryClearCache);
    }

    private static void tryClearCache() {
        for (String key : cacheMap.keySet()) {
            cacheMap.computeIfPresent(key, (k, list) -> {
                list.removeIf(cache -> System.nanoTime() - cache.lastUsed >= MAX_CACHE_MIL * 1000_000L);

                if (list.isEmpty()) {
                    return null;
                } else {
                    return list;
                }
            });
        }

        executor.schedule(ReadDirCache::tryClearCache, 30, TimeUnit.MILLISECONDS);
    }

    String bucket;
    String prefix;

    // <objName, Inode>
    final TreeMap<String, Inode> cache = new TreeMap<>();
    // <cookie, objName>
    final Map<Long, String> cookieMap = new HashMap<>();

    String marker = "";
    boolean end = false;
    long lastOffset = 0;
    long lastUsed = System.nanoTime();
    int lastList = 0;
    String lastObj = "";
    NFSHandler nfsHandler;

    private ReadDirCache(String bucket, String prefix, NFSHandler nfsHandler) {
        this.bucket = bucket;
        this.prefix = prefix;
        this.nfsHandler = nfsHandler;
    }

    private void fillCacheRes(String startMarker, Inode[] inodeList, long dirNodeId) {
        synchronized (cache) {
            if (marker.equals(startMarker)) {
                for (Inode inode : inodeList) {
                    long offset = inode.getCookie();

                    String objName = inode.getObjName();
                    cache.put(objName, inode);
                    cookieMap.put(offset, objName);

                    marker = objName;
                }

                if (inodeList.length == 0) {
                    end = true;
                }

                lastList = inodeList.length;
            }
        }
    }

    AtomicLong t = new AtomicLong();

    AtomicLong running = new AtomicLong();

    private Mono<Boolean> fillCache(boolean force, long dirNodeId, List<Inode.ACE> dirACEs) {
        if (running.getAndIncrement() > 0L && !force) {
            running.decrementAndGet();
            return Mono.just(true);
        }

        if (end) {
            return Mono.just(true);
        } else {
            String startMarker = marker;
            return FsUtils.listObject(bucket, prefix, startMarker, READ_AHEAD_SIZE)
                    .flatMapMany(inodeList -> Flux.fromStream(inodeList.stream()))
                    .index()
                    .flatMap(t -> {
                        Inode inode = t.getT2();
                        if (inode.getCookie() == 0 && dirNodeId != -1) {
                            return Mono.just(t.getT1())
                                    .zipWith(Node.getInstance().createS3Inode(dirNodeId, bucket, inode.getObjName(), inode.getVersionId(), inode.getReference(), dirACEs));
                        } else if (inode.getCounter() > 0) {
                            return Mono.just(t.getT1())
                                    .zipWith(Node.getInstance().repairCookieAndInode(inode.getNodeId(), bucket, inode.getCreateTime()));
                        } else {
                            return Mono.just(t);
                        }
                    })
                    .collectList()
                    .map(inodeList -> {
                        Inode[] inodes = new Inode[inodeList.size()];

                        inodeList.forEach(t -> {
                            inodes[t.getT1().intValue()] = t.getT2();
                        });

                        fillCacheRes(startMarker, inodes, dirNodeId);
                        running.decrementAndGet();
                        return true;
                    });
        }
    }

    private Mono<Boolean> mayFillCache(long offset, long maxSize, long dirNodeId, List<Inode.ACE> dirACEs) {
        if (offset == 0) {
            return fillCache(true, dirNodeId, dirACEs);
        } else {
            String objName = cookieMap.get(offset);

            String key = cache.higherKey(objName);
            int size = 0;
            while (key != null) {
                size += cache.get(key).countDirentplusSize();

                if (size >= maxSize) {
                    break;
                }

                key = cache.higherKey(key);
            }

            if (size < maxSize) {
                return fillCache(true, dirNodeId, dirACEs);
            }
        }

        return Mono.just(true);
    }

    private Mono<List<Inode>> list(long offset, long maxSize, long dirNodeId, String markerObj, List<Inode.ACE> dirACEs) {
        return mayFillCache(offset, maxSize, dirNodeId, dirACEs).map(b -> {
            LinkedList<Inode> res = new LinkedList<>();
            String objName;
            String key;
            if (offset == 0) {
                objName = "";
            } else {
                // 通过cookie表查得的offset不在cookieMap中
                objName = cookieMap.get(offset);
                if (objName == null) {
                    if (markerObj != null) {
                        objName = markerObj;
                    } else {
                        lastUsed = System.nanoTime();
                        if (debug) {
                            log.info("list: find cookie {} fail, marker: {}, lastOffset: {}, end: {} ", offset, marker, lastOffset, end);
                        }
                        return res;
                    }
                }
            }

            key = cache.higherKey(objName);

            int size = 0;
            while (key != null) {
                size += cache.get(key).countDirentplusSize();
                res.add(cache.get(key));

                if (size >= maxSize) {
                    break;
                }

                key = cache.higherKey(key);
            }

            if (key != null) {
                synchronized (cache) {
                    int lastSize = 0;
                    key = cache.lowerKey(lastObj);
                    while (key != null) {
                        if (lastSize < READ_AHEAD_SIZE) {
                            lastSize += cache.get(key).countDirentplusSize();
                        } else {
                            synchronized (cache) {
                                Set<String> keySet = cache.headMap(key).keySet();
                                if (!keySet.isEmpty()) {
                                    if (debug) {
                                        log.info("remove cache: {}, key: {}, marker: {}, lastOffset: {}, offset: {}, dirNodeId: {}", keySet.size(), key, marker, lastOffset, offset, dirNodeId);
                                    }
                                    cookieMap.values().removeAll(keySet);
                                    cache.headMap(key).clear();
                                }
                            }
                            break;
                        }


                        key = cache.lowerKey(key);
                    }

                    if (cache.size() < lastList) {
                        FsUtils.fsExecutor.submit(() -> {
                            fillCache(false, dirNodeId, dirACEs).subscribe();
                        });
                    }
                }
                lastOffset = offset;
                lastObj = objName;
            }

            lastUsed = System.nanoTime();
            return res;
        });
    }

    /**
     * 一次ls会触发多个readDirPlu请求，一般情况下，第一次遍历完返回cookie等于最后文件的cookie；第二次遍历从上一次cookie开始遍历，此时由于
     * end为true所以不再重复从磁盘列取，而是直接返空；第三次则可能从已返回的文件cookie开始倒着遍历，此时若cookie不与metaData绑定，则会重新
     * 从磁盘中列取文件，导致ls出现重复文件的情况；现在metaData生成时即将cookie与metaData绑定，inode中不保存cookie；倒查文件时返回的cookie
     * 一定为当前ls对应文件的cookie，若缓存过期失效，则应在offset=0时完成重新列取，而不会在后续的readDirPlu中重复列取；在列取时，若文件为s3
     * 创建，则在缓存中生成cookie，再向磁盘中更新元数据
     **/
    public static <T> Mono<List<Inode>> listAndCache(String bucket, String prefix, long offset, long maxSize, NFSHandler nfsHandler, long dirNodeId, T reply, List<Inode.ACE> dirACEs) {
        AtomicReference<String> markerObj = new AtomicReference<>();
        MonoProcessor<ReadDirCache> ref = MonoProcessor.create();
        AtomicReference<ReadDirCache> reference = new AtomicReference<>();
        cacheMap.compute(bucket + '/' + prefix, (k, v) -> {
            if (v == null) {
                v = new LinkedList<>();
                reference.set(new ReadDirCache(bucket, prefix, nfsHandler));
            } else if (offset == 0) {
                // 比较nfsHandler地址区分客户端，每次list重置当前客户端的所有ReadDirCache，避免删除文件后仍然显示的问题
                v.removeIf(cache -> cache.nfsHandler.equals(nfsHandler));
                reference.set(new ReadDirCache(bucket, prefix, nfsHandler));
            } else {
                ReadDirCache exitReadDirCache = getExitReadDirCache(v, nfsHandler, offset);
                if (exitReadDirCache != null) {
                    ref.onNext(exitReadDirCache);
                    reference.set(exitReadDirCache);
                    return v;
                }
            }
            if (reference.get() != null) {
                v.add(reference.get());
                ref.onNext(reference.get());
            }
            return v;
        });
        if (reference.get() == null) {
            ReadDirCache readDirCache = new ReadDirCache(bucket, prefix, nfsHandler);
            if (null != reply) {
                // 通过cookie去cookie表中寻找key
                FsUtils.findCookie(bucket, offset)
                        .timeout(Duration.ofSeconds(60))
                        .doOnError(t -> {
                            log.error("find cookie error {}:{} {}", bucket, prefix, offset);
                            updateReadDirCacheMap(bucket, prefix, nfsHandler, readDirCache, offset, ref);
                        })
                        .subscribe(objName -> {
                            if (StringUtils.isNotBlank(objName)) {
                                if (!objName.startsWith(prefix)) {
                                    // cookie过期返错
                                    log.info("find cache bad cookie {}:{} {}, find obj: {}", bucket, prefix, offset, objName);
                                    if (reply instanceof ReadDirPlusReply) {
                                        ((ReadDirPlusReply) reply).status = NFS3ERR_BAD_COOKIE;
                                    } else if (reply instanceof ReadDirReply) {
                                        ((ReadDirReply) reply).status = NFS3ERR_BAD_COOKIE;
                                    }
                                    reference.set(readDirCache);
                                } else {
                                    log.info("find cookie {}:{} {}：{}", bucket, prefix, offset, objName);
                                    readDirCache.marker = objName;
                                    reference.set(readDirCache);
                                    markerObj.set(objName);
                                }
                            } else {
                                log.info("find cache fail {}:{} {}, find obj: {}", bucket, prefix, offset, objName);
                                // 查找不到cookie对应的key返错
                                if (reply instanceof ReadDirPlusReply) {
                                    ((ReadDirPlusReply) reply).status = NFS3ERR_I0;
                                } else if (reply instanceof ReadDirReply) {
                                    ((ReadDirReply) reply).status = NFS3ERR_I0;
                                }
                                reference.set(readDirCache);
                            }
                            updateReadDirCacheMap(bucket, prefix, nfsHandler, reference.get(), offset, ref);
                        });
            } else {
                updateReadDirCacheMap(bucket, prefix, nfsHandler, readDirCache, offset, ref);
            }
        }
        return ref.flatMap(readDirCache ->
                readDirCache.list(offset, maxSize, dirNodeId, markerObj.get(), dirACEs)
                        .flatMap(list -> {
                            int status = 0;
                            if (reply instanceof ReadDirPlusReply) {
                                status = ((ReadDirPlusReply) reply).status;
                            } else if (reply instanceof ReadDirReply) {
                                status = ((ReadDirReply) reply).status;
                            }
                            if (status == NFS3ERR_I0) {
                                long stamp = System.currentTimeMillis() / 1000;
                                int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
                                return Node.getInstance()
                                        .updateInodeTime(dirNodeId, bucket, stamp, stampNano, false, true, true)
                                        .map(i -> list);
                            }
                            return Mono.just(list);
                        }));
    }


    private static ReadDirCache getExitReadDirCache(List<ReadDirCache> list, NFSHandler nfsHandler, long offset) {

        for (ReadDirCache cache : list) {
            if (cache.nfsHandler.equals(nfsHandler) && cache.cookieMap.containsKey(offset)) {
                String curObj = cache.cookieMap.get(offset);
                TreeMap<String, Inode> treeMap = cache.cache;
                if (curObj != null && treeMap.containsKey(curObj)) {
                    return cache;
                }
            }
        }
        return null;
    }

    /***
     * 更新readdircache缓存
     */
    private static void updateReadDirCacheMap(String bucket, String prefix, NFSHandler nfsHandler, ReadDirCache readDirCache, long offset, MonoProcessor<ReadDirCache> ref) {
        cacheMap.compute(bucket + '/' + prefix, (k, v) -> {
            if (v != null) {
                ReadDirCache exitReadDirCache = getExitReadDirCache(v, nfsHandler, offset);
                if (exitReadDirCache != null) {
                    ref.onNext(exitReadDirCache);
                    return v;
                } else {
                    v.add(readDirCache);
                }
            }
            ref.onNext(readDirCache);
            return v;
        });
    }
}
