package com.macrosan.filesystem.cache;

import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.TimeStat;
import com.macrosan.filesystem.utils.ReadObjClient;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Log4j2
public class ReadObjCache {
    private final static TimeCache<Long, CacheMark> markMap = new TimeCache<>(Duration.ofSeconds(5).toNanos(), ErasureServer.DISK_SCHEDULER);
    private static final int MAX_CACHE_NUM = 256;
    private static final long MAX_CACHE_SIZE = (4 << 20L);
    public static boolean readCacheDebug = false;

    private static Flux<Tuple2<Integer, byte[]>> readDirect(Inode inode, long offset, long size, boolean isPrefetch) {
        List<Inode.InodeData> inodeData = inode.getInodeData();
        return ReadObjClient.readFromList(0, offset, size, inode.getBucket(), inodeData, isPrefetch);
    }

    private static Flux<Tuple2<Integer, byte[]>> flatMap(int readIndex, byte[][] cached, long start, long offset, long size) {
        long bStart = start;

        long reqStart = offset;
        long reqEnd = offset + size;

        List<Tuple2<Integer, byte[]>> resList = new LinkedList<>();
        long test = 0;
        for (byte[] bytes : cached) {
            long bEnd = bStart + bytes.length;
            if (bStart >= reqEnd) {
                break;
            } else if (bEnd > reqStart) {
                byte[] res;
                if (bStart >= reqStart && bEnd <= reqEnd) {
                    res = bytes;
                } else {
                    long realStart = Math.max(reqStart, bStart);
                    long realEnd = Math.min(reqEnd, bEnd);
                    res = new byte[(int) (realEnd - realStart)];
                    System.arraycopy(bytes, (int) (realStart - bStart), res, 0, res.length);
                }

                test += res.length;
                resList.add(new Tuple2<>((int) (readIndex + Math.max(reqStart, bStart) - reqStart), res));
            }

            bStart = bEnd;
        }

        if (test != size) {
            log.error("flat map test fail");
        }
        return Flux.fromIterable(resList);
    }

    public static Flux<Tuple2<Integer, byte[]>> readObj(Inode inode, long offset, long size) {
        AtomicReference<MarkRes> markRes = new AtomicReference<>();
        long startTime = System.nanoTime();
        int cacheSize = markMap.cache.isEmpty() ? 1 : markMap.cache.size();
        int superFetch = Math.min(MAX_CACHE_NUM / cacheSize, 16);
        markMap.compute(inode.getNodeId(), (k, mark) -> {
            if (mark == null || !inode.getVersionNum().equals(mark.inodeVersion)) {
                //direct
                mark = new CacheMark(inode.getNodeId(), inode.getVersionNum(), offset, offset + size);
                markRes.set(new MarkRes(1, -1, null));
                return mark;
            } else {
                mark.updateTime = System.nanoTime();

                if (mark.end == offset) {
                    //需要新的预读
                    boolean needFetch = mark.cacheEnd <= offset + size;

                    if (!needFetch) {
                        if (mark.cacheEnd < inode.getSize() && mark.fetchSize == MAX_CACHE_SIZE && mark.cached.size() < superFetch) {
                            needFetch = true;
                        }
                    }

                    if (needFetch) {
                        long fetch = mark.fetchSize == 0 ? size * 2 : mark.fetchSize * 2;
                        if (fetch < size) {
                            fetch = size;
                        }

                        if (fetch > MAX_CACHE_SIZE) {
                            fetch = MAX_CACHE_SIZE;
                        }

                        if (mark.cacheEnd + fetch > inode.getSize()) {
                            fetch = inode.getSize() - mark.cacheEnd;
                        }

                        mark.fetchSize = fetch;
                        ReadCache nextCache = new ReadCache();
                        nextCache.cacheStart = mark.cacheEnd;
                        nextCache.cacheEnd = mark.cacheEnd + fetch;

                        long finalFetch = fetch;
                        FsUtils.fsExecutor.submit(() -> {
                            try {
                                readDirect(inode, nextCache.cacheStart, finalFetch, true).collectList().subscribe(list -> {
                                    byte[][] cached = list.stream().sorted(Comparator.comparing(t -> t.var1)).map(Tuple2::var2).toArray(byte[][]::new);
                                    nextCache.cacheMono.onNext(cached);
                                }, e -> {
                                    if (null != e && null != e.getMessage() && e.getMessage().contains("pre-read data modified")) {
                                        if (readCacheDebug) {
                                            log.info("{}: offset: {}, obj: {}", e.getMessage(), offset, inode.getObjName());
                                        }
                                    } else {
                                        log.error("readTask read fail: offset: {}, obj: {}", offset, inode.getObjName(), e);
                                    }

                                    nextCache.cacheMono.onError(e);
                                });
                            } catch (Exception e) {
                                nextCache.cacheMono.onError(e);
                            }
                        });

                        mark.cacheEnd = nextCache.cacheEnd;
                        mark.cached.add(nextCache);
                    }

                    long start = -1;
                    Mono<List<byte[]>> cacheMono = null;
                    long testSize = 0L;
                    ListIterator<ReadCache> iterator = mark.cached.listIterator();

                    while (iterator.hasNext()) {
                        ReadCache cache = iterator.next();

                        if (cache.cacheEnd <= offset) {
                            //删除旧的缓存
                            iterator.remove();
                            mark.cacheStart = cache.cacheEnd;
                        } else if (cache.cacheStart >= offset + size) {
                            break;
                        } else {
                            testSize += Math.min(cache.cacheEnd, offset + size) - Math.max(offset, cache.cacheStart);
                            if (start == -1) {
                                start = cache.cacheStart;
                                cacheMono = cache.cacheMono.map(bs -> new LinkedList<>(Arrays.asList(bs)));
                            } else {
                                cacheMono = cacheMono.flatMap(list -> cache.cacheMono.map(bs -> {
                                    list.addAll(Arrays.asList(bs));
                                    return list;
                                }));
                            }
                        }
                    }

                    markRes.set(new MarkRes(0, start, cacheMono.map(l -> l.toArray(new byte[0][]))));

                    //读末尾
                    if (offset + size == inode.getSize()) {
                        return null;
                    }

                    mark.start = offset;
                    mark.end = offset + size;
                    return mark;
                } else {
                    //direct
                    mark = new CacheMark(inode.getNodeId(), inode.getVersionNum(), offset, offset + size);
                    markRes.set(new MarkRes(1, -1, null));
                    return mark;
                }
            }
        });

        MarkRes res = markRes.get();
        switch (res.state) {
            case 0:
                return res.async.flatMapMany(cached -> {
//                    TimeStat.addTime("cached", System.nanoTime() - startTime);
                    return flatMap(0, cached, res.start, offset, size);
                });
            case 1:
//                TimeStat.addTime("direct", System.nanoTime() - startTime);
                return readDirect(inode, offset, size, false);
            default:
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "get read mark fail");
        }
    }

    private static class CacheMark {
        long nodeID;
        //上一次read的start和end
        long start;
        long end;
        //缓存数据的start和end
        long cacheStart;
        long cacheEnd;
        //上一次预读的大小
        long fetchSize = 0;
        String inodeVersion;
        long updateTime = System.nanoTime();
        List<ReadCache> cached = new LinkedList<>();

        CacheMark(long nodeID, String inodeVersion, long start, long end) {
            this.nodeID = nodeID;
            this.inodeVersion = inodeVersion;
            this.start = start;
            this.end = end;
            this.cacheStart = start;
            this.cacheEnd = end;
        }
    }

    private static class ReadCache {
        long cacheStart;
        long cacheEnd;
        MonoProcessor<byte[][]> cacheMono = MonoProcessor.create();
    }

    private static class MarkRes {
        long start;
        int state;
        Mono<byte[][]> async;

        MarkRes(int state, long start, Mono<byte[][]> async) {
            this.state = state;
            this.start = start;
            this.async = async;
        }
    }
}
