package com.macrosan.filesystem.cache;

import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.functional.Tuple2;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class CifsQueryDirCache {
    private static final long READ_AHEAD_SIZE = 65536;

    // <objName, Inode>
    final TreeMap<String, Inode> cache = new TreeMap<>();
    // <cookie, objName>
    final Map<Long, String> cookieMap = new HashMap<>();

    String marker = "";
    boolean end = false;
    public long queryDirOffset;
    int lastList = 0;

    String bucket;
    String prefix;
    String pattern;
    long maxSize;

    private CifsQueryDirCache(String bucket, String prefix, String pattern, long maxSize) {
        this.bucket = bucket;
        this.prefix = prefix;
        this.pattern = pattern;
        this.maxSize = maxSize;
    }

    public String getMarker(){
        return marker;
    }

    public static Mono<Tuple2<List<Inode>, Boolean>> listAndCache(byte queryClass, String pattern, SMB2FileId.FileInfo fileInfo, long maxSize) {
        if (fileInfo.queryDirCache == null) {
            fileInfo.queryDirCache = new CifsQueryDirCache(fileInfo.bucket, fileInfo.obj, pattern, maxSize);
        }

        Inode listDir = fileInfo.openInode;
        List<Inode.ACE> acesOfListDir = listDir == null ? null : listDir.getACEs();
        return fileInfo.queryDirCache.list(queryClass, maxSize, fileInfo.dirInode, acesOfListDir);
    }

    AtomicLong running = new AtomicLong();

    private void fillCacheRes(String startMarker, Inode[] inodeList) {
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

    private Mono<Boolean> fillCache(boolean force, long dirNodeId, byte queryClass, List<Inode.ACE> dirACEs) {
        if (running.getAndIncrement() > 0L && !force) {
            running.decrementAndGet();
            return Mono.just(true);
        }

        if (end) {
            return Mono.just(true);
        } else {
            String startMarker = marker;
            return FsUtils.listObject(bucket, prefix, startMarker, READ_AHEAD_SIZE, pattern, queryClass)
                    .flatMapMany(inodeList -> Flux.fromStream(inodeList.stream()))
                    .index()
                    .flatMap(t -> {
                        Inode inode = t.getT2();
                        if (inode.getCookie() == 0) {
                            return Mono.just(t.getT1())
                                    .zipWith(Node.getInstance().createS3Inode(dirNodeId, bucket, inode.getObjName(), inode.getVersionId(), inode.getReference(), dirACEs));
                        } else if (inode.getCounter() > 0) {
                            return Mono.just(t.getT1())
                                    .zipWith(Node.getInstance().repairCookieAndInode(inode.getNodeId(), bucket, inode.getCreateTime())
                                            .map(inode1 -> {
                                                if (inode1.getCreateTime() == 0 && inode1.getNodeId() > 0 && inode1.getAtime() > 0) {
                                                    inode1.setCreateTime(Inode.getMinTime(inode1));
                                                }
                                                return inode1;
                                            }));
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

                        fillCacheRes(startMarker, inodes);
                        running.decrementAndGet();
                        return true;
                    });
        }
    }

    private Mono<Boolean> mayFillCache(long offset, byte queryClass, long maxSize, long dirNodeId, List<Inode.ACE> dirACEs) {
        if (offset == 0) {
            return fillCache(true, dirNodeId, queryClass, dirACEs);
        } else {
            int size = 0;

            // 如果遍历时上一个列举请求触发了异步的fillCache，且正在cache.put()；当前cache获取entry可能为null，因此增加同步锁
            synchronized (cache) {
                String objName = cookieMap.get(offset);
                Map.Entry<String,Inode> entry = cache.higherEntry(objName);
                while (entry != null) {
                    Inode inode = entry.getValue();
                    size += inode.countQueryDirInfo(queryClass, prefix);

                    if (size >= maxSize) {
                        break;
                    }

                    entry = cache.higherEntry(entry.getKey());
                }
            }

            if (size < maxSize) {
                return fillCache(true, dirNodeId, queryClass, dirACEs);
            }
        }

        return Mono.just(true);
    }

    /**
     * @return List<Inode> 为返回的条目列表
     *         boolean 为判断是否返回 smb2_info_length_mismatch的标志位
     **/
    private Mono<Tuple2<List<Inode>, Boolean>> list(byte queryClass, long maxSize, long dirNodeId, List<Inode.ACE> dirACEs) {
        return mayFillCache(queryDirOffset, queryClass, maxSize, dirNodeId, dirACEs).map(b -> {
            LinkedList<Inode> res = new LinkedList<>();
            Tuple2<List<Inode>, Boolean> tuple2 = new Tuple2<>(res, false);

            Map.Entry<String, Inode> entry;

            synchronized (cache) {
                String objName;

                if (queryDirOffset == 0) {
                    objName = "";
                } else {
                    objName = cookieMap.get(queryDirOffset);
                    //end
                    if (objName == null) {
                        return tuple2;
                    }
                }

                entry = cache.higherEntry(objName);

                int size = 0;
                while (entry != null) {
                    Inode inode = entry.getValue();
                    size += inode.countQueryDirInfo(queryClass, prefix);

                    if (maxSize == 1) {
                        // 如果设置 single 标志位，则列举一个再返回；此时说明key存在，因此直接返回当前key即可
                        res.add(inode);
                        queryDirOffset = inode.getCookie();
                        break;
                    } else if (size > maxSize) {
                        // 先判断size再add进列表，如果判断超过size，则需要将key退回成上一个key；正好size==maxsize的情况允许列举
                        entry = cache.lowerEntry(entry.getKey());
                        if (res.isEmpty()) {
                            // 返回列表为空，但是实际没有列举完，文件info大于请求长度，此时返回mismatch错误码
                            tuple2.var2 = true;
                        }
                        break;
                    }

                    res.add(inode);
                    queryDirOffset = inode.getCookie();
                    entry = cache.higherEntry(entry.getKey());
                }
            }

            if (entry != null) {
                synchronized (cache) {
                    Set<String> keySet = cache.headMap(entry.getKey()).keySet();
                    if (!keySet.isEmpty()) {
                        cookieMap.values().removeAll(keySet);
                        cache.headMap(entry.getKey()).clear();
                    }
                }

                if (cache.size() < lastList) {
                    FsUtils.fsExecutor.submit(() -> {
                        fillCache(false, dirNodeId, queryClass, dirACEs).subscribe();
                    });
                }
            }

            return tuple2;
        });
    }

    public void clear() {
        synchronized (cache) {
            cookieMap.clear();
            cache.clear();
        }
    }
}
