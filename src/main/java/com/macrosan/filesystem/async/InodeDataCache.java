package com.macrosan.filesystem.async;

import com.macrosan.doubleActive.DataSynChecker;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cache.TimeCache;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.SysConstants.ROCKS_CHUNK_FILE_KEY;

@Log4j2
public class InodeDataCache {
    private final static TimeCache<Long, CacheMark> markMap = new TimeCache<>(Duration.ofSeconds(5).toNanos(), DataSynChecker.SCAN_SCHEDULER);

    private static final int cacheListThreshold = 100;

    private static class CacheMark {
        long nodeId;
        // key为InodeData的fileName
        // todo f 改成set
        Map<String, Inode.InodeData> inodeDataMap;

        CacheMark(long nodeId) {
            this.nodeId = nodeId;
            inodeDataMap = new HashMap<>();
        }
    }

    static CacheMark getInodeDataCache(Inode curInode) {
        long nodeId = curInode.getNodeId();
        return markMap.compute(nodeId, (k, v) -> {
            if (v == null) {
                v = new CacheMark(nodeId);
                for (Inode.InodeData inodeData : curInode.getInodeData()) {
                    if (!inodeData.chunk()) {
                        v.inodeDataMap.put(inodeData.fileName, inodeData);
                    }
                }
                return v;
            } else {
                return v;
            }
        });
    }

    /**
     * 确认record记录中的InodeData是否需要执行同步。
     * 目的是防止InodeDataList很大的时候，每次同步数据都需要去通过遍历确定fileName是否匹配
     *
     * @param recordFileName record中记录的inodeData.recordFileName
     * @param curInode       getInode的当前结果
     * @return 当前文件相关的InodeData是否匹配record中的InodeData，true表示将继续dealRecord流程，false表示无需处理该record。
     */
    public static Mono<Boolean> checkInodeData0(String recordFileName, Inode curInode, long fileOffset) {
        // 目前差异记录处理为每个桶一个线程逐个处理，暂不需要使用缓存
        if (curInode.getInodeData().size() < cacheListThreshold) {
            return traversingInodeDataList(recordFileName, curInode, fileOffset);
        }

        // 如果查询到的curInode里的inodeData list很长，尝试使用读取内存。
        CacheMark cacheMark = getInodeDataCache(curInode);
        if (cacheMark != null && cacheMark.inodeDataMap.containsKey(recordFileName)) {
            // 此时可能缓存尚未更新。后续的同步流程里readObj会出错并超时，本轮record处理将返回false。
            return Mono.just(true);
        } else {
            // 此时可能是inode已被修改覆盖（record不处理），或者缓存还未更新（record要处理）。
            // 将curInode中的inodeDataList遍历一遍确认fileName是否匹配。
            return traversingInodeDataList(recordFileName, curInode, fileOffset);
        }
    }

    public static Mono<Boolean> checkInodeData(String recordFileName, Inode curInode, long fileOffset) {
        return traversingInodeDataList(recordFileName, curInode, fileOffset);
    }

    static Mono<Boolean> traversingInodeDataList(String recordFileName, Inode curInode, long fileOffset) {
        List<Inode.InodeData> curInodeDataList = curInode.getInodeData();
        long curOffset = 0;
        for (Inode.InodeData data : curInodeDataList) {
            if (curOffset <= fileOffset && curOffset + data.size > fileOffset) {
                if (data.chunk()) {
                    return traversingChunk(recordFileName, data.fileName, curOffset, fileOffset);
                } else if (data.fileName.equals(recordFileName)) {
                    return Mono.just(true);
                } else {
                    return Mono.just(false);
                }
            }
            curOffset += data.size;
        }
        return Mono.just(false);
    }

    static Mono<Boolean> traversingChunk(String recordFileName, String fileName, long offset, long fileOffset) {
        if (fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
            return Node.getInstance().getChunk(fileName)
                    .flatMap(chunkFile -> {
                        if (chunkFile.size < 0) {
                            return Mono.just(false);
                        } else {
                            long[] curOffset = {offset};
                            LinkedList<Inode.InodeData> chunkList = chunkFile.getChunkList();
                            for (Inode.InodeData data : chunkList) {
                                if (curOffset[0] <= fileOffset && curOffset[0] + data.size > fileOffset) {
                                    if (data.chunk()) {
                                        return traversingChunk(recordFileName, data.fileName, curOffset[0], fileOffset);
                                    } else if (data.fileName.equals(recordFileName)) {
                                        return Mono.just(true);
                                    } else {
                                        return Mono.just(false);
                                    }
                                }
                                curOffset[0] += data.size;
                            }
                            return Mono.just(false);
                        }
                    });
        } else {
            return Mono.just(fileName.equals(recordFileName));
        }
    }

    public static Mono<Tuple3<Long, Long, String>> searchInodeData(String value, List<Inode.InodeData> list, long fileOffset, long fileSize, String storage) {
        //有fileOffset和fileSize，只要找到一个元数据就可以返回
        long curOffset = 0L;

        List<Mono<Tuple3<Long, Long, String>>> chunkRes = new LinkedList<>();

        for (Inode.InodeData inodeData : list) {
            long curEnd = curOffset + inodeData.getSize();

            if (curOffset >= fileOffset + fileSize) {
                break;
            } else if (fileOffset < curEnd) {
                if (inodeData.fileName.replace("/split/", "").equals(value)) {
                    return Mono.just(new Tuple3<>(fileOffset, fileSize, inodeData.storage));
                } else if (inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                    long chunkOff = fileOffset - curOffset + inodeData.getOffset();
                    long chunkSize = fileSize;
                    Mono<Tuple3<Long, Long, String>> res = Mono.just(true)
                            .flatMap(b -> Node.getInstance().getChunk(inodeData.fileName))
                            .flatMap(chunk -> searchInodeData(value, chunk.getChunkList(), chunkOff, chunkSize, storage));

                    chunkRes.add(res);
                }
            }

            curOffset = curEnd;
        }

        if (chunkRes.isEmpty()) {
            return Mono.just(new Tuple3<>(-1L, -1L, ""));
        } else {
            return Flux.merge(Flux.fromStream(chunkRes.stream()), 1, 1)
                    .collectList()
                    .map(l -> l.stream()
                            .filter(t -> t.var1 != -1L)
                            .findFirst()
                            .map(t -> {
                                return new Tuple3<>(fileOffset, fileSize, t.var3);
                            })
                            .orElse(new Tuple3<>(-1L, -1L, "")));
        }
    }
}
