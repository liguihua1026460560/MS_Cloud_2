package com.macrosan.filesystem.cache.model;

import com.google.common.io.BaseEncoding;
import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.cache.InodeOperator;
import com.macrosan.filesystem.utils.ChunkFileUtils;
import com.macrosan.message.jsonmsg.ChunkFile;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.cache.Md5DigestPool;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.ROCKS_CHUNK_FILE_KEY;
import static com.macrosan.filesystem.FsConstants.S_IFLNK;
import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.filesystem.utils.ChunkFileUtils.MAX_CHUNK_SIZE;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;

@Log4j2
public class ChunkModel implements DataModel {
    private Mono<ChunkFile> updateChunk(Inode inode, List<ChunkFile.UpdateChunkOpt> list, String chunkKey,
                                        ChunkFile chunkFile, Map<String, List<Inode.InodeData>> needDelete) {
        if (chunkFile != null && chunkFile.size < 0) {
            //error chunk
            return Mono.just(chunkFile);
        }

        for (ChunkFile.UpdateChunkOpt opt : list) {
            switch (opt.getOpt()) {
                case 0:
                    chunkFile = ((ChunkFile.CreateChunkOpt) opt).chunkFile;
                    break;
                case 1:
                    if (chunkFile == null) {
                        return ChunkFileUtils.getChunk(inode.getBucket(), chunkKey)
                                .flatMap(chunkFile0 -> updateChunk(inode, list, chunkKey, chunkFile0, needDelete));
                    }
                    ChunkFile.AppendChunkOpt appendChunkOpt = (ChunkFile.AppendChunkOpt) opt;
                    Inode.InodeData data = ((ChunkFile.AppendChunkOpt) opt).data;
                    Inode.InodeData lastInodeData = chunkFile.getChunkList().get(chunkFile.getChunkList().size() - 1);

                    /**
                     * 此种情况是，数据块已写入成功，且chunk中size更新成功，但是inode元数据未更新成功，后续有数据块重新传输，inode.size比预期要小，
                     * 然后直接进行了append，此种情况不需要拼接。
                     */
                    if (null != lastInodeData.getEtag()
                            && lastInodeData.getEtag().equals(data.getEtag())
                            && chunkFile.getSize() == (data.getSize() + appendChunkOpt.offset)) {
                        break;
                    }

                    //此种情况是，chunk中size更新成功，但是inode元数据未更新成功，导致inode.size比预期 要小，客户端发送的reqOffset=chunk.size,然后拼接了holeFile，此种情况不需要拼接
                    if (StringUtils.isBlank(data.getFileName()) && chunkFile.getSize() == (data.getSize() + appendChunkOpt.offset)) {
                        break;
                    }

                    //此种情况是，chunk中size更新成功，但是inode元数据未更新成功，导致inode.size比预期 要小，客户端发送的reqOffset>chunk.size 然后拼接的holeFile过大，此种情况需要修改holeFile的大小
                    if (chunkFile.getSize() > appendChunkOpt.offset && StringUtils.isBlank(data.getFileName())) {
                        data.size -= (chunkFile.getSize() - appendChunkOpt.offset);
                    }

                    chunkFile.getChunkList().add(data);
                    chunkFile.size += data.size;
                    break;
                case 2:
                    if (chunkFile == null) {
                        return ChunkFileUtils.getChunk(inode.getBucket(), chunkKey)
                                .flatMap(chunkFile0 -> updateChunk(inode, list, chunkKey, chunkFile0, needDelete));
                    }
                    modifyChunk(chunkFile, ((ChunkFile.ModifyChunkOpt) opt).data, ((ChunkFile.ModifyChunkOpt) opt).reqOffset, chunkKey, needDelete);
                    break;
                case 3:
                    if (chunkFile == null) {
                        return ChunkFileUtils.getChunk(inode.getBucket(), chunkKey)
                                .flatMap(chunkFile0 -> updateChunk(inode, list, chunkKey, chunkFile0, needDelete));
                    }
                    boolean flag = false;

                    for (Inode.InodeData chunk : chunkFile.getChunkList()) {
                        if (((ChunkFile.CheckAndModifyChunkOpt) opt).check(Json.encode(chunk))) {
                            log.debug("oldInodeData={}", ((ChunkFile.CheckAndModifyChunkOpt) opt).oldInodeData);
                            log.debug("currentInodeData={}", chunk);
                            flag = true;
                            break;
                        }
                    }
                    String fileName = Json.decodeValue(((ChunkFile.CheckAndModifyChunkOpt) opt).oldInodeData, Inode.InodeData.class).getFileName();
                    // 创建更新inodeData记录map
                    if (inode.getUpdateInodeDataStatus() == null)
                        inode.setUpdateInodeDataStatus(new ConcurrentHashMap<>());
                    if (flag) {
                        modifyChunk(chunkFile, ((ChunkFile.CheckAndModifyChunkOpt) opt).data, ((ChunkFile.CheckAndModifyChunkOpt) opt).reqOffset, chunkKey, needDelete);
                        inode.getUpdateInodeDataStatus().compute(fileName, (k, v) -> true);
                    } else {
                        inode.getUpdateInodeDataStatus().compute(fileName, (k, v) -> v == null ? false : v);
                    }
                    break;
                case 4:
                    if (chunkFile == null) {
                        return ChunkFileUtils.getChunk(inode.getBucket(), chunkKey)
                                .flatMap(chunkFile0 -> updateChunk(inode, list, chunkKey, chunkFile0, needDelete));
                    }

                    ChunkFile.ErrorChunkOpt errorChunkOpt = (ChunkFile.ErrorChunkOpt) opt;
                    String oldDataStr = errorChunkOpt.oldInodeData;
                    String fileName0 = Json.decodeValue(oldDataStr, Inode.InodeData.class).getFileName();
                    // 创建更新inodeData记录map
                    if (inode.getUpdateInodeDataStatus() == null)
                        inode.setUpdateInodeDataStatus(new ConcurrentHashMap<>());
                    inode.getUpdateInodeDataStatus().compute(fileName0, (k, v) -> v == null ? false : v);
                    break;
                default:
                    log.error("no such update chunk opt {}", opt);
            }
        }

        return Mono.just(chunkFile);
    }

    private void modifyChunk(ChunkFile chunkFile, Inode.InodeData opt, long opt1, String chunkKey, Map<String, List<Inode.InodeData>> needDelete) {
        List<Inode.InodeData> oldList = new ArrayList<>(chunkFile.getChunkList());
        Inode.InodeData data = opt;
        long reqOffset = opt1;
        boolean[] hasCovered = new boolean[1];
        if (!chunkFile.hasDeleteFiles.isEmpty()
                && chunkFile.hasDeleteFiles.contains(opt.getFileName())) {
            return;
        }
        chunkFile.size = ChunkFileUtils.updateChunkData(reqOffset, data, chunkFile.getChunkList(), chunkFile.size, hasCovered);
        if (hasCovered[0]) {
            addCoveredFile(oldList, chunkFile.getChunkList(), chunkKey, needDelete);
            needDelete.computeIfPresent(chunkKey, (k, v) -> {
                if (!v.isEmpty()) {
                    for (Inode.InodeData inodeData : v) {
                        if (chunkFile.hasDeleteFiles.size() >= 16) {
                            chunkFile.hasDeleteFiles.pollFirst();
                        }
                        chunkFile.hasDeleteFiles.add(inodeData.getFileName());
                    }
                }
                return v;
            });
        }
    }

    private void addCoveredFile(List<Inode.InodeData> oldList, List<Inode.InodeData> newList, String chunkKey, Map<String, List<Inode.InodeData>> needDelete) {
        Set<String> newFileNames = newList.stream().map(l -> l.fileName).collect(Collectors.toSet());
        for (Inode.InodeData inodeData : oldList) {
            if (!newFileNames.contains(inodeData.getFileName()) && StringUtils.isNotBlank(inodeData.getFileName())) {
                needDelete.computeIfPresent(chunkKey, (k, v) -> {
                    v.add(inodeData);
                    return v;
                });
            }
        }
    }

    public Mono<Inode> updateChunk(Inode inode, Map<String, List<Inode.InodeData>> needDelete) {
        if (inode.getUpdateChunk() != null) {
            Map<String, List<ChunkFile.UpdateChunkOpt>> updateChunk = inode.getUpdateChunk();

            List<Mono<Tuple2<String, ChunkFile>>> chunkResList = new LinkedList<>();
            for (String chunkKey : updateChunk.keySet()) {
                List<ChunkFile.UpdateChunkOpt> list = updateChunk.get(chunkKey);
                needDelete.computeIfAbsent(chunkKey, k -> new LinkedList<>());
                Mono<ChunkFile> chunkRes = updateChunk(inode, list, chunkKey, null, needDelete);
                chunkResList.add(Mono.just(chunkKey).zipWith(chunkRes));
            }

            Mono<Tuple2<String, ChunkFile>>[] merge = chunkResList.toArray(new Mono[0]);

            return Flux.merge(merge)
                    .collectList()
                    .map(chunkList -> {
                        Map<String, ChunkFile> chunkFileMap = new HashMap<>();
                        for (Tuple2<String, ChunkFile> tuple2 : chunkList) {
                            if (tuple2.getT2().size < 0) {
                                return ERROR_INODE;
                            }

                            chunkFileMap.put(tuple2.getT1(), tuple2.getT2().setVersionNum(inode.getVersionNum()));
                        }
                        MessageDigest digest = Md5DigestPool.acquire();
                        List<Inode.InodeData> inodeDataList = inode.getInodeData();
                        for (Inode.InodeData inodeData : inodeDataList) {
                            String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), inodeData.fileName);
                            if (chunkFileMap.get(chunkKey) != null) {
                                ChunkFile chunkFile = chunkFileMap.get(chunkKey);
                                List<Inode.InodeData> chunkDataList = chunkFile.getChunkList();
                                inodeData.chunkNum = chunkDataList.size();
                                for (Inode.InodeData chunkData : chunkDataList) {
                                    if (StringUtils.isNotBlank(chunkData.getEtag())) {
                                        digest.update(BaseEncoding.base16().decode(chunkData.getEtag().toUpperCase()));
                                    }
                                }
                            } else if (inodeData.chunkNum == 0) {
                                if (StringUtils.isNotBlank(inodeData.getEtag())) {
                                    digest.update(BaseEncoding.base16().decode(inodeData.getEtag().toUpperCase()));
                                }
                            }
                        }
                        if (inodeDataList.size() > 0) {
                            Inode.InodeData last = inodeDataList.get(inodeDataList.size() - 1);
                            if ((inode.getMode() & S_IFMT) != S_IFLNK && inode.getLinkN() > 1
                                    && last.fileName.contains("partNum")) {
                                inode.setReference(inode.getObjName());
                            }
                            last.setEtag(Hex.encodeHexString(digest.digest()));
                        }
                        Md5DigestPool.release(digest);
                        inode.setUpdateChunk(null);
                        inode.setChunkFileMap(chunkFileMap);
                        return inode;
                    });
        } else {
            return Mono.just(inode);
        }
    }

    public void append(List<Inode.InodeData> list, Inode.InodeData data, Inode inode, long reqOffset) {
        if (list.isEmpty()) {
            if (StringUtils.isBlank(data.fileName)) {
                ChunkFileUtils.creatMultiHole(list, Inode.InodeData.newHoleFileList(data.size, true), inode);
            } else {
                ChunkFileUtils.createData(list, data, inode);
            }
        } else {
            Inode.InodeData last = list.get(list.size() - 1);
            // 如果数据存在chunk则进行追加；若last.fileName为chunk，且该chunk未满，则可追加chunk；若为其它，则应当创建新的chunk
            if ((!last.full(inode.getSize()) || StringUtils.isBlank(data.fileName)) && last.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                if (StringUtils.isBlank(data.fileName)) {
                    // 若data是holeFile且last未满，hole小则拼接last，hole大则部分拼接last，剩余部分切分为多个hole
                    long holeSize = data.size;
                    if (!last.full(inode.getSize())) {
                        if (last.size + holeSize <= MAX_CHUNK_SIZE) {
                            ChunkFileUtils.lastFileAppendHole(last, holeSize, inode, reqOffset);
                        } else {
                            long lastSize = last.size;
                            ChunkFileUtils.lastFileAppendHole(last, MAX_CHUNK_SIZE - lastSize, inode, reqOffset);
                            List<Inode.InodeData> holeFileList = Inode.InodeData.newHoleFileList(lastSize + holeSize - MAX_CHUNK_SIZE, true);
                            ChunkFileUtils.creatMultiHole(list, holeFileList, inode);
                        }
                    } else {
                        ChunkFileUtils.creatMultiHole(list, Inode.InodeData.newHoleFileList(holeSize, true), inode);
                    }
                } else {
                    // 若data不是holeFile则与last拼接
                    ChunkFileUtils.lastFileAppendData(last, data, inode, reqOffset);
                }
            } else if (!last.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                // inodeData中的file非chunk_file时进行inodeData类型更新
                // 1. 先对旧的inodeData删除，并更新成chunk类型的inodeData，
                // 2. 然后在添加追加的数据
                // 删除旧的inodeData
                list.remove(last);
                // 转换inodeData类型，从file->chunk
                String chunkKey = ChunkFile.getChunkKey(last.fileName);
                Inode.InodeData chunk = ChunkFileUtils.newChunk(last.fileName, last, inode)
                        .setChunkNum(1)
                        .setSize(last.size);

                ChunkFile chunkFile = new ChunkFile(inode.getNodeId(), inode.getBucket(), inode.getObjName(),
                        inode.getVersionId(), VersionUtil.getVersionNum(), last.size);
                List<Inode.InodeData> chunkList = chunkFile.getChunkList();
                chunkList.add(last);
                // 更新chunk file
                Inode.updateChunk(inode, chunkKey, new ChunkFile.CreateChunkOpt(chunkFile));

                // 插入新的InodeData
                list.add(chunk);
                // 添加chunk file
                Inode.updateChunk(inode, chunkKey, new ChunkFile.AppendChunkOpt(data, reqOffset));
                chunk.size += data.size;
                chunk.chunkNum++;
            } else {
                ChunkFileUtils.createData(list, data, inode);
            }

        }
    }

    public void updateInodeData(long reqOffset, Inode.InodeData data, Inode inode, InodeOperator.UpdateArgs args, String oldInodeData) {
        long reqEnd = reqOffset + data.size;
        List<Inode.InodeData> list = inode.getInodeData();
        if (reqOffset >= inode.getSize()) {
            if (StringUtils.isNotEmpty(oldInodeData)) {
                // 文件被删除或size被修改过等异常情况
                Inode.updateChunk(inode, ChunkFile.getChunkKey(data.fileName), new ChunkFile.ErrorChunkOpt(oldInodeData));
            } else {
                // 写入位置大于当前文件末端
                if (reqOffset > inode.getSize()) {
                    if (list.isEmpty()) {
                        // 将holeFile切分成多个5G数据块，不满5G的部分与data拼接生成一个chunkFile
                        List<Inode.InodeData> holeFileList = Inode.InodeData.newHoleFileList(reqOffset - inode.getSize(), true);
                        ChunkFileUtils.creatMultiHole(list, holeFileList, inode);
                        append(list, data, inode, reqOffset);
                    } else {
                        // 文件末端数据块为文件端上传，以)为前缀
                        append(list, Inode.InodeData.newHoleFile(reqOffset - inode.getSize()), inode, inode.getSize());
                        append(list, data, inode, reqOffset);
                    }
                } else {
                    // 写入位置等于当前文件末端
                    append(list, data, inode, reqOffset);
                }
            }
        } else {
            // 写入位置与当前文件数据块有交集
            args.needDelete = true;
            ListIterator<Inode.InodeData> listIterator = list.listIterator();
            long curOffset = 0L;
            boolean addData = false;
            if ((inode.getMode() & S_IFMT) != S_IFLNK && inode.getLinkN() > 1
                    && !list.isEmpty() && list.get(0).fileName.contains("partNum")) {
                inode.setReference(inode.getObjName());
            }
            while (listIterator.hasNext()) {
                Inode.InodeData inodeData = listIterator.next();
                long curEnd = inodeData.size + curOffset;
                if (curEnd < reqOffset) {
                    curOffset = curEnd;
                } else if (reqEnd < curOffset) {
                    break;
                } else {
                    if (addData) {
                        if (reqEnd < curEnd) {
                            long newSize = curEnd - reqEnd;
                            inodeData.offset += inodeData.size - newSize;
                            inodeData.size = newSize;
                        } else {
                            listIterator.remove();
                        }
                    } else {
                        if (inodeData.getFileName().startsWith(ROCKS_CHUNK_FILE_KEY)) {
                            if (!inodeData.full(inode.getSize()) || reqEnd <= curEnd) {
                                String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), inodeData.fileName);
                                long chunkOffset = reqOffset - curOffset + inodeData.offset;
                                ;
                                // 加入CheckChunkOpt，判断数据是否被修改过，保证chunk更新原子性
                                if (StringUtils.isNotEmpty(oldInodeData)) {
                                    Inode.updateChunk(inode, chunkKey, new ChunkFile.CheckAndModifyChunkOpt(oldInodeData, chunkOffset, data));
                                } else {
                                    Inode.updateChunk(inode, chunkKey, new ChunkFile.ModifyChunkOpt(chunkOffset, data));
                                }
                                if (reqEnd > curEnd) {
                                    inodeData.size += reqEnd - curEnd;
                                }

                                if (chunkOffset < 0) {
                                    inodeData.size -= chunkOffset;
                                }

                                inodeData.chunkNum++;
                                addData = true;
                            } else {
                                if (curOffset > reqOffset) {
                                    listIterator.remove();
                                } else {
                                    inodeData.size = reqOffset - curOffset;
                                }
                            }
                        } else {
                            listIterator.remove();
                            String chunkKey = ChunkFile.getChunkKey(inodeData.fileName);
                            Inode.InodeData chunk = ChunkFileUtils.newChunk(inodeData.fileName, inodeData, inode)
                                    .setChunkNum(1)
                                    .setSize(inodeData.size);

                            ChunkFile chunkFile = new ChunkFile(inode.getNodeId(), inode.getBucket(), inode.getObjName(),
                                    inode.getVersionId(), VersionUtil.getVersionNum(), inodeData.size);
                            List<Inode.InodeData> chunkList = chunkFile.getChunkList();
                            chunkList.add(inodeData);
                            Inode.updateChunk(inode, chunkKey, new ChunkFile.CreateChunkOpt(chunkFile));
                            listIterator.add(chunk);

                            long chunkOffset = reqOffset - curOffset;
                            // 加入CheckChunkOpt，判断数据是否被修改过，保证chunk更新原子性
                            if (StringUtils.isNotEmpty(oldInodeData)) {
                                Inode.updateChunk(inode, chunkKey, new ChunkFile.CheckAndModifyChunkOpt(oldInodeData, chunk.offset, data));
                            } else {
                                Inode.updateChunk(inode, chunkKey, new ChunkFile.ModifyChunkOpt(chunkOffset, data));
                            }
                            if (reqEnd > curEnd) {
                                chunk.size += reqEnd - curEnd;
                            }

                            if (chunkOffset < 0) {
                                chunk.size -= chunkOffset;
                            }

                            chunk.chunkNum++;

                            addData = true;
                        }
                    }


                    curOffset = curEnd;
                }
            }

            if (!addData) {
                if (StringUtils.isNotEmpty(oldInodeData)) {
                    // inode中size正常，inodeData数据异常了
                    Inode.updateChunk(inode, ChunkFile.getChunkKey(data.fileName), new ChunkFile.ErrorChunkOpt(oldInodeData));
                } else {
                    append(list, data, inode, reqOffset);
                }
            }
        }


        if (reqEnd > inode.getSize()) {
            inode.setSize(reqEnd);
        }
    }
}
