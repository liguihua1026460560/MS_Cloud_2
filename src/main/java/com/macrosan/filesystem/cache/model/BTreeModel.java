package com.macrosan.filesystem.cache.model;

import com.google.common.io.BaseEncoding;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.cache.InodeOperator.UpdateArgs;
import com.macrosan.filesystem.utils.ChunkFileUtils;
import com.macrosan.message.jsonmsg.ChunkFile;
import com.macrosan.message.jsonmsg.ChunkFile.CheckAndModifyChunkOpt;
import com.macrosan.message.jsonmsg.ChunkFile.ModifyChunkOpt;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.Inode.InodeData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.cache.Md5DigestPool;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.filesystem.FsConstants.S_IFLNK;
import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.jsonmsg.ChunkFile.CHECK_FAIL_CHUNK;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;

@Log4j2
public class BTreeModel implements DataModel {
    private static final int SPLIT_NUM = 128;

    private static final UpdateArgs APPEND_ARGS = new UpdateArgs();

    @Override
    public void append(List<InodeData> list, InodeData data, Inode inode, long reqOffset) {
        updateInodeData(reqOffset, data, inode, APPEND_ARGS, "");
    }

    public void updateInodeDataStorage(long reqOffset, InodeData update, List<InodeData> list, Inode inode) {
        long reqEnd = reqOffset + update.size;

        boolean chunk = !list.isEmpty() && list.get(0).chunk();

        if (chunk) {
            boolean mark = false;

            long curOffset = 0L;
            for (InodeData data : list) {
                long curEnd = curOffset + data.size;

                if (reqEnd < curOffset) {
                    break;
                } else if (curEnd >= reqOffset) {
                    String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), data.fileName);
                    long chunkReqOff = reqOffset - curOffset + data.offset;
                    Inode.updateChunk(inode, chunkKey, new CheckAndModifyChunkOpt("oldInodeData", chunkReqOff, update));
                    mark = true;
                }

                curOffset = curEnd;
            }

            if (!mark) {
                markCheckFail(inode, update);
            }
        } else {
            boolean mark = false;
            for (InodeData data : list) {
                if (data.fileName.replace("/split/", "").equals(update.fileName)) {
                    data.storage = update.storage;
                    mark = true;
                }
            }

            if (mark) {
                markCheckSuccess(inode, update);
            } else {
                markCheckFail(inode, update);
            }
        }
    }

    @Override
    public void updateInodeData(long reqOffset, InodeData update, Inode inode, UpdateArgs args,
                                String oldInodeData) {

        if (StringUtils.isNotBlank(oldInodeData)) {
            updateInodeDataStorage(reqOffset, update, inode.getInodeData(), inode);
            return;
        }

        if (args.needDeleteMap == null) {
            args.needDeleteMap = new ConcurrentHashMap<>();
        }

        List<InodeData> list = inode.getInodeData();
        boolean chunk = !list.isEmpty() && list.get(0).chunk();

        if (chunk) {
            if (reqOffset >= inode.getSize()) {
                InodeData lastChunk = list.get(list.size() - 1);
                String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), lastChunk.fileName);

                //append
                long holeSize = reqOffset - inode.getSize();
                if (holeSize > 0) {
                    InodeData hole = InodeData.newHoleFile(holeSize);
                    Inode.updateChunk(inode, chunkKey, new ChunkFile.AppendChunkOpt(hole, inode.getSize()));
                    lastChunk.chunkNum++;
                }
                Inode.updateChunk(inode, chunkKey, new ChunkFile.AppendChunkOpt(update, reqOffset));
                lastChunk.chunkNum++;

                lastChunk.size += update.size + holeSize;
            } else {
                updateChunkRange(reqOffset, update, inode.getInodeData(), inode, args.needDeleteMap);
            }
        } else {
            if (reqOffset < inode.getSize()) {
                updateRange(reqOffset, update, inode.getInodeData(), inode, args.needDeleteMap);
            } else {
                //append
                long holeSize = reqOffset - inode.getSize();
                if (holeSize > 0) {
                    InodeData hole = InodeData.newHoleFile(holeSize);
                    list.add(hole);
                }
                list.add(update);
            }

            if (list.size() > SPLIT_NUM) {
                inodeSplitToChunks(list, inode);
            }
        }

        if (reqOffset + update.size > inode.getSize()) {
            inode.setSize(reqOffset + update.size);
        }
    }

    private void updateChunkRange(long reqOffset, InodeData update, List<InodeData> list, Inode inode,
                                  Map<String, List<InodeData>> needDelete) {
        boolean addData = false;
        ListIterator<InodeData> listIterator = list.listIterator();
        long curOffset = 0L;
        long reqEnd = reqOffset + update.size;

        while (listIterator.hasNext()) {
            InodeData cur = listIterator.next();
            long curEnd = curOffset + cur.size;

            if (curEnd < reqOffset) {
                curOffset = curEnd;
            } else if (reqEnd < curOffset) {
                break;
            } else {
                if (addData) {
                    if (reqEnd < curEnd) {
                        long newSize = curEnd - reqEnd;
                        cur.offset += cur.size - newSize;
                        //TODO 此时可以删除chunkFile中部分文件
                        cur.size = newSize;
                    } else {
                        needDelete.computeIfAbsent("chunk", k -> new LinkedList<>()).add(cur);
                        listIterator.remove();
                    }
                } else {
                    if (reqEnd > curEnd) {
                        cur.size = reqEnd - curOffset;
                    }

                    String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), cur.fileName);
                    long chunkReqOff = reqOffset - curOffset + cur.offset;
                    Inode.updateChunk(inode, chunkKey, new ModifyChunkOpt(chunkReqOff, update));
                    addData = true;

                }

                curOffset = curEnd;
            }
        }

        if (!addData) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "updateChunk fail");
        }
    }

    private void updateRange(long reqOffset, InodeData update, List<InodeData> dataList, Inode inode,
                             Map<String, List<InodeData>> needDelete) {
        boolean addData = false;
        ListIterator<InodeData> listIterator = dataList.listIterator();
        long curOffset = 0L;
        long reqEnd = reqOffset + update.size;

        List<InodeData> deleteList = new LinkedList<>();

        while (listIterator.hasNext()) {
            InodeData cur = listIterator.next();
            long curEnd = curOffset + cur.size;

            if (curEnd < reqOffset) {
                curOffset = curEnd;
            } else if (reqEnd < curOffset) {
                break;
            } else {
                if (addData) {
                    if (reqEnd < curEnd) {
                        long newSize = curEnd - reqEnd;
                        cur.offset += cur.size - newSize;
                        cur.size = newSize;
                    } else {
                        deleteList.add(cur);
                        listIterator.remove();
                    }
                } else {
                    if (reqOffset == curOffset) {
                        deleteList.add(cur);
                        listIterator.remove();
                    } else {
                        cur.size0 += cur.size - (reqOffset - curOffset);
                        cur.size = reqOffset - curOffset;
                    }

                    listIterator.add(update);
                    addData = true;

                    if (reqEnd < curEnd) {
                        InodeData next = new InodeData();
                        long newSize = curEnd - reqEnd;
                        //cur.size可能被修改，重新计算最初的cur.size
                        next.offset = cur.offset + (curEnd - curOffset) - newSize;
                        next.size = newSize;
                        next.etag = cur.etag;
                        next.fileName = cur.fileName;
                        next.storage = cur.storage;
                        next.chunkNum = cur.chunkNum;
                        next.size0 = cur.size0 + cur.size + cur.offset - next.offset - next.size;

                        listIterator.add(next);
                    }
                }

                curOffset = curEnd;
            }
        }

        if (!addData) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "updateRange fail.nodeId:" + inode.getNodeId()
                    + ", bucket:" + inode.getBucket()
                    + ", objName:" + inode.getObjName()
                    + ", reqOffset:" + reqOffset);
        }

        if (!deleteList.isEmpty()) {
            for (InodeData delete : deleteList) {
                boolean check = true;
                for (InodeData data : dataList) {
                    if (data.fileName.equals(delete.fileName)) {
                        check = false;
                        break;
                    }
                }
                if (check) {
                    String deleteKey = "";
                    if (delete.fileName.endsWith("/split/")) {
                        deleteKey = "split";
                    }
                    needDelete.computeIfAbsent(deleteKey, k -> new LinkedList<>()).add(delete);
                }
            }
        }
    }

    private static long[] splitList(List<InodeData> list1, List<InodeData> list2, List<InodeData> list) {
        int i = 0;
        int mid = list.size() / 2;
        ListIterator<InodeData> iterator = list.listIterator();
        long size1 = 0L;
        long size2 = 0L;
        Set<String> fileNameSet1 = new HashSet<>(mid);
        Set<String> dupFileNames = new HashSet<>();

        while (iterator.hasNext()) {
            InodeData data = iterator.next();
            if (i++ < mid) {
                if (list1 != list) {
                    list1.add(data);
                    iterator.remove();
                }
                fileNameSet1.add(data.getFileName());
                size1 += data.size;
            } else {
                list2.add(data);
                size2 += data.size;
                iterator.remove();

                //同一个数据块被分裂到 两个不通的chunk中。可能会多次触发删除
                if (StringUtils.isNotBlank(data.fileName) && fileNameSet1.contains(data.fileName)) {
                    dupFileNames.add(data.fileName);
                }
            }
        }

        for (String dupFileName : dupFileNames) {
            for (InodeData data : list1) {
                if (dupFileName.equals(data.fileName)) {
                    if (!dupFileName.endsWith("/split/")) {
                        data.fileName += "/split/";
                    }
                }
            }

            for (InodeData data : list2) {
                if (dupFileName.equals(data.fileName)) {
                    if (!dupFileName.endsWith("/split/")) {
                        data.fileName += "/split/";
                    }
                }
            }
        }

        return new long[]{size1, size2};
    }

    private static void inodeSplitToChunks(List<InodeData> list, Inode inode) {
        boolean chunk = list.get(0).chunk();
        List<InodeData> list1 = new LinkedList<>();
        List<InodeData> list2 = new LinkedList<>();

        long[] splitSize = splitList(list1, list2, list);
        long size1 = splitSize[0];
        long size2 = splitSize[1];

        String fileName1;
        String fileName2;
        InodeData first1 = list1.get(0);
        InodeData first2 = list2.get(0);

        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(inode.getBucket());
        if (chunk) {
            fileName1 = Utils.getHoleFileName(metaPool, inode.getBucket(), getRequestId());
            fileName2 = Utils.getHoleFileName(metaPool, inode.getBucket(), getRequestId());
        } else {
            fileName1 = StringUtils.isBlank(first1.getFileName()) ?
                    Utils.getHoleFileName(metaPool, inode.getBucket(), getRequestId()) : first1.getFileName();
            fileName2 = StringUtils.isBlank(first2.getFileName()) ?
                    Utils.getHoleFileName(metaPool, inode.getBucket(), getRequestId()) : first2.getFileName();

            if (fileName1.equals(fileName2)) {
                fileName2 = Utils.getHoleFileName(metaPool, inode.getBucket(), getRequestId());
            }
        }

        InodeData chunk1 = ChunkFileUtils.newChunk(fileName1, first1, inode)
                .setSize(size1)
                .setChunkNum(list1.size());

        InodeData chunk2 = ChunkFileUtils.newChunk(fileName2, first2, inode)
                .setSize(size2)
                .setChunkNum(list2.size());

        String chunkKey1 = ChunkFile.getChunkKey(fileName1);
        ChunkFile chunkFile1 = new ChunkFile(inode.getNodeId(), inode.getBucket(), inode.getObjName(),
                inode.getVersionId(), VersionUtil.getVersionNum(), size1);
        chunkFile1.getChunkList().addAll(list1);
        Inode.updateChunk(inode, chunkKey1, new ChunkFile.CreateChunkOpt(chunkFile1));

        String chunkKey2 = ChunkFile.getChunkKey(fileName2);
        ChunkFile chunkFile2 = new ChunkFile(inode.getNodeId(), inode.getBucket(), inode.getObjName(),
                inode.getVersionId(), VersionUtil.getVersionNum(), size2);
        chunkFile2.getChunkList().addAll(list2);
        Inode.updateChunk(inode, chunkKey2, new ChunkFile.CreateChunkOpt(chunkFile2));

        list.add(chunk1);
        list.add(chunk2);
    }

    private static long chunkSplitToChunks(List<InodeData> curList, InodeData cur, ListIterator<InodeData> parentIte, Inode inode) {
        LinkedList<InodeData> nextList = new LinkedList<>();
        long[] splitSize = splitList(curList, nextList, curList);
        long size1 = splitSize[0];
        long size2 = splitSize[1];

        String nextFileName = Utils.getHoleFileName(StoragePoolFactory.getMetaStoragePool(inode.getBucket()),
                inode.getBucket(), getRequestId());

        long nextOff = Math.max(0, cur.offset - size1);
        InodeData next = ChunkFileUtils.newChunk(nextFileName, nextList.getFirst(), inode)
                .setSize(size2 - nextOff)
                .setOffset(nextOff)
                .setChunkNum(nextList.size());

        parentIte.add(next);

        long curOff = Math.min(size1, cur.offset);
        cur.setOffset(curOff)
                .setSize(size1 - curOff)
                .setChunkNum(curList.size());

        ChunkFile nextChunkFile = new ChunkFile(inode.getNodeId(), inode.getBucket(), inode.getObjName(),
                inode.getVersionId(), VersionUtil.getVersionNum(), size2);
        nextChunkFile.setChunkList(nextList);
        String nextChunkKey = ChunkFile.getChunkKey(nextFileName);
        Inode.updateChunk(inode, nextChunkKey, new ChunkFile.CreateChunkOpt(nextChunkFile));
        return size1;
    }

    private Mono<ChunkFile> updateChunk(Inode inode, List<ChunkFile.UpdateChunkOpt> list, String chunkKey,
                                        ChunkFile chunkFile, Map<String, List<InodeData>> needDelete) {
        if (chunkFile != null && chunkFile.size < 0) {
            //error chunk
            return Mono.just(chunkFile);
        }

        if (chunkFile == null && list.get(0).getOpt() != 0) {
            return ChunkFileUtils.getChunk(inode.getBucket(), chunkKey)
                    .flatMap(chunkFile0 -> updateChunk(inode, list, chunkKey, chunkFile0, needDelete));
        }

        for (ChunkFile.UpdateChunkOpt opt : list) {
            switch (opt.getOpt()) {
                case 0:
                    chunkFile = ((ChunkFile.CreateChunkOpt) opt).chunkFile;
                    break;
                case 1:
                    ChunkFile.AppendChunkOpt appendChunkOpt = (ChunkFile.AppendChunkOpt) opt;
                    InodeData appendData = appendChunkOpt.data;

                    boolean appendChunk = chunkFile.getChunkList().get(0).chunk();
                    if (appendChunk) {
                        InodeData lastChunk = chunkFile.getChunkList().getLast();
                        String lastChunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), lastChunk.fileName);
                        Inode.updateChunk(inode, lastChunkKey, new ChunkFile.AppendChunkOpt(appendData, appendChunkOpt.offset));
                        chunkFile.size += appendData.size;
                        lastChunk.size += appendData.size;
                        lastChunk.chunkNum++;
                    } else {
                        chunkFile.getChunkList().add(appendData);
                        chunkFile.size += appendData.size;
                    }

                    break;
                case 2:
                    boolean chunk = chunkFile.getChunkList().getFirst().chunk();
                    ModifyChunkOpt modifyChunkOpt = ((ModifyChunkOpt) opt);
                    long reqOffset = modifyChunkOpt.reqOffset;
                    InodeData modifyData = modifyChunkOpt.data;

                    if (chunk) {
                        updateChunkRange(reqOffset, modifyData, chunkFile.getChunkList(), inode, needDelete);
                    } else {
                        updateRange(reqOffset, modifyData, chunkFile.getChunkList(), inode, needDelete);
                    }

                    if (reqOffset + modifyData.size > chunkFile.size) {
                        chunkFile.size = reqOffset + modifyData.size;
                    }

                    break;
                case 3:
                    CheckAndModifyChunkOpt checkAndModifyChunkOpt = ((CheckAndModifyChunkOpt) opt);
                    updateInodeDataStorage(checkAndModifyChunkOpt.reqOffset, checkAndModifyChunkOpt.data, chunkFile.getChunkList(), inode);
                    break;
                default:
                    log.error("no such update chunk opt {}", opt);
            }
        }

        if (chunkFile == null) {
            return Mono.just(CHECK_FAIL_CHUNK);
        }

        inode.getChunkFileMap().put(chunkKey, chunkFile);

        return Mono.just(chunkFile);
    }

    public Mono<Inode> checkSplit(Inode inode, Map<String, List<InodeData>> needDelete) {
        if (inode.getInodeData().size() > SPLIT_NUM) {
            inodeSplitToChunks(inode.getInodeData(), inode);
            return updateChunk(inode, needDelete, true);
        }

        Map<String, ChunkFile> chunkFileMap = inode.getChunkFileMap();
        if (chunkFileMap != null) {
            for (String chunkKey : chunkFileMap.keySet()) {
                ChunkFile chunkFile = chunkFileMap.get(chunkKey);
                if (chunkFile.getChunkList().size() > SPLIT_NUM) {
                    String parentKey = null;
                    ListIterator<InodeData> parentIte = null;
                    InodeData cur = null;

                    parentIte = inode.getInodeData().listIterator();
                    while (parentIte.hasNext()) {
                        cur = parentIte.next();
                        String nextChunkKey = StringUtils.isBlank(cur.fileName) ? "" : ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), cur.fileName);
                        if (nextChunkKey.equals(chunkKey)) {
                            parentKey = "";
                            break;
                        }
                    }

                    if (parentKey == null) {
                        for (String parent : chunkFileMap.keySet()) {
                            if (parentKey != null) {
                                break;
                            }
                            ChunkFile mayParentChunk = chunkFileMap.get(parent);
                            parentIte = mayParentChunk.getChunkList().listIterator();
                            while (parentIte.hasNext()) {
                                cur = parentIte.next();
                                String nextChunkKey = StringUtils.isBlank(cur.fileName) ? "" : ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), cur.fileName);
                                if (nextChunkKey.equals(chunkKey)) {
                                    parentKey = parent;
                                    break;
                                }
                            }
                        }
                    }

                    if (parentKey != null) {
                        chunkFile.size = chunkSplitToChunks(chunkFile.getChunkList(), cur, parentIte, inode);
                        inode.getChunkFileMap().put(chunkKey, chunkFile);
                        return updateChunk(inode, needDelete, true);
                    } else {
                        log.error("parentKey not found {}", chunkKey);
                        return Mono.just(inode);
                    }
                }
            }
        }

        return Mono.just(inode);
    }

    @Override
    public Mono<Inode> updateChunk(Inode inode, Map<String, List<InodeData>> needDelete) {
        return updateChunk(inode, needDelete, false);
    }

    public Mono<Inode> updateChunk(Inode inode, Map<String, List<InodeData>> needDelete, boolean splited) {
        if (inode.getUpdateChunk() == null || inode.getUpdateChunk().isEmpty()) {
            if (!splited) {
                return checkSplit(inode, needDelete);
            }

            return Mono.just(inode);
        }

        Map<String, List<ChunkFile.UpdateChunkOpt>> updateChunk = inode.getUpdateChunk();
        if (inode.getChunkFileMap() == null) {
            inode.setChunkFileMap(new ConcurrentHashMap<>());
        }

        List<Mono<Tuple2<String, ChunkFile>>> chunkResList = new LinkedList<>();
        for (String chunkKey : new LinkedList<>(updateChunk.keySet())) {
            List<ChunkFile.UpdateChunkOpt> list = updateChunk.remove(chunkKey);
            needDelete.computeIfAbsent(chunkKey, k -> new LinkedList<>());
            Mono<ChunkFile> chunkRes = updateChunk(inode, list, chunkKey, null, needDelete);
            chunkResList.add(Mono.just(chunkKey).zipWith(chunkRes, Tuple2::new));
        }

        Mono<Tuple2<String, ChunkFile>>[] merge = chunkResList.toArray(new Mono[0]);

        return Flux.merge(Flux.fromArray(merge), 1, 1)
                .collectList()
                .flatMap(chunkList -> {
                    Map<String, ChunkFile> chunkFileMap = inode.getChunkFileMap();
                    for (Tuple2<String, ChunkFile> tuple2 : chunkList) {
                        if (tuple2.var2.size < 0) {
                            if (CHECK_FAIL_CHUNK.size == tuple2.var2.size) {
                                continue;
                            }
                            return Mono.just(ERROR_INODE);
                        }

                        tuple2.var2.setVersionNum(inode.getVersionNum());
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

                    if (inode.getUpdateChunk() == null || inode.getUpdateChunk().isEmpty()) {
                        if (!splited) {
                            return checkSplit(inode, needDelete);
                        }

                        inode.setUpdateChunk(null);
                        return Mono.just(inode);
                    } else {
                        return updateChunk(inode, needDelete);
                    }
                });
    }

    private void markCheckFail(Inode inode, InodeData update) {
        String fileName = update.fileName;
        if (inode.getUpdateInodeDataStatus() == null) {
            inode.setUpdateInodeDataStatus(new ConcurrentHashMap<>());
        }
        inode.getUpdateInodeDataStatus().putIfAbsent(fileName, false);
    }

    private void markCheckSuccess(Inode inode, InodeData update) {
        String fileName = update.fileName;
        if (inode.getUpdateInodeDataStatus() == null) {
            inode.setUpdateInodeDataStatus(new ConcurrentHashMap<>());
        }
        inode.getUpdateInodeDataStatus().compute(fileName, (k, v) -> true);
    }
}
