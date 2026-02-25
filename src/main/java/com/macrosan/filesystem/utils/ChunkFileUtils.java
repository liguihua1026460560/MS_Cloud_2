package com.macrosan.filesystem.utils;

import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.GetMetaResEnum;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.ChunkFile;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.Inode.InodeData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import reactor.core.publisher.Mono;

import java.util.*;

import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_CHUNK;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.jsonmsg.ChunkFile.ERROR_CHUNK;
import static com.macrosan.message.jsonmsg.ChunkFile.NOT_FOUND_CHUNK;

public class ChunkFileUtils {
    public static InodeData newChunk(String fileName, InodeData singleFile, Inode inode) {
        return new InodeData()
                .setSize(singleFile.size)
                .setOffset(0)
                .setChunkNum(1)
                .setStorage(inode.getStorage())
                .setEtag(singleFile.etag)
                .setFileName(ChunkFile.getChunkFileName(inode.getNodeId(), fileName));
    }

    private static Mono<Integer> repairChunk(String key, ChunkFile chunkFile, List<Tuple3<String, String, String>> nodeList, Tuple2<ErasureServer.PayloadMetaType, ChunkFile>[] res) {
        //ERROR or NOT FOUND
        if (chunkFile.size < 0) {
            return Mono.just(1);
        }

        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<ErasureServer.PayloadMetaType, ChunkFile> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.getVersionNum());
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(chunkFile.getBucket());
//        chunkFile.setVersionNum(VersionUtil.getLastVersionNum(chunkFile.versionNum));

        return ECUtils.updateRocksKey(pool, oldVersionNum, key, Json.encode(chunkFile), PUT_CHUNK, ERROR_PUT_CHUNK, nodeList, null);
    }

    public static Mono<ChunkFile> getChunk(String bucket, String chunkKey) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();

        return BucketSyncSwitchCache.isSyncSwitchOffMono(bucket)
                .map(isSyncSwitchOff -> new ChunkFile().setSize(NOT_FOUND_CHUNK.size)
                        .setVersionNum(VersionUtil.getLastVersionNum("", isSyncSwitchOff)))
                .flatMap(deleteMark -> ECUtils.getRocksKey(pool, chunkKey, ChunkFile.class, GET_CHUNK, NOT_FOUND_CHUNK, ERROR_CHUNK, deleteMark,
                        ChunkFile::getVersionNum, Comparator.comparing(ChunkFile::getVersionNum), ChunkFileUtils::repairChunk, nodeList, null))
                .map(t -> t.var1);
    }

    public static PartInfo[] mapToPartInfo(ChunkFile chunkFile) {
        PartInfo[] partList = new PartInfo[chunkFile.getChunkList().size()];
        int i = 0;
        for (InodeData inodeData : chunkFile.getChunkList()) {
            partList[i] = new PartInfo()
                    .setUploadId("inode")
                    .setBucket(chunkFile.getBucket())
                    .setObject(chunkFile.getObjName())
                    .setPartNum(String.valueOf(i + 1))
                    .setFileName(inodeData.fileName)
                    .setPartSize(inodeData.size)
                    .setDelete(false)
                    .setEtag(inodeData.etag)
                    .setStorage(inodeData.storage)
                    .setVersionId(chunkFile.getVersionId())
                    .setOffset(inodeData.offset);
            i++;
        }

        return partList;
    }

    public static final long MAX_CHUNK_NUM = 1024;

    public static final long MAX_CHUNK_SIZE = 5L << 30;


    public static long updateChunkData(long reqOffset, InodeData data, List<InodeData> list, long size, boolean[] hasCovered) {
        long reqEnd = reqOffset + data.size;
        if (reqEnd <= 0) {
            //加到头
            list.listIterator(0).add(data);
            return size + data.size;
        } else if (reqOffset >= size) {
            //加到尾
            list.add(data);
            return size + data.size;
        } else {
            boolean addData = false;

            ListIterator<InodeData> listIterator = list.listIterator();
            long curOffset = 0L;

            while (listIterator.hasNext()) {
                InodeData inodeData = listIterator.next();
                long curEnd = inodeData.size + curOffset;
                boolean needDelete = true;
                if (curEnd < reqOffset) {
                    curOffset = curEnd;
                } else if (reqEnd < curOffset) {
                    break;
                } else {
                    //有交集 处理覆盖
                    listIterator.remove();
                    if (reqOffset > curOffset) {
                        //小于data的部分 把size改小
                        InodeData pre = new InodeData()
                                .setFileName(inodeData.fileName)
                                .setStorage(inodeData.storage)
                                .setOffset(inodeData.offset)
                                .setEtag(inodeData.etag)
                                .setSize(reqOffset - curOffset);

                        listIterator.add(pre);
                        needDelete = false;
                    }

                    if (!addData) {
                        listIterator.add(data);
                        addData = true;
                    }

                    if (reqEnd < curEnd) {
                        //大于data的部分 把size改小 并修改新的offset
                        InodeData next = new InodeData()
                                .setFileName(inodeData.fileName)
                                .setStorage(inodeData.storage)
                                .setEtag(inodeData.etag)
                                .setSize(curEnd - reqEnd);
                        next.setOffset(inodeData.offset + inodeData.size - next.size);
                        listIterator.add(next);
                        needDelete = false;
                    }
                    if (needDelete) {
                        hasCovered[0] = true;
                    }
                    curOffset = curEnd;
                }
            }


            if (reqEnd > size) {
                size += reqEnd - size;
            }

            if (reqOffset < 0) {
                size -= reqOffset;
            }
            return size;
        }
    }

    /**
     * last是文件数据块，拼接 data
     **/
    public static void lastFileAppendData(InodeData last, InodeData data, Inode inode, long reqOffset) {
        String chunkKey = ChunkFile.getChunkKeyFromChunkFileName(inode.getBucket(), last.fileName);
        long chunkOffset = reqOffset;
        for (InodeData inodeData : inode.getInodeData()) {
            if (null != last.fileName
                    && last.fileName.equals(inodeData.fileName)
                    && last.size == inodeData.size) {
                break;
            }
            chunkOffset -= inodeData.size;
        }
        Inode.updateChunk(inode, chunkKey, new ChunkFile.AppendChunkOpt(data, chunkOffset));
        last.size += data.size;
        last.chunkNum++;
    }

    /**
     * last是文件数据块，拼接 holeFile
     **/
    public static void lastFileAppendHole(InodeData last, long holeSize, Inode inode, long reqOffset) {
        if (holeSize > 0) {
            InodeData appendHole = InodeData.newHoleFile(holeSize);
            lastFileAppendData(last, appendHole, inode, reqOffset);
        }
    }

    /**
     * 根据holeFile列表中的 inodeData 生成对应的 chunkFile
     **/
    public static void creatMultiHole(List<InodeData> list, List<InodeData> holeFileList, Inode inode) {
        for (InodeData holeFile : holeFileList) {
            String holeFileName = Utils.getHoleFileName(StoragePoolFactory.getMetaStoragePool(inode.getBucket()),
                    inode.getBucket(), getRequestId());
            InodeData holeChunk = ChunkFileUtils.newChunk(holeFileName, holeFile, inode);
            ChunkFile holeChunkFile = new ChunkFile(inode.getNodeId(), inode.getBucket(), inode.getObjName(),
                    inode.getVersionId(), VersionUtil.getVersionNum(), holeFile.size);
            holeChunkFile.getChunkList().add(holeFile);

            Inode.updateChunk(inode, ChunkFile.getChunkKey(holeFileName), new ChunkFile.CreateChunkOpt(holeChunkFile));
            list.add(holeChunk);
        }
    }

    /**
     * 生成对应 data 的 chunkFile
     **/
    public static void createData(List<InodeData> list, InodeData data, Inode inode) {
        String chunkKey = ChunkFile.getChunkKey(data.fileName);
        InodeData chunk = ChunkFileUtils.newChunk(data.fileName, data, inode);
        ChunkFile chunkFile = new ChunkFile(inode.getNodeId(), inode.getBucket(), inode.getObjName(),
                inode.getVersionId(), VersionUtil.getVersionNum(), data.size);
        chunkFile.getChunkList().add(data);
        Inode.updateChunk(inode, chunkKey, new ChunkFile.CreateChunkOpt(chunkFile));
        list.add(chunk);
    }

    /**
     * last数据块为s3数据块，将last数据块，holeFile拼接生成一个chunkFile
     **/
    public static void lastS3PartCreateData(List<InodeData> list, InodeData last, long holeSize, Inode inode) {
        if (holeSize > 0) {
            InodeData holeFile = InodeData.newHoleFile(holeSize);
            String chunkKey = ChunkFile.getChunkKey(last.fileName);
            InodeData chunk = ChunkFileUtils.newChunk(last.fileName, last, inode)
                    .setChunkNum(2)
                    .setSize(last.size + holeFile.size);
            ChunkFile chunkFile = new ChunkFile(inode.getNodeId(), inode.getBucket(), inode.getObjName(),
                    inode.getVersionId(), VersionUtil.getVersionNum(), last.size + holeFile.size);
            List<InodeData> chunkList = chunkFile.getChunkList();
            chunkList.add(last);
            chunkList.add(holeFile);
            Inode.updateChunk(inode, chunkKey, new ChunkFile.CreateChunkOpt(chunkFile));
            list.add(chunk);
        } else {
            createData(list, last, inode);
        }
    }

    /**
     * 将last数据块、hole和 data拼接生成一个 chunkFile
     **/
    public static void createLastHoleData(List<InodeData> list, InodeData last, long holeSize, InodeData data, Inode inode) {
        InodeData holeFile = InodeData.newHoleFile(holeSize);
        String chunkKey = ChunkFile.getChunkKey(last.fileName);
        InodeData chunk = ChunkFileUtils.newChunk(last.fileName, last, inode)
                .setChunkNum(3)
                .setSize(last.size + holeFile.size + data.size);
        ChunkFile chunkFile = new ChunkFile(inode.getNodeId(), inode.getBucket(), inode.getObjName(),
                inode.getVersionId(), VersionUtil.getVersionNum(), last.size + holeFile.size + data.size);
        List<InodeData> chunkList = chunkFile.getChunkList();
        chunkList.add(last);
        chunkList.add(holeFile);
        chunkList.add(data);
        Inode.updateChunk(inode, chunkKey, new ChunkFile.CreateChunkOpt(chunkFile));
        list.add(chunk);
    }
}
