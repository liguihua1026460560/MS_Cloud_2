package com.macrosan.message.jsonmsg;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.macrosan.message.jsonmsg.Inode.InodeData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple3;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.LinkedList;

import static com.macrosan.constants.SysConstants.ROCKS_CHUNK_FILE_KEY;

@Data
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ChunkFile {
    public long nodeId;
    public String bucket;
    public String objName;
    public String versionId;
    public String versionNum;
    public LinkedList<String> hasDeleteFiles = new LinkedList<>();
    public long size;

    LinkedList<InodeData> chunkList = new LinkedList<>();

    public ChunkFile() {
    }

    public ChunkFile(long nodeId, String bucket, String key, String versionId, String versionNum, long size) {
        this.nodeId = nodeId;
        this.bucket = bucket;
        this.objName = key;
        this.versionId = versionId;
        this.versionNum = versionNum;
        this.size = size;
    }

    public static Tuple3<Long, String, String> getChunkFromFileName(String chunkFileName) {
        String[] split = chunkFileName.substring(ROCKS_CHUNK_FILE_KEY.length()).split("_");
        long nodeId = Long.parseLong(split[0]);
        String fileName = chunkFileName.substring(split[0].length() + 1 + ROCKS_CHUNK_FILE_KEY.length());
        String bucket = fileName.split("_")[1];
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String v = pool.getBucketVnodeId(bucket);
        String chunkKey = getChunkKey(v, fileName);
        return new Tuple3<>(nodeId, bucket, chunkKey);
    }

    //Inode中记录的fileName
    public static String getChunkFileName(long nodeId, String fileName) {
        return ROCKS_CHUNK_FILE_KEY + nodeId + '_' + fileName;
    }

    //兼容一般的key格式，增加vnode 方便迁移
    public static String getChunkKey(String vnode, String fileName) {
        return ROCKS_CHUNK_FILE_KEY + vnode + '_' + fileName;
    }

    public static String getChunkKeyFromChunkFileName(String bucket, String chunkFileName) {
        String fileName = chunkFileName.substring(chunkFileName.split("_")[0].length() + 1);
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String v = pool.getBucketVnodeId(bucket);
        return getChunkKey(v, fileName);
    }

    public static String getChunkKey(String fileName) {
        String bucket = fileName.split("_")[1];
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String v = pool.getBucketVnodeId(bucket);
        return getChunkKey(v, fileName);
    }

    public static ChunkFile ERROR_CHUNK = new ChunkFile().setSize(-1);
    public static ChunkFile NOT_FOUND_CHUNK = new ChunkFile().setSize(-2);
    public static ChunkFile CHECK_FAIL_CHUNK = new ChunkFile().setSize(-3);

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "opt", visible = true)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = CreateChunkOpt.class, name = "0"),
            @JsonSubTypes.Type(value = AppendChunkOpt.class, name = "1"),
            @JsonSubTypes.Type(value = ModifyChunkOpt.class, name = "2"),
            @JsonSubTypes.Type(value = CheckAndModifyChunkOpt.class, name = "3"),
            @JsonSubTypes.Type(value = ErrorChunkOpt.class, name = "4")
    })
    @Data
    public abstract static class UpdateChunkOpt {
        int opt;
    }

    @NoArgsConstructor
    public static class CreateChunkOpt extends UpdateChunkOpt {
        public ChunkFile chunkFile;

        public CreateChunkOpt(ChunkFile chunkFile) {
            this.chunkFile = chunkFile;
            opt = 0;
        }
    }

    @NoArgsConstructor
    public static class AppendChunkOpt extends UpdateChunkOpt {
        public InodeData data;
        public long offset;

        public AppendChunkOpt(InodeData data, long offset) {
            this.data = data;
            this.offset = offset;
            opt = 1;
        }
    }

    @NoArgsConstructor
    public static class ModifyChunkOpt extends UpdateChunkOpt {
        public long reqOffset;
        public InodeData data;

        public ModifyChunkOpt(long reqOffset, InodeData data) {
            this.reqOffset = reqOffset;
            this.data = data;
            opt = 2;
        }
    }

    @NoArgsConstructor
    public static class CheckAndModifyChunkOpt extends UpdateChunkOpt {
        public String oldInodeData;
        public long reqOffset;
        public InodeData data;

        public CheckAndModifyChunkOpt(String oldInodeData, long reqOffset, InodeData data) {
            this.oldInodeData = oldInodeData;
            this.reqOffset = reqOffset;
            this.data = data;
            opt = 3;
        }

        public boolean check(String currentInodeData) {
            return this.oldInodeData.equals(currentInodeData);
        }
    }

    @NoArgsConstructor
    public static class ErrorChunkOpt extends UpdateChunkOpt {
        public String oldInodeData;

        public ErrorChunkOpt(String oldInodeData) {
            this.oldInodeData = oldInodeData;
            opt = 4;
        }
    }

    public static String printChunk(ChunkFile chunkFile) {
        return new ChunkFile(chunkFile.nodeId, chunkFile.bucket, chunkFile.objName, chunkFile.versionId, chunkFile.versionNum, chunkFile.size).toString();
    }
}
