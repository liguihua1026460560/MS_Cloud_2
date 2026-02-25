package com.macrosan.message.jsonmsg;

import com.dslplatform.json.JsonAttribute;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import io.vertx.core.json.JsonObject;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;

import static com.macrosan.constants.SysConstants.ROCKS_ES_KEY;
import static com.macrosan.ec.Utils.ZERO_STR;

@Data
@Accessors(chain = true)
public class EsMeta {

    @JsonAttribute
    public String bucketName;
    @JsonAttribute
    public String userId;
    @JsonAttribute
    public String objName;
    @JsonAttribute
    public String versionId;
    @JsonAttribute
    public String objSize;
    @JsonAttribute
    public String sysMetaData;
    @JsonAttribute
    public String userMetaData;
    @JsonAttribute
    public String stamp;
    @JsonAttribute
    public String versionNum;
    @JsonAttribute
    public boolean toDelete;
    @JsonAttribute
    public long inode;
    @JsonAttribute
    public boolean deleteSource = false;

    public EsMeta setVersionId(String versionId) {
        this.versionId = versionId == null ? "null" : versionId;
        return this;
    }

    public static final EsMeta ERROR_ES_META = new EsMeta().setStamp("error").setBucketName("").setObjName("").setVersionId("");
    public static final EsMeta NOT_FOUND_ES_META = new EsMeta().setStamp("not found").setBucketName("").setObjName("").setVersionId("");
    public static final String INODE_STAMP = "a";
    public static final String LINK_STAMP = "b";
    public static final String NORMAL_SUFFIX = "";
    public static final String CHECK_SUFFIX = "'";
    public static final String INODE_SUFFIX = "''";
    public static final String LINK_SUFFIX = "'''";
    public static final String ADD_LINK = "_1";
    public static final String DEL_LINK = "__1";
    public static final String SPLIT_FLAG = ":";

    public static Inode delInode(String bucket, String objName, String versionId, long nodeId) {
        return new Inode()
                .setBucket(bucket)
                .setObjName(objName)
                .setVersionId(versionId)
                .setLinkN(1)
                .setNodeId(nodeId);
    }

    public static Inode metaMapDelInode(MetaData metaData) {
        long stamp = Long.parseLong(metaData.getStamp());
        long mtime = stamp / 1000;
        long mtimensec = (stamp % 1000) * 1_000_000;
        return new Inode()
                .setObjName(metaData.getKey())
                .setBucket(metaData.getBucket())
                .setVersionId(metaData.getVersionId() == null ? "null" : metaData.getVersionId())
                .setNodeId(metaData.getInode())
                .setMtime(mtime)
                .setMtimensec(Math.toIntExact(mtimensec))
                .setVersionNum(metaData.getVersionNum());
    }

    public static EsMeta mapInodeMeta(Inode inode) {
        return new EsMeta()
                .setObjName(inode.getObjName())
                .setBucketName(inode.getBucket())
                .setVersionId(inode.getVersionId() == null ? "null" : inode.getVersionId())
                .setInode(inode.getNodeId())
                .setStamp(String.valueOf(System.currentTimeMillis()))
                .setVersionNum(inode.getVersionNum())
                .setUserId("0")
                .setUserMetaData("")
                .setSysMetaData("")
                .setObjSize("")
                .setToDelete(false);
    }

    public static EsMeta mapInodeMeta(EsMeta esMeta) {
        return new EsMeta()
                .setObjName(esMeta.getObjName())
                .setBucketName(esMeta.getBucketName())
                .setVersionId(esMeta.getVersionId() == null ? "null" : esMeta.getVersionId())
                .setInode(esMeta.getInode())
                .setStamp(esMeta.stamp)
                .setVersionNum(esMeta.getVersionNum().replace(ADD_LINK, ""))
                .setUserId("0")
                .setUserMetaData("")
                .setSysMetaData("")
                .setObjSize("0")
                .setToDelete(false);
    }

    public static EsMeta mapLinkMeta(EsMeta esMeta, String linkObjNames) {
        return new EsMeta()
                .setObjName(linkObjNames)
                .setBucketName(esMeta.getBucketName())
                .setVersionId(esMeta.getVersionId() == null ? "null" : esMeta.getVersionId())
                .setInode(esMeta.getInode())
                .setStamp(esMeta.stamp)
                .setVersionNum(esMeta.getVersionNum())
                .setUserId("0")
                .setUserMetaData("")
                .setSysMetaData("")
                .setObjSize("0")
                .setToDelete(false);
    }

    public static EsMeta mapEsMeta(EsMeta linkMeta, String objName) {
        return new EsMeta()
                .setObjName(objName)
                .setBucketName(linkMeta.getBucketName())
                .setVersionId(linkMeta.getVersionId() == null ? "null" : linkMeta.getVersionId())
                .setInode(linkMeta.getInode())
                .setStamp(linkMeta.stamp)
                .setVersionNum(linkMeta.getVersionNum())
                .setUserId("0")
                .setUserMetaData("")
                .setSysMetaData("")
                .setObjSize(linkMeta.objSize)
                .setToDelete(false);
    }

    public static boolean needPut(String key, EsMeta esMeta) {
        return esMeta.inode > 0 && !key.endsWith(INODE_SUFFIX) && !key.endsWith(LINK_SUFFIX);
    }

    public static EsMeta mapLinkMeta(Inode inode) {
        return new EsMeta().setObjName("")
                .setBucketName(inode.getBucket())
                .setVersionId(inode.getVersionId() == null ? "null" : inode.getVersionId())
                .setInode(inode.getNodeId())
                .setStamp(String.valueOf(System.currentTimeMillis()))
                .setVersionNum(inode.getVersionNum())
                .setUserId("0")
                .setUserMetaData("")
                .setSysMetaData("")
                .setObjSize(String.valueOf(inode.getSize()))
                .setToDelete(false);
    }

    public static EsMeta inodeMetaMapEsMeta(MetaData metaData, Inode inode) {
        JsonObject jsonObject = new JsonObject(metaData.sysMetaData);
        String owner = jsonObject.getString("owner");
        return new EsMeta()
                .setUserId(owner)
                .setSysMetaData(metaData.sysMetaData)
                .setUserMetaData(metaData.userMetaData)
                .setBucketName(metaData.getBucket())
                .setObjName(metaData.getKey())
                .setVersionId(metaData.getVersionId())
                .setStamp(String.valueOf(inode.getMtime() * 1000 + inode.getMtimensec() / 1_000_000))
                .setObjSize(String.valueOf(inode.getSize()))
                .setInode(inode.getNodeId())
                .setVersionNum(inode.getVersionNum())
                .setToDelete(false);
    }

    public static EsMeta inodeMapEsMeta(Inode inode) {
        return new EsMeta()
                .setObjName(inode.getObjName())
                .setBucketName(inode.getBucket())
                .setVersionId(inode.getVersionId() == null ? "null" : inode.getVersionId())
                .setInode(inode.getNodeId())
                .setObjSize(String.valueOf(inode.getSize()))
                .setVersionNum(inode.getVersionNum())
                .setStamp(String.valueOf(inode.getMtime() * 1000 + inode.getMtimensec() / 1_000_000));
    }

    public String oldRocksKey(boolean needCheck) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName);
        if (needCheck) {
            return ROCKS_ES_KEY + stamp + File.separator + bucketVnode + File.separator + bucketName + File.separator + objName + ZERO_STR + versionId + ROCKS_ES_KEY;
        }
        return ROCKS_ES_KEY + stamp + File.separator + bucketVnode + File.separator + bucketName + File.separator + objName + ZERO_STR + versionId;
    }

    public String rocksKey(boolean needCheck, boolean oldType, boolean inodeRecord, boolean linkRecord) {
        if (oldType) {
            return oldRocksKey(needCheck);
        }
        if (inodeRecord) {
            return getInodeKey(this);
        }
        if (linkRecord) {
            return getLinkKey(this);
        }
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName);
        //为了升级不出现decode问题不增加属性
        if (needCheck) {
            return ROCKS_ES_KEY + bucketVnode + File.separator + stamp + File.separator + bucketName + File.separator + objName + File.separator + versionNum + ZERO_STR + versionId + CHECK_SUFFIX;
        }
        return ROCKS_ES_KEY + bucketVnode + File.separator + stamp + File.separator + bucketName + File.separator + objName + File.separator + versionNum + ZERO_STR + versionId;
    }

    public static String getInodeKey(EsMeta esMeta) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(esMeta.bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(esMeta.bucketName);
        return ROCKS_ES_KEY + bucketVnode + File.separator + INODE_STAMP + File.separator + esMeta.bucketName + File.separator + esMeta.inode + ZERO_STR + esMeta.versionId + INODE_SUFFIX;
    }

    public static String getLinkKey(EsMeta esMeta) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(esMeta.bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(esMeta.bucketName);
        return ROCKS_ES_KEY + bucketVnode + File.separator + INODE_STAMP + File.separator + esMeta.bucketName + File.separator + esMeta.inode + ZERO_STR + esMeta.versionId + LINK_SUFFIX;
    }

    public static String getKey(EsMeta esMeta) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(esMeta.bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(esMeta.bucketName);
        if (esMeta.inode > 0) {
            return ROCKS_ES_KEY + bucketVnode + File.separator + esMeta.bucketName + File.separator + esMeta.inode;
        } else {
            return ROCKS_ES_KEY + bucketVnode + File.separator + esMeta.bucketName + File.separator + esMeta.objName + ZERO_STR + esMeta.versionId;
        }
    }

}
