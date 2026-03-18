package com.macrosan.message.jsonmsg;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.BaseEncoding;
import com.macrosan.constants.SysConstants;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.cifs.types.smb2.QueryDirInfo;
import com.macrosan.filesystem.cifs.types.smb2.SID;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.filesystem.utils.acl.NFSACL;
import com.macrosan.message.jsonmsg.ChunkFile.UpdateChunkOpt;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.cache.Md5DigestPool;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsDateUtils;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NFSACLType.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.CONTAINER_INHERIT_ACE;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.OBJECT_INHERIT_ACE;
import static com.macrosan.filesystem.FsConstants.SMB2ACEType.ACCESS_ALLOWED_ACE_TYPE;
import static com.macrosan.filesystem.FsConstants.SMB2ACEType.ACCESS_DENIED_ACE_TYPE;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_TYPE;
import static com.macrosan.filesystem.utils.ChunkFileUtils.MAX_CHUNK_NUM;
import static com.macrosan.filesystem.utils.ChunkFileUtils.MAX_CHUNK_SIZE;

@Data
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Log4j2
public class Inode {
    int mode;
    int cifsMode;
    long size;
    long nodeId;
    long atime;
    long mtime;
    long ctime;
    long createTime;
    int atimensec;
    int mtimensec;
    int ctimensec;

    int linkN;
    int uid;
    int gid;
    int majorDev;
    int minorDev;

    String bucket;
    String objName;
    String versionId = "null";

    public String getVersionId() {
        return versionId == null ? "null" : versionId;
    }

    //软连接
    String reference;
    String versionNum;
    // 在create时记录存储池，确保多存储池上传情况中所有数据块在同一存储池中
    String storage;

    /**
     * key:quotaTypeKey
     * value:stamp-size
     */

    Map<String, String> quotaStartStampAndSize = null;

    boolean deleteMark = false;

    //存放NFS ACL的权限
    List<Inode.ACE> ACEs = null;

    //存放对象ACL的权限
    String objAcl;

    Map<String, Boolean> updateInodeDataStatus = null; // 记录file的更新状态，true 更新成功，false 更新失败

    long cookie;

    // 统计升级前后当前inode含有的新版本createTime属性元数据的数量
    @JsonIgnore
    int counter;

    //附加属性
    Map<String, String> xAttrMap = new HashMap<>();

    List<InodeData> inodeData = new LinkedList<>();
    //InodeOperator的updateInode时临时使用, 一般情况下都是null
    Map<String, List<UpdateChunkOpt>> updateChunk = null;
    //putInode时同时写入ChunkFile时写入
    Map<String, ChunkFile> chunkFileMap = null;

    //创建inode的moss账户若配置了uid；或者uid账户配置了moss账户，则该aclTag置为1；否则为0，用于标识未完成配置的旧文件，使旧文件仍旧可读
    int aclTag;

    public static void updateChunk(Inode inode, String chunkKey, UpdateChunkOpt opt) {
        if (inode.updateChunk == null) {
            inode.updateChunk = new ConcurrentHashMap<>();
        }

        inode.updateChunk.computeIfAbsent(chunkKey, k -> new LinkedList<>())
                .add(opt);
    }

    public static String getKey(String vnode, String bucket, long nodeId) {
        return ROCKS_INODE_PREFIX + vnode + File.separator + bucket + File.separator
                + nodeId;
    }

    public static String getCookieKey(String vnode, String bucket, long cookie) {
        return ROCKS_COOKIE_KEY + vnode + File.separator + bucket + File.separator + cookie;
    }

    public static Inode ERROR_INODE = new Inode().setLinkN(-1);
    public static Inode NOT_FOUND_INODE = new Inode().setLinkN(-2);
    public static Inode RETRY_INODE = new Inode().setLinkN(-3);
    public static Inode NO_PERMISSION_INODE = new Inode().setLinkN(-4);
    public static Inode STALE_HANDLE_INODE = new Inode().setLinkN(-5);
    public static Inode DELETEMARK_INODE = new Inode().setLinkN(-6);
    public static Inode INODE_DELETE_MARK = new Inode().setDeleteMark(true);
    public static Inode DEL_SUCCESS_INODE = new Inode().setLinkN(0);
    public static Inode HEART_DOWN_INODE = new Inode().setLinkN(-7);
    public static Inode NAME_TOO_LONG_INODE = new Inode().setLinkN(-8);
    public static Inode DEL_QUOTA_FREQUENCY_INODE = new Inode().setLinkN(-9);
    public static Inode CAP_QUOTA_EXCCED_INODE = new Inode().setLinkN(-10);
    public static Inode FILES_QUOTA_EXCCED_INODE = new Inode().setLinkN(-11);
    public static Inode BUSY_INODE = new Inode().setLinkN(-12);
    public static Inode UPDATE_PROCESS_INODE = new Inode().setLinkN(-13);
    public static Inode INVALID_ARGUMENT_INODE = new Inode().setLinkN(-14);

    public static Boolean NOATIME = false;
    public static Boolean NODIRATIME = false;
    public static Boolean RELATIME = false;

    public Inode clone() {
        return new Inode()
                .setMode(mode)
                .setSize(size)
                .setNodeId(nodeId)
                .setBucket(bucket)
                .setObjName(objName)
                .setVersionNum(versionNum)
                .setVersionId(versionId)
                .setLinkN(linkN)
                .setReference(reference)
                .setCookie(cookie)
                .setGid(gid)
                .setUid(uid)
                .setCifsMode(cifsMode)
                .setMajorDev(majorDev)
                .setMinorDev(minorDev)
                .setSelfTime(atime, mtime, ctime, atimensec, mtimensec, ctimensec, createTime)
                .setInodeData(cloneInodeData(inodeData))
                .setUpdateChunk(updateChunk == null ? null : cloneUpdateChunk(updateChunk))
                .setStorage(storage)
                .setDeleteMark(deleteMark)
                .setQuotaStartStampAndSize(quotaStartStampAndSize)
                .setXAttrMap(cloneXAttrMap(xAttrMap))
                .setUpdateInodeDataStatus(updateInodeDataStatus == null ? null : cloneUpdateInodeDataStatus(updateInodeDataStatus))
                .setACEs(cloneACE(ACEs))
                .setObjAcl(objAcl)
                .setAclTag(aclTag);
    }

    public Map<String, String> cloneXAttrMap(Map<String, String> oldMap) {
        Map<String, String> newMap = new HashMap<>();
        for (String key : oldMap.keySet()) {
            if (!QUOTA_TYPE.equals(key)) {
                String value = oldMap.get(key);
                newMap.put(key, value);
            }
        }
        return newMap;
    }

    /**
     * 深拷贝updateChunk集合，为集合及集合中的元素创建全新地址的副本，防止inodeOperator中vnode.exec()时返回转换JSON格式时因为
     * updateChunk被其它操作修改而出错
     **/
    public Map<String, List<UpdateChunkOpt>> cloneUpdateChunk(Map<String, List<UpdateChunkOpt>> updateChunk) {
        Map<String, List<UpdateChunkOpt>> newMap = new ConcurrentHashMap<>();
        Iterator<String> iterator = updateChunk.keySet().iterator();
        while (iterator.hasNext()) {
            String chunkKey = iterator.next();
            List<UpdateChunkOpt> list = new LinkedList<>(updateChunk.get(chunkKey));
            newMap.computeIfAbsent(chunkKey, k -> list);
        }
        return newMap;
    }

    /**
     * 深拷贝updateChunk集合，为集合及集合中的元素创建全新地址的副本，防止inodeOperator中vnode.exec()时返回转换JSON格式时因为
     * UpdateInodeDataStatus被其它操作修改而出错
     **/
    public Map<String, Boolean> cloneUpdateInodeDataStatus(Map<String, Boolean> updateInodeDataStatus) {
        Map<String, Boolean> newMap = new ConcurrentHashMap<>();
        Iterator<String> iterator = updateInodeDataStatus.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            newMap.computeIfAbsent(key, k -> updateInodeDataStatus.get(key));
        }
        return newMap;
    }

    public List<InodeData> cloneInodeData(List<InodeData> inodeData) {
        List<InodeData> newInodeData = new LinkedList<>();
        for (InodeData data : inodeData) {
            InodeData inodeData0 = new InodeData();
            inodeData0.setFileName(data.getFileName());
            inodeData0.setSize(data.getSize());
            inodeData0.setStorage(data.getStorage());
            inodeData0.setOffset(data.getOffset());
            inodeData0.setEtag(data.getEtag());
            inodeData0.setChunkNum(data.getChunkNum());
            inodeData0.setSize0(data.getSize0());
            newInodeData.add(inodeData0);
        }
        return newInodeData;
    }

    public List<Inode.ACE> cloneACE(List<Inode.ACE> aces) {
        if (null == aces) {
            return null;
        }

        List<Inode.ACE> list = new LinkedList<>();
        for (ACE ace : aces) {
            Inode.ACE newACE = new ACE();
            newACE.setNType(ace.getNType())
                    .setRight(ace.getRight())
                    .setId(ace.getId())
                    .setCType(ace.getCType())
                    .setFlag(ace.getFlag())
                    .setSid(ace.getSid())
                    .setMask(ace.getMask());
            list.add(newACE);
        }
        return list;
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    /**
     * 原始文件时fileName.
     * 实际读取时，从原始文件的offset处，读取size个字节
     */
    public static class InodeData {
        public long size;
        public String fileName;
        public String storage;
        public long offset;
        public String etag;
        public int chunkNum;
        //实际数据块的 fileSize = size + offset + size0
        // 减小size并且offset变时 通过修改size0，保持fileSize不变
        //chunk 不需要记录 size0
        public long size0;

        @JsonIgnore
        public boolean chunk() {
            return StringUtils.isNotBlank(fileName) && fileName.startsWith(ROCKS_CHUNK_FILE_KEY);
        }

        @JsonIgnore
        public boolean full(long inodeSize) {
            return chunkNum >= MAX_CHUNK_NUM || size >= MAX_CHUNK_SIZE;
        }

        public static InodeData newHoleFile(long size) {
            return new InodeData()
                    .setFileName("")
                    .setOffset(0L)
                    .setSize(size)
                    .setEtag("")
                    .setStorage("");
        }

        /**
         * 根据空洞的大小切分成多个 5G holeFile，从而避免后续生成过大的 holeChunkFile
         * 不足5G的部分根据标志 isAddLeft 判断是否加入列表
         *
         * @param holeSize  总空洞大小
         * @param isAddLeft 是否加上未整除的部分
         */
        public static List<InodeData> newHoleFileList(long holeSize, boolean isAddLeft) {
            List<InodeData> holeFileList = new LinkedList<>();
            int num = (int) (holeSize / MAX_CHUNK_SIZE);
            long leftHoleSize = holeSize - num * MAX_CHUNK_SIZE;
            for (int i = 0; i < num; i++) {
                holeFileList.add(newHoleFile(MAX_CHUNK_SIZE));
            }

            if (isAddLeft && leftHoleSize > 0) {
                holeFileList.add(newHoleFile(leftHoleSize));
            }
            return holeFileList;
        }

        public InodeData clone() {
            return new InodeData()
                    .setSize(size)
                    .setFileName(fileName)
                    .setStorage(storage)
                    .setOffset(offset)
                    .setEtag(etag)
                    .setChunkNum(chunkNum)
                    .setSize0(size0);
        }
    }

    public Inode setAtime(long atime) {
        this.atime = atime;
        return this;
    }

    public Inode setMtime(long mtime) {
        this.mtime = mtime;
        return this;
    }

    public Inode setCtime(long ctime) {
        this.ctime = ctime;
        return this;
    }

    public Inode setRootTime(long time, int timensec) {
        setAtime(time);
        setMtime(time);
        setCtime(time);
        setCreateTime(time);

        setAtimensec(timensec);
        setMtimensec(timensec);
        setCtimensec(timensec);

        return this;
    }

    public Inode setSelfTime(long atime, long mtime, long ctime, int atimensec, int mtimensec, int ctimensec, long createTime) {
        this.atime = atime;
        this.mtime = mtime;
        this.ctime = ctime;
        this.createTime = createTime;
        this.atimensec = atimensec;
        this.mtimensec = mtimensec;
        this.ctimensec = ctimensec;
        return this;
    }

    public static boolean isRelaUpdate(Inode inode) {
        if (inode.atime < inode.mtime || inode.atime < inode.ctime) {
            return true;
        }

        if (inode.atime == inode.mtime && inode.atime == inode.ctime) {
            if (inode.atimensec < inode.mtimensec || inode.atimensec < inode.ctimensec) {
                return true;
            } else {
                return false;
            }
        }

        return false;
    }

    public static void mergeMeta(MetaData metaData, Inode inode) {
        if (inode.deleteMark || metaData.deleteMark) {
            return;
        }
        metaData.setStartIndex(0L);
        metaData.setEndIndex(inode.size - 1);
        metaData.setPartUploadId("inode" + inode.getNodeId());
        metaData.setStorage(inode.getStorage());
        Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
        });
        int i = 0;
        MessageDigest digest = Md5DigestPool.acquire();
        //0kb文件/目录，对该文件临时生成一个inodeData
        if (inode.getInodeData().size() == 0) {
            StorageOperate operate = new StorageOperate(StorageOperate.PoolType.DATA, "", Long.MAX_VALUE);
            StoragePool dataPool = StoragePoolFactory.getStoragePool(operate, metaData.bucket);
            String fileName = Utils.getObjFileName(dataPool, metaData.bucket, inode.getObjName() + RandomStringUtils.randomAlphanumeric(4),
                    RandomStringUtils.randomAlphanumeric(32));
            Inode.InodeData inodeData = new Inode.InodeData()
                    .setSize(0)
                    .setStorage(dataPool.getVnodePrefix())
                    .setOffset(0L)
                    .setEtag(SysConstants.ZERO_ETAG)
                    .setFileName(fileName);
            inode.getInodeData().add(inodeData);
        }
        PartInfo[] partList = new PartInfo[inode.getInodeData().size()];
        String etag = "";
        boolean isChunk = false;
        if (partList.length != 1) {
            int realInodeDataLength = 0;
            for (InodeData inodeData : inode.getInodeData()) {
                if (inodeData.getChunkNum() == 0) {
                    realInodeDataLength += 1;
                } else {
                    isChunk = true;
                    realInodeDataLength += inodeData.getChunkNum();
                }
                partList[i] = new PartInfo()
                        .setUploadId("inode")
                        .setBucket(metaData.bucket)
                        .setObject(metaData.key)
                        .setPartNum(String.valueOf(i + 1))
                        .setFileName(inodeData.fileName)
                        .setPartSize(inodeData.size)
                        .setDelete(false)
                        .setEtag(inodeData.etag)
                        .setStorage(inodeData.storage)
                        .setVersionId(metaData.versionId)
                        .setOffset(inodeData.offset);
                i++;
                if (StringUtils.isNotEmpty(inodeData.getEtag())) {
                    digest.update(BaseEncoding.base16().decode(inodeData.getEtag().toUpperCase()));
                }
            }
            etag = inode.getInodeData().get(inode.getInodeData().size() - 1).getEtag() + "-" + realInodeDataLength;
            //处理自动分段的情况
            if (!isChunk && StringUtils.isNotEmpty(partList[0].fileName)
                    && sysMetaMap.containsKey(ETAG)
                    && !"00000000000000000000000000000000".equals(sysMetaMap.get(ETAG))) {
                etag = sysMetaMap.get(ETAG);
            }
            if (inode.getLinkN() > 1) {
                String lastFile = partList[partList.length - 1].fileName;
                if ((inode.getMode() & S_IFMT) != S_IFLNK &&
                        StringUtils.isNotEmpty(lastFile) && !lastFile.startsWith(ROCKS_CHUNK_FILE_KEY)
                        && lastFile.contains("partNum")) {
                    etag = inode.getReference();
                } else {
                    etag = Hex.encodeHexString(digest.digest()) + "-" + realInodeDataLength;
                }
            }
            digest.digest();
            Md5DigestPool.release(digest);
        } else {
            InodeData inodeData = inode.getInodeData().get(0);
            String inodeDataEtag = StringUtils.isBlank(inodeData.getFileName()) ? "00000000000000000000000000000000-1" : inodeData.getEtag();
            partList[i] = new PartInfo()
                    .setUploadId("inode")
                    .setBucket(metaData.bucket)
                    .setObject(metaData.key)
                    .setPartNum(String.valueOf(i + 1))
                    .setFileName(inodeData.fileName)
                    .setPartSize(inodeData.size)
                    .setDelete(false)
                    .setEtag(inodeDataEtag)
                    .setStorage(inodeData.storage)
                    .setVersionId(metaData.versionId)
                    .setOffset(inodeData.offset);
            if (inodeData.getFileName().startsWith(")")) {
                etag = inodeData.etag + "-" + (inodeData.getChunkNum() == 0 ? 1 : inodeData.getChunkNum());
            } else {
                etag = inodeDataEtag;
            }

        }

        //默认旧版本合权限功能前，对象的objACL为私有读写；暂时调整为公共读写
        if (StringUtils.isBlank(inode.getObjAcl())) {
            if (StringUtils.isNotBlank(metaData.getObjectAcl())) {
                //如果inode中objAcl为空，则应以metaData中的objAcl为准，即此时metaData中的objectAcl不动
                inode.setObjAcl(metaData.getObjectAcl());
            } else {
                //如果metaData中的objAcl也为空，再从metaData的sysMeta中获取owner
                JsonObject aclJson = new JsonObject();
                //正常情况下不会走到这里，所以设置为公共读写
                aclJson.put("acl", String.valueOf(OBJECT_PERMISSION_SHARE_READ_WRITE_NUM));
                String owner = sysMetaMap.containsKey("owner") ? sysMetaMap.get("owner") : null;
                if (null == owner) {
                    owner = NFSBucketInfo.isExistBucketOwner(metaData.bucket) ?
                            NFSBucketInfo.getBucketOwner(metaData.bucket) : DEFAULT_USER_ID;
                }

                aclJson.put("owner", owner);
                inode.setObjAcl(aclJson.encode());
            }
        } else {
            //若inode中的objAcl不为空，则以inode中的objAcl为准
            metaData.setObjectAcl(inode.getObjAcl());
        }

        sysMetaMap.put(ETAG, etag);
        sysMetaMap.put(LAST_MODIFY, MsDateUtils.stampToGMT(inode.getMtime() * 1000));
        sysMetaMap.put(CONTENT_LENGTH, inode.getSize() + "");
        metaData.setSysMetaData(Json.encode(sysMetaMap));
        metaData.setPartInfos(partList);
        inode.getInodeData().clear();
        metaData.setTmpInodeStr(Json.encode(inode));
    }

    public static void mergeObjAcl(Inode inode, MetaData metaData) {
        if (StringUtils.isBlank(inode.getObjAcl())) {
            //latestMeta中没有objectAcl字段，只能从sysMetaMap中获取owner
            if (StringUtils.isBlank(metaData.getObjectAcl())) {
                if (StringUtils.isNotBlank(metaData.sysMetaData)) {
                    Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                    });
                    JsonObject aclJson = new JsonObject();
                    //正常情况下应该能获取到，所以设置为私有读写
                    aclJson.put("acl", String.valueOf(OBJECT_PERMISSION_PRIVATE_NUM));
                    String owner = sysMetaMap.containsKey("owner") ? sysMetaMap.get("owner") : null;
                    if (null == owner) {
                        owner = NFSBucketInfo.isExistBucketOwner(metaData.bucket) ?
                                NFSBucketInfo.getBucketOwner(metaData.bucket) : DEFAULT_USER_ID;
                    }
                    aclJson.put("owner", owner);
                    inode.setObjAcl(aclJson.encode());
                }
            } else {
                inode.setObjAcl(metaData.getObjectAcl());
            }
        }
    }

    public static void reduceMeta(MetaData metaData) {
        metaData.setStartIndex(0L);
        metaData.setEndIndex(-1L);
        metaData.setPartUploadId(null);
        metaData.setPartInfos(null);
        metaData.setFileName(null);
    }

    public static Inode defaultCookieInode(Inode inode) {
        return new Inode()
                .setMode(inode.mode)
                .setCifsMode(inode.cifsMode)
                .setSize(inode.size)
                .setNodeId(inode.nodeId)
                .setBucket(inode.bucket)
                .setObjName(inode.objName)
                .setVersionNum(inode.versionNum)
                .setLinkN(inode.linkN)
                .setReference(inode.reference)
                .setSelfTime(inode.atime, inode.mtime, inode.ctime, inode.atimensec, inode.mtimensec, inode.ctimensec, inode.createTime)
                .setCookie(inode.cookie)
                .setStorage(inode.storage);
    }

    public static Inode defaultInode(MetaData metaData) {
        List<InodeData> data = new LinkedList<>();
        Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
        });
        if (StringUtils.isNotBlank(metaData.fileName) && !ECUtils.isAppendableObject(metaData)) {
            data.add(new InodeData()
                    .setSize(metaData.endIndex + 1)
                    .setStorage(metaData.storage)
                    .setFileName(metaData.fileName)
                    .setEtag(sysMetaMap.get(ETAG))
                    .setOffset(0L));
        } else if (metaData.getPartInfos() != null) {
            for (PartInfo partInfo : metaData.getPartInfos()) {
                data.add(new InodeData()
                        .setSize(partInfo.partSize)
                        .setStorage(partInfo.storage)
                        .setFileName(partInfo.fileName)
                        .setEtag(partInfo.etag)
                        .setOffset(0L));
            }
        }
        int[] uidAndGid = ACLUtils.getUidAndGid(sysMetaMap.getOrDefault("owner", "0"));
        return new Inode()
                .setRootTime(Long.parseLong(metaData.stamp) / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO))
                .setMode(metaData.getKey().endsWith("/") ? (S_IFDIR | DEFAULT_MODE) : (S_IFREG | DEFAULT_MODE))
                .setCifsMode(metaData.getKey().endsWith("/") ? FILE_ATTRIBUTE_DIRECTORY : FILE_ATTRIBUTE_ARCHIVE)
                .setSize(metaData.endIndex + 1)
                .setObjName(metaData.getKey())
                .setBucket(metaData.getBucket())
                .setReference(metaData.getKey())
                .setStorage(metaData.storage)
                .setLinkN(1)
                .setVersionNum(metaData.versionNum)
                .setNodeId(VersionUtil.newInode())
                .setInodeData(data)
                .setVersionId(metaData.versionId)
                .setUid(uidAndGid[0])
                .setGid(uidAndGid[1])
                .setObjAcl(metaData.getObjectAcl());
    }

    public static void addInodeData(MetaData metaData, Inode inode) {
        List<InodeData> data = new LinkedList<>();
        if (StringUtils.isNotBlank(metaData.fileName) && !ECUtils.isAppendableObject(metaData)) {
            Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
            });

            data.add(new InodeData()
                    .setSize(metaData.endIndex + 1)
                    .setStorage(metaData.storage)
                    .setFileName(metaData.fileName)
                    .setEtag(sysMetaMap.get(ETAG))
                    .setOffset(0L));
        } else {
            for (PartInfo partInfo : metaData.getPartInfos()) {
                data.add(new InodeData()
                        .setSize(partInfo.partSize)
                        .setStorage(partInfo.storage)
                        .setFileName(partInfo.fileName)
                        .setEtag(partInfo.etag)
                        .setOffset(0L));
            }
        }
        inode.setInodeData(data);
    }

    /**
     * metaData中存在inodeId但是，inode元数据还未写入时，
     * 根据metaData生成一个还未上传的Inode
     *
     * @param metaData 对象元数据
     * @return inode
     */
    public static Inode notPutInode(MetaData metaData) {
        Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
        });
        int[] uidAndGid = ACLUtils.getUidAndGid(sysMetaMap.getOrDefault("owner", "0"));
        return new Inode()
                .setRootTime(Long.parseLong(metaData.stamp) / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO))
                .setMode(metaData.getKey().endsWith("/") ? (S_IFDIR | DEFAULT_MODE) : (S_IFREG | DEFAULT_MODE))
                .setCifsMode(metaData.getKey().endsWith("/") ? FILE_ATTRIBUTE_DIRECTORY : FILE_ATTRIBUTE_ARCHIVE)
                .setSize(0)
                .setObjName(metaData.getKey())
                .setBucket(metaData.getBucket())
                .setReference(metaData.getKey())
                .setLinkN(1)
                .setVersionNum("")
                .setNodeId(metaData.getInode())
                .setUid(uidAndGid[0])
                .setGid(uidAndGid[1])
                .setObjAcl(metaData.getObjectAcl())
                .setStorage("");
    }

    @JsonIgnore
    int direntplusSize = 0;

    public int countDirentplusSize() {
        if (direntplusSize == 0) {
            byte[] name = objName.getBytes();
            int padding = ((8 - (name.length & 7)) & 7) + name.length + 152;
            direntplusSize = padding;
            return padding;
        } else {
            return direntplusSize;
        }
    }

    public int countQuerySize(int queryClass, String prefix) {
        //nfs
        if (queryClass == -1) {
            return countDirentplusSize();
        }
        //smb2
        else {
            return countQueryDirInfo((byte) (queryClass & 0xff), prefix);
        }
    }

    public int countQueryDirInfo(byte queryClass, String prefix) {
        return QueryDirInfo.size(objName.getBytes(StandardCharsets.UTF_16LE), prefix.getBytes(StandardCharsets.UTF_16LE), queryClass);
    }

    public String realFileName(int dirNameLen) {
        if (StringUtils.isNotBlank(objName)) {
            if (objName.equals(".") || objName.equals("..")) {
                return objName;
            }

            if (objName.endsWith("/")) {
                return objName.substring(dirNameLen, objName.length() - 1);
            } else {
                return objName.substring(dirNameLen);
            }
        }
        return "";
    }

    public static long getMinTime(Inode inode) {
        long[] timeArr = {inode.getAtime(), inode.getMtime(), inode.getCtime()};
        long min = 0L;
        for (int i = 0; i < timeArr.length; i++) {
            if (timeArr[i] > min) {
                min = timeArr[i];
            }
        }
        return min;
    }

    public static String getNodeIdFromInodeKey(String inodeKey) {
        if (StringUtils.isBlank(inodeKey)) {
            return "1";
        }
        return inodeKey.substring(inodeKey.lastIndexOf("/") + 1);
    }

    /**
     * 获取mask值，同时更新ACEs
     *
     * @param ACEs
     * @param groupMode
     * @param isSet     是否要更新ACEs
     * @return <mask, 更新后的ACEs>
     */
    public static Tuple2<Integer, List<ACE>> getAndSetNFSMask(List<Inode.ACE> ACEs, int groupMode, boolean isSet) {
        int mask = 0;
        List<Inode.ACE> curAcl = ACEs;
        ListIterator<Inode.ACE> listIterator = curAcl.listIterator();
        while (listIterator.hasNext()) {
            Inode.ACE ace = listIterator.next();
            int type = ace.getNType();
            if (NFSACL_CLASS == type) {
                mask = ace.getRight();
                if (isSet) {
                    if (groupMode != mask) {
                        listIterator.remove();
                        listIterator.add(new ACE(type, groupMode, ace.getId()));
                    }
                }
                break;
            }
        }

        Tuple2<Integer, List<Inode.ACE>> resTuple = new Tuple2<>(mask, curAcl);
        return resTuple;
    }

    public static void updateMask(Inode inode, int hasMode, int mode) {
        if (inode.getNodeId() > 1) {
            if (null != inode.getACEs() && !inode.getACEs().isEmpty() && hasMode != 0) {
                int groupMode = NFSACL.parseModeToInt(mode, NFSACL_GROUP_OBJ);
                Tuple2<Integer, List<Inode.ACE>> tuple2 = getAndSetNFSMask(inode.getACEs(), groupMode, true);
                if (groupMode != tuple2.var1) {
                    inode.setACEs(tuple2.var2);
                }
            }
        }
    }

    public static void updateACL(Inode inode, int uid, int gid) {
        if (null != inode && null != inode.getACEs() && !inode.getACEs().isEmpty()) {
            //仅存在 cifs ace时更改ace属主
            if (CIFSACL.isExistCifsACE(inode)) {
                //获取inode对应的属主和属组
                String inoOwnerSID = FSIdentity.getUserSIDByUid(inode.getUid());
                String inoGroupSID = FSIdentity.getGroupSIDByGid(inode.getGid());

                for (ACE ace : inode.getACEs()) {
                    byte cType = ace.getCType();
                    short inheritFlag = ace.getFlag();
                    String sid = ace.getSid();
                    switch (cType) {
                        case ACCESS_ALLOWED_ACE_TYPE:
                        case ACCESS_DENIED_ACE_TYPE:
                            if (inode.getObjName().endsWith("/") && (inheritFlag & OBJECT_INHERIT_ACE) != 0 || (inheritFlag & CONTAINER_INHERIT_ACE) != 0) {
                                break;
                            } else {
                                //不存在继承权限
                                if (inoOwnerSID.equals(sid) && uid >= 0) {
                                    ace.setSid(FSIdentity.getUserSIDByUid(uid));
                                    break;
                                }

                                if (inoGroupSID.equals(sid) && gid > 0) {
                                    ace.setSid(FSIdentity.getGroupSIDByGid(gid));
                                    break;
                                }
                            }
                            break;
                        default:
                    }
                }
            }
        }
    }

    public static void updateCifsUGOACL(Inode inode, int oldMode) {
        if (null != inode && null != inode.getACEs() && !inode.getACEs().isEmpty()) {
            //获取inode对应的属主和属组
            String inoOwnerSID = FSIdentity.getUserSIDByUid(inode.getUid());
            String inoGroupSID = FSIdentity.getGroupSIDByGid(inode.getGid());

            boolean[] checkMode = CIFSACL.checkModeUpdType(oldMode, inode.getMode());

            Iterator<Inode.ACE> iterator = inode.getACEs().listIterator();
            while (iterator.hasNext()) {
                Inode.ACE ace = iterator.next();
                byte cType = ace.getCType();
                short inheritFlag = ace.getFlag();
                String sid = ace.getSid();
                switch (cType) {
                    case ACCESS_ALLOWED_ACE_TYPE:
                        if (inode.getObjName().endsWith("/") && (inheritFlag & OBJECT_INHERIT_ACE) != 0 || (inheritFlag & CONTAINER_INHERIT_ACE) != 0) {
                            break;
                        } else {
                            //不存在继承权限
                            if (inoOwnerSID.equals(sid) && checkMode[0]) {
                                int nfsRight = NFSACL.parseModeToInt(inode.getMode(), NFSACL_USER_OBJ);
                                long access = CIFSACL.ugoToCIFSAccess(nfsRight);
                                ace.setMask(access);
                                break;
                            }

                            if (inoGroupSID.equals(sid) && checkMode[1]) {
                                int nfsRight = NFSACL.parseModeToInt(inode.getMode(), NFSACL_GROUP_OBJ);
                                long access = CIFSACL.ugoToCIFSAccess(nfsRight);
                                ace.setMask(access);
                                break;
                            }

                            if (SID.EVERYONE.getDisplayName().equals(sid) && checkMode[2]) {
                                int nfsRight = NFSACL.parseModeToInt(inode.getMode(), NFSACL_OTHER);
                                long access = CIFSACL.ugoToCIFSAccess(nfsRight);
                                ace.setMask(access);
                                break;
                            }
                        }
                        break;
                    case ACCESS_DENIED_ACE_TYPE:
                    default:
                }
            }
        }
    }

    @Data
    @Accessors(chain = true)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public static class ACE {
        //保存NFSACLType，cifs创建时，根据设置的是用户权限还是用户组权限对nType进行同步设置
        //即用户:NFSACL_USER，用户组：NFSACL_GROUP，其余nType cifs无需设置
        int nType;

        //保存nfs acl r、w、x权限
        int right;

        //保存cifs的ACLType
        byte cType;

        //保存cifs的权限继承范围 aceFlags
        short flag;

        //赋予的uid或gid
        int id;

        //cifs使用sid记录，用于区分group还是owner
        String sid;

        //保存cifs的特殊权限
        long mask;

        public ACE() {
        }

        public ACE(int nType, int right, int id) {
            this.nType = nType;
            this.right = right;
            this.id = id;
        }

        public ACE(byte cType, short flag, String sid, long mask) {
            this.cType = cType;
            this.flag = flag;
            this.sid = sid;
            this.mask = mask;
        }

        public ACE clone() {
            Inode.ACE newACE = new ACE();
            newACE.setNType(this.getNType())
                    .setRight(this.getRight())
                    .setId(this.getId())
                    .setCType(this.getCType())
                    .setFlag(this.getFlag())
                    .setSid(this.getSid())
                    .setMask(this.getMask());
            return newACE;
        }
    }
}
