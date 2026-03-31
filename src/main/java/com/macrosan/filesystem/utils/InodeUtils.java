package com.macrosan.filesystem.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.reply.smb2.CreateReply;
import com.macrosan.filesystem.cifs.types.smb2.CompoundRequest;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.redlock.LockType;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.nfs.call.MkNodCall;
import com.macrosan.filesystem.nfs.reply.EntryOutReply;
import com.macrosan.filesystem.nfs.types.FAttr3;
import com.macrosan.filesystem.nfs.types.FH2;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.message.jsonmsg.EsMeta;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.quota.QuotaRecorder;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.GET_INODE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.PUT_INODE;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NFSACLType.*;
import static com.macrosan.filesystem.FsConstants.NTStatus.*;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_DQUOT;
import static com.macrosan.filesystem.cache.Node.INODE_TIME_MAP;
import static com.macrosan.filesystem.nfs.NFS.IDLE_TIMEOUT;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.filesystem.utils.FSQuotaUtils.*;
import static com.macrosan.message.jsonmsg.EsMeta.inodeMapEsMeta;
import static com.macrosan.message.jsonmsg.Inode.*;

@Log4j2
public class InodeUtils {
    private static final Map<String, Inode> roots = new ConcurrentHashMap<>();

    public static MetaData newFsMetaMeta(String bucket, Map<String, String> bucketInfo, Inode inode, long stamp, String versionNum, String s3Id, String s3Name) {
        String lastModify = MsDateUtils.stampToGMT(stamp);

        Map<String, String> sysMetaMap = new HashMap<>();
        sysMetaMap.put("owner", s3Id);
        sysMetaMap.put(LAST_MODIFY, lastModify);
        sysMetaMap.put(ETAG, "00000000000000000000000000000000");
        sysMetaMap.put("displayName", s3Name);
        sysMetaMap.put(CONTENT_TYPE, "application/octet-stream");
        return new MetaData()
                .setStorage(inode.getStorage())
                .setFileName("")
                .setBucket(bucket)
//                .setObjectAcl("{\"acl\":\"256\",\"owner\":\"" + userId + "\"}")
                .setKey(inode.getObjName())
                .setDeleteMark(false)
                .setStartIndex(0)
                .setEndIndex(-1)
                .setDeleteMarker(false)
                .setReferencedBucket(bucket)
                .setReferencedKey(inode.getObjName())
                .setStamp(stamp + "")
                .setSysMetaData(Json.encode(sysMetaMap))
                .setVersionId(inode.getVersionId())
                .setReferencedVersionId(inode.getVersionId())
                .setVersionNum(versionNum)
                .setSyncStamp(versionNum)
                .setInode(inode.getNodeId())
                .setTmpInodeStr(Json.encode(inode))
                .setCookie(inode.getCookie() > 0 ? inode.getCookie() : VersionUtil.newInode());  // 用于readDirPlu
    }

    public static Mono<Inode> getInode(String bucket, long nodeId, boolean isNotRepair) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();

        String key = Inode.getKey(bucketVnode, bucket, nodeId);

        return BucketSyncSwitchCache.isSyncSwitchOffMono(bucket)
                .map(isSyncSwitchOff -> NOT_FOUND_INODE.clone()
                        .setVersionNum(VersionUtil.getLastVersionNum("", isSyncSwitchOff)))
                .flatMap(deleteMark -> ECUtils.getRocksKey(pool, key, Inode.class, GET_INODE, NOT_FOUND_INODE, ERROR_INODE, deleteMark,
                        Inode::getVersionNum, Comparator.comparing(a -> a.getVersionNum()), isNotRepair ? (a, b, c, d) -> Mono.just(1) : FsUtils::repairInode, nodeList, null))
                .map(t -> t.var1);
    }

    public static Mono<Boolean> updateInode(String bucket, Inode inode, boolean isRecover, boolean repairCookieAndInode) {
        if (inode.getNodeId() == 1) {
            updateRootInodeCache(bucket, inode);
            return Mono.just(true);
        }
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();
        CifsUtils.setDefaultCifsMode(inode);
        String key = Inode.getKey(bucketVnode, inode.getBucket(), inode.getNodeId());
        Map<String, String> bucketInfo = NFSBucketInfo.getBucketInfo(inode.getBucket());
        EsMeta esMeta = null;
        String mda = bucketInfo.get(ES_SWITCH);
        if (ES_ON.equals(mda)) {
            esMeta = inodeMapEsMeta(inode);
        }
        EsMeta finalEsMeta = esMeta;
        if (isRecover) {
            return ErasureClient.getObjectMetaVersionUnlimitedNotRecover(inode.getBucket(), inode.getObjName(), inode.getVersionId(), nodeList, null, null, null)
                    .flatMap(meta -> {
                        //如果meta已经不存在则不再修复inode
                        if (meta != null && meta.equals(MetaData.NOT_FOUND_META)) {
                            return Mono.just(true);
                        }

                        return FsUtils.putRocksKey(pool, key, Json.encode(inode), PUT_INODE, isRecover, repairCookieAndInode, nodeList, mda, finalEsMeta);
                    });
        }

        return FsUtils.putRocksKey(pool, key, Json.encode(inode), PUT_INODE, isRecover, repairCookieAndInode, nodeList, mda, finalEsMeta);
    }

    public static Inode updateRootInodeCache(String bucket, Inode rootInode) {
        return roots.computeIfPresent(bucket, (k, v) -> {
            if (rootInode.getCifsMode() == 0) {
                rootInode.setCifsMode(FILE_ATTRIBUTE_DIRECTORY);
            }
            return rootInode;
        });
    }

    public static void updateRootInodeCache(String bucket, long stamp, int stampNano, boolean isUpdAtime, boolean isUpdMtime, boolean isUpdCtime) {
        roots.computeIfPresent(bucket, (k, v) -> {
            if (isUpdAtime) {
                v.setAtime(stamp);
                v.setAtimensec(stampNano);
            }

            if (isUpdMtime) {
                v.setMtime(stamp);
                v.setMtimensec(stampNano);
            }

            if (isUpdCtime) {
                v.setCtime(stamp);
                v.setCtimensec(stampNano);
            }
            log.debug("bucket:{};root stamp:{}", bucket, stamp);
            return v;
        });
    }

    /**
     * 删除时更新Inode中的linkN；更新的结果放置于tuple2中，
     * 若 linkN>1，则更新 linkN，仅重置元数据、不删除数据块；<true, inode>
     * 若 linkN=1，则添加删除标记并删除数据块；<false, inode>
     * 如果查询中出错，则不删除数据块、不添加删除标记
     *
     * @param bucket     文件所处的桶
     * @param meta       getObjectMetaVersion 得到的 metaData
     * @param reqDelName 请求删除的文件名
     * @param tuple2     放置更新结果的元组 <Boolean, Inode>
     * @return 原样返回 T object
     **/
    public static <T> Mono<T> updateLinkN(T object, String bucket, MetaData meta, String reqDelName, Tuple2<Boolean, Inode>[] tuple2) {
        try {
            if (meta.inode > 0) {
                return Node.getInstance()
                        .getInodeAndUpdateCache(meta.bucket, meta.inode)
                        .flatMap(inode -> {
                            if (isError(inode)) {
                                tuple2[0] = new Tuple2<>(false, inode);
                                return Mono.just(object);
                            }
                            int linkN = inode.getLinkN();
                            if (linkN > 1) {
                                tuple2[0] = new Tuple2<>(true, inode);
                                return Mono.just(object);
                            }
                            tuple2[0] = new Tuple2<>(false, inode);
                            return Mono.just(object);
                        })
                        .doOnError(e -> {
                            log.error("updateLink error: metaData: {}, reqName: {}. {}", meta, reqDelName, e);
                            tuple2[0] = new Tuple2<>(false, ERROR_INODE);
                        })
                        .onErrorReturn(object);
            }
            tuple2[0] = new Tuple2<>(false, new Inode());
            return Mono.just(object);
        } catch (Exception e) {
            log.error("updateLink error: metaData: {}, reqName: {}. {}", meta, reqDelName, e);
            // 如果查询时出错，则不删除数据块、不更新元数据
            tuple2[0] = new Tuple2<>(false, ERROR_INODE);
            return Mono.just(object);
        }
    }


    /**
     * 新建 Inode
     * 参考 com.macrosan.filesystem.utils.InodeUtils.smbCreate方法实现
     *
     * @param reqHeader
     * @param dirInode
     * @param mode
     * @param cifsMode
     * @param obj
     * @return
     */
    public static Mono<Inode> ftpCreate(ReqInfo reqHeader, Inode dirInode, int mode, int cifsMode, String obj) {
        if (InodeUtils.isError(dirInode)) {
            log.info("get inode fail.bucket:{}, nodeId:{}", reqHeader.bucket, dirInode.getNodeId());
            return Mono.just(dirInode);
        }
        String tmpName = obj;
        if ((mode & S_IFMT) == S_IFDIR) {
            obj += '/';
        }

        if (obj.getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH || CheckUtils.isFileNameTooLong(tmpName)) {
            return Mono.just(ERROR_INODE);
        }

        Map<String, String> parameter = new HashMap<>();
        if (null != dirInode.getACEs() && !dirInode.getACEs().isEmpty()) {
            parameter.put(NFS_ACE, Json.encode(dirInode.getACEs()));
        }

        String finalObj = obj;
        return create(reqHeader, mode, cifsMode, finalObj, finalObj, -1, "", null, parameter)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        return Mono.just(ERROR_INODE);
                    } else {
                        Node.getInstance().updateInodeTime(dirInode.getNodeId(), reqHeader.bucket, inode.getMtime(), inode.getMtimensec(), false, true, true)
                                .map(i -> {
                                    if (isError(i)) {
                                        log.error("update dir: {} inode fail.....", dirInode.getObjName());
                                    }
                                    return i;
                                }).subscribe();
                    }
                    return Mono.just(inode);
                });
    }

    public static Mono<Inode> smbCreate(ReqInfo reqHeader, Inode dirInode, int mode, int cifsMode, String obj, CreateReply body, SMB2.SMB2Reply reply, CompoundRequest compoundRequest, int createOptions, String reqS3Id) {
        long id0 = SMB2FileId.getId();
        return smbCreate(reqHeader, dirInode, mode, cifsMode, obj, body, reply, compoundRequest, createOptions, id0, reqS3Id);
    }

    public static Mono<Inode> smbCreate(ReqInfo reqHeader, Inode dirInode, int mode, int cifsMode, String obj, CreateReply body, SMB2.SMB2Reply reply, CompoundRequest compoundRequest, int createOptions, long id0, String reqS3Id) {
        String tmpName = new String(obj);
        String name = obj.substring(obj.lastIndexOf("/") + 1);
        if ((mode & S_IFMT) == S_IFDIR) {
            obj += '/';
        }

        if (obj.getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH || CheckUtils.isFileNameTooLong(tmpName)) {
            reply.getHeader().setStatus(STATUS_NAME_TOO_LONG);
            return Mono.just(ERROR_INODE);
        }
        if (CheckUtils.isInValidSMB2FileName(name)) {
            reply.getHeader().setStatus(STATUS_OBJECT_NAME_INVALID);
            //参照samba，冒号特殊处理
            if (obj.indexOf(':') > 0) {
                reply.getHeader().setStatus(STATUS_OBJECT_NAME_NOT_FOUND);
            }
            return Mono.just(ERROR_INODE);
        }

        Map<String, String> parameter = new HashMap<>();
        if (null != dirInode.getACEs() && !dirInode.getACEs().isEmpty()) {
            parameter.put(NFS_ACE, Json.encode(dirInode.getACEs()));
        }
        //cifs端创建的文件，如果没有继承权限，那么需要记录对应的ugo权限、以及对应的uid和gid
        parameter.put(CIFS_CREATE, reqS3Id);

        String finalObj = obj;
        return create(reqHeader, mode, cifsMode, finalObj, finalObj, -1, "", null, parameter)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        reply.getHeader().setStatus(STATUS_INVALID_PARAMETER);
                        return Mono.just(ERROR_INODE);
                    } else {
                        body.setMode(CifsUtils.changeToHiddenCifsMode(inode.getObjName(), inode.getCifsMode(), true));
                        body.setFileId(SMB2FileId.randomFileId(compoundRequest, reqHeader.bucket, finalObj, inode.getNodeId(), dirInode.getNodeId(), createOptions, inode, dirInode.getACEs(), id0));
                        body.setCreateTime(CifsUtils.nttime(inode.getCreateTime() * 1000));
                        body.setAccessTime(CifsUtils.nttime(inode.getAtime() * 1000) + inode.getAtimensec() / 100);
                        body.setWriteTime(CifsUtils.nttime(inode.getMtime() * 1000) + inode.getMtimensec() / 100);
                        body.setChangTime(CifsUtils.nttime(inode.getCtime() * 1000) + inode.getCtimensec() / 100);
                        body.setFileSize(inode.getSize());
                        body.setAllocSize(CifsUtils.getAllocationSize(inode.getMode(), inode.getSize()));
                        reply.setBody(body);
                        reply.getHeader().setStatus(STATUS_SUCCESS);
                        Node.getInstance().updateInodeTime(dirInode.getNodeId(), reqHeader.bucket, inode.getMtime(), inode.getMtimensec(), false, true, true)
                                .map(i -> {
                                    if (isError(i)) {
                                        log.error("update dir: {} inode fail.....", dirInode.getObjName());
                                    }
                                    return i;
                                }).subscribe();
                    }
                    return Mono.just(inode);
                });
    }

    public static Mono<Inode> nfsCreate(ReqInfo reqHeader, Inode dirInode, int mode, byte[] buf, EntryOutReply entryOutReply, int fsid, RpcCallHeader callHeader, MkNodCall mkNodCall) {
        if (InodeUtils.isError(dirInode)) {
            entryOutReply.status = NfsErrorNo.NFS3ERR_I0;
            entryOutReply.attrFollow = 0;
            entryOutReply.objFhFollows = 0;
            log.info("get inode fail.bucket:{}, nodeId:{}", reqHeader.bucket, dirInode.getNodeId());
            return Mono.just(dirInode);
        }
        String name = dirInode.getObjName() + new String(buf, 0, buf.length);
        int cifsMode = FILE_ATTRIBUTE_ARCHIVE;
        if ((mode & S_IFMT) == S_IFDIR) {
            name += '/';
            cifsMode = FILE_ATTRIBUTE_DIRECTORY;
        }
        if (name.getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH) {
            entryOutReply.status = NfsErrorNo.NFS3ERR_NAMETOOLONG;
            return Mono.just(ERROR_INODE);
        }

        Map<String, String> parameter = new HashMap<>();
        if (null != dirInode.getACEs() && !dirInode.getACEs().isEmpty()) {
            parameter.put(NFS_ACE, Json.encode(dirInode.getACEs()));
        }

        return create(reqHeader, mode, cifsMode, name, name, -1, "", mkNodCall, parameter, callHeader)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        entryOutReply.status = EIO;
                        entryOutReply.attrFollow = 0;
                        entryOutReply.objFhFollows = 0;
                    } else {
                        entryOutReply.fh = FH2.mapToFH2(inode, fsid);
                        entryOutReply.attr = FAttr3.mapToAttr(inode, fsid);
                        dirInode.setAtime(inode.getAtime());
                        dirInode.setAtimensec(inode.getAtimensec());

                        dirInode.setMtime(inode.getMtime());
                        dirInode.setMtimensec(inode.getMtimensec());

                        dirInode.setCtime(inode.getCtime());
                        dirInode.setCtimensec(inode.getCtimensec());
                        Node.getInstance().updateInodeTime(dirInode.getNodeId(), reqHeader.bucket, inode.getMtime(), inode.getMtimensec(), false, true, true)
                                .map(i -> {
                                    if (isError(i)) {
                                        log.error("update dir: {} inode fail.....", dirInode.getObjName());
                                    }
                                    return i;
                                }).subscribe();
                    }
                    return Mono.just(inode);
                });
    }

    public static Mono<Inode> create(ReqInfo reqHeader, int mode, int cifsMode, String name,
                                     String reference, long cookie, String storage, MkNodCall mkNodCall, Map<String, String> parameter, RpcCallHeader... callHeaders) {
        String bucket = reqHeader.bucket;
        long nodeId = VersionUtil.newInode();
        String versionStatus = reqHeader.bucketInfo.getOrDefault(BUCKET_VERSION_STATUS, "NULL");
        String versionId = "NULL".equals(versionStatus) || VERSION_SUSPENDED.equals(versionStatus) ?
                "null" : RandomStringUtils.randomAlphanumeric(32);

        StorageOperate operate = new StorageOperate(StorageOperate.PoolType.DATA, "", Long.MAX_VALUE);
        if (StringUtils.isBlank(storage)) {
            storage = StoragePoolFactory.getStoragePool(operate, reqHeader.bucket).getVnodePrefix();
        }

        long stamp = System.currentTimeMillis();
        int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);

        Inode inode = new Inode()
                .setMode(mode)
                .setCifsMode(cifsMode)
                .setObjName(name)
                .setReference(reference)
                .setBucket(bucket)
                .setVersionId(versionId)
                .setNodeId(nodeId)
                .setLinkN(1)
                .setSize(0)
                .setAtime(stamp / 1000)
                .setMtime(stamp / 1000)
                .setCtime(stamp / 1000)
                .setCreateTime(stamp / 1000)
                .setAtimensec(stampNano)
                .setMtimensec(stampNano)
                .setCtimensec(stampNano)
                .setStorage(storage);
        inode.setCifsMode(CifsUtils.changeToHiddenCifsMode(name, inode.getCifsMode(), false));

        //软连接inode.size为reference的长度
        if ((inode.getMode() & S_IFMT) == S_IFLNK) {
            inode.setSize(inode.getReference().getBytes(StandardCharsets.UTF_8).length);
        }

        // 仅在s3上传dir无metaData时createS3Inode传入cookie
        if (cookie != -1) {
            inode.setCookie(cookie);
        } else {
            inode.setCookie(VersionUtil.newInode());
        }

        //设置拥有者文件uid以及s3Id
        String s3Account = ACLUtils.setInodeIdAndS3Id(inode, callHeaders, parameter, reqHeader);

        //设置ACL继承属性
        ACLUtils.updateDefACL(inode, parameter);

        // 文件端创建文件，默认对象ACL是公共读写，由文件端控制权限
        JsonObject aclJson = new JsonObject();
        aclJson.put("acl", String.valueOf(OBJECT_PERMISSION_SHARE_READ_WRITE_NUM));
        aclJson.put("owner", s3Account);
        inode.setObjAcl(aclJson.encode());

        if (mkNodCall != null) {
            inode.setMajorDev(mkNodCall.specData1);
            inode.setMinorDev(mkNodCall.specData2);
        }

        String[] displayName = {null};
        return FSQuotaUtils.addQuotaDirInfo(inode, stamp, false)
                .flatMap(res -> {
                    if (res.getLinkN() == FILES_QUOTA_EXCCED_INODE.getLinkN()) {
                        throw new NFSException(NFS3ERR_DQUOT, "can not create ,because of exceed quota.bucket:" + bucket + ",objName:" + inode.getObjName() + ",nodeId:" + inode.getNodeId());
                    }
                    return pool.getReactive(REDIS_USERINFO_INDEX).hget(s3Account, USER_DATABASE_ID_NAME)
                            .flatMap(s3Name -> {
                                displayName[0] = s3Name;
                                return Node.getInstance().getVersion(nodeId, bucket, true, "");
                            })
                            .flatMap(versionInode -> {
                                if (InodeUtils.isError(versionInode)) {
                                    log.info("get inode version {} fail", Json.encode(inode));
                                    return Mono.just(versionInode);
                                } else {
                                    return RedLockClient.lock(reqHeader, inode.getObjName(), LockType.WRITE, true, NFSV3.NFS_RED_LOCK)
                                            .flatMap(l -> Node.getInstance().createInode(bucket, inode, nodeId, stamp, versionInode.getVersionNum(), s3Account, displayName[0]))
                                            .doOnNext(i -> releaseLock(reqHeader));
                                }
                            });
                });
    }


    public static Mono<Inode> create0(String bucket, Inode inode, long nodeId, long stamp, String version, String s3Account, String displayName) {
        Map<String, String> bucketInfo = NFSBucketInfo.getBucketInfo(bucket);
        inode.setVersionNum(version);
        MetaData metaData = InodeUtils.newFsMetaMeta(bucket, bucketInfo, inode, stamp, version, s3Account, displayName);

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();

        String key = Utils.getMetaDataKey(bucketVnode, bucket, inode.getObjName(), metaData.versionId, metaData.stamp);

        return ErasureClient.getObjectMetaVersionUnlimitedNotRecover(inode.getBucket(), inode.getObjName(), inode.getVersionId(), nodeList, null, null, null)
                .flatMap(oldMeta -> {
                    boolean isDeleteMark = false;
                    if (oldMeta.isDeleteMark() || oldMeta.isDeleteMarker()) {
                        isDeleteMark = true;
                        if (oldMeta.getVersionNum().compareTo(metaData.versionNum) >= 0) {
                            metaData.versionNum = oldMeta.versionNum + "0";
                        }
                    }

                    if (oldMeta.inode > 0 && !isDeleteMark && StringUtils.isNotBlank(oldMeta.tmpInodeStr)) {
                        Inode resInode = Json.decodeValue(oldMeta.tmpInodeStr, Inode.class);
                        return Mono.just(resInode);
                    }
                    EsMeta esMeta = null;
                    String mda = bucketInfo.get(ES_SWITCH);
                    if (ES_ON.equals(mda)) {
                        esMeta = EsMeta.inodeMetaMapEsMeta(metaData, inode);
                    }
                    return ErasureClient.putMetaData(key, metaData, nodeList, esMeta, mda)
                            .doOnNext(b -> {
                                if (!b) {
                                    log.info("put meta {} fail", metaData);
                                }
                                QuotaRecorder.addCheckBucket(bucket);
                            })
                            .map(b -> b ? inode : ERROR_INODE);
                }).flatMap(i -> {
                    if (ES_ON.equals(bucketInfo.get(ES_SWITCH)) && !isError(i)) {
                        return EsMetaTask.putEsMeta(i).map(b0 -> i);
                    }
                    return Mono.just(i);
                });
    }

    public static void releaseLock(ReqInfo reqHeader) {
        try {
            if (NFSV3.NFS_RED_LOCK && !reqHeader.lock.isEmpty()) {
                for (String key : reqHeader.lock.keySet()) {
                    String value = reqHeader.lock.get(key);
                    RedLockClient.unlock(reqHeader.bucket, key, value, true).subscribe();
                }
            }
        } catch (Exception e) {
            log.error("release create lock error", e);
        }
    }

    public static Mono<Inode> createS3Inode(String bucket, String obj, String versionId, String metaHash, String dirDefACEs) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();

        String key = Utils.getVersionMetaDataKey(bucketVnode, bucket, obj, versionId);
        return ErasureClient.getObjectMetaVersionResUnlimited(bucket, obj, versionId, nodeList, null, null, null)
                .flatMap(tuple2 -> {
                    MetaData metaData = tuple2.var1;
                    if (!metaData.isAvailable()) {
                        if (Utils.DEFAULT_META_HASH.equals(metaHash)) {
                            return createDirMetaData(obj, bucket, dirDefACEs, metaData);
                        }
                        return Mono.just(ERROR_INODE);
                    }

                    if (!metaHash.equals(Utils.metaHash(metaData))) {
                        //当前元数据被覆盖
                        log.error("create s3 inode fail meta changed. old hash {}, cur meta {}", metaHash, metaData);
                        return Mono.just(ERROR_INODE);
                    }

                    //已经创建过inode 不需要重复创建
                    if (metaData.inode > 0) {
                        if (StringUtils.isNotEmpty(metaData.tmpInodeStr)) {
                            return Mono.just(Json.decodeValue(metaData.tmpInodeStr, Inode.class));
                        } else {
                            //未预期的错误
                            log.error("unkown error meta {}", metaData);
                            return Mono.just(ERROR_INODE);
                        }
                    }

                    Inode inode = Inode.defaultInode(metaData);
                    inode.setCifsMode(CifsUtils.changeToHiddenCifsMode(obj, inode.getCifsMode(), false));
                    inode.setNodeId(VersionUtil.newInode());
                    inode.setCookie(VersionUtil.newInode());
                    ACLUtils.setIdWhenCreateS3Inode(inode, metaData, dirDefACEs);

                    if (inode.getUid() > 0) {
                        addQuotaInfoToS3Inode(inode);
                    }
                    metaData.setTmpInodeStr(Json.encode(inode))
                            .setInode(inode.getNodeId())
                            .setCookie(inode.getCookie());

                    return ErasureClient.updateMetaDataAcl(key, metaData, nodeList, null, tuple2.var2, inode)
                            .map(r -> {
                                if (r.var1 == 1) {
                                    return inode;
                                } else if (r.var1 == 2) {
                                    return r.var2 == null ? inode : r.var2;
                                } else {
                                    return ERROR_INODE;
                                }
                            });

                });
    }

    public static Boolean isError(Inode inode) {
        return inode.equals(ERROR_INODE) || inode.equals(NOT_FOUND_INODE);
    }

    public static Inode getAndPutRootInode(String bucket) {
        return roots.compute(bucket, (k, v) -> {
            if (v == null) {
                v = new Inode()
                        .setBucket(bucket)
                        .setSize(0)
                        .setNodeId(1)
                        .setMode(S_IFDIR | 511)
                        .setCifsMode(FILE_ATTRIBUTE_DIRECTORY)
                        .setObjName("")
                        .setReference("")
                        .setLinkN(1)
                        .setVersionNum("")
                        .setRootTime(System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO));
            }
            return v;
        });
    }

    public static Inode getAndPutRootInode(String bucket, Map<String, String> bucketInfo) {
        return roots.compute(bucket, (k, v) -> {
            if (v == null) {
                v = new Inode()
                        .setBucket(bucket)
                        .setSize(0)
                        .setNodeId(1)
                        .setMode(S_IFDIR | 511)
                        .setCifsMode(FILE_ATTRIBUTE_DIRECTORY)
                        .setObjName("")
                        .setReference("")
                        .setLinkN(1)
                        .setVersionNum("")
                        .setRootTime(System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO));
            }

            if (bucketInfo != null) {
                if (bucketInfo.get("mode") != null) {
                    v.setMode(Integer.parseInt(bucketInfo.get("mode")));
                }

                if (bucketInfo.get("ACE") != null) {
                    List<Inode.ACE> curAcl = Json.decodeValue(bucketInfo.get("ACE"), new TypeReference<List<ACE>>() {
                    });
                    v.setACEs(curAcl);
                }
            }

            return v;
        });
    }

    public static Mono<Inode> getAndPutRootInodeReactive(String bucket, boolean reload) {
        if (!reload) {
            AtomicBoolean isExist = new AtomicBoolean(true);
            Inode root = roots.compute(bucket, (k, v) -> {
                if (v == null) {
                    isExist.set(false);
                }
                return v;
            });

            if (isExist.get()) {
                return Mono.just(root);
            }
        }

        return NFSBucketInfo.getBucketInfoReactive(bucket)
                .flatMap(info -> {
                    Inode inode = new Inode()
                            .setBucket(bucket)
                            .setSize(0)
                            .setNodeId(1)
                            .setMode(S_IFDIR | 511)
                            .setCifsMode(FILE_ATTRIBUTE_DIRECTORY)
                            .setObjName("")
                            .setReference("")
                            .setLinkN(1)
                            .setVersionNum("")
                            .setRootTime(System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO));

                    if (info != null) {
                        if (info.get("mode") != null) {
                            inode.setMode(Integer.parseInt(info.get("mode")));
                        }

                        if (info.get("ACE") != null) {
                            List<Inode.ACE> curAcl = Json.decodeValue(info.get("ACE"), new TypeReference<List<ACE>>() {
                            });
                            inode.setACEs(curAcl);
                        }
                    }

                    return Mono.just(roots.compute(bucket, (k, v) -> {
                        if (v == null) {
                            v = inode;
                        }
                        return v;
                    }));
                });
    }

    public static Inode changeMode(Inode inode, int mode) {
        if (inode.getNodeId() == 1) {
            return inode.setMode(mode | S_IFDIR);
        }

        switch (inode.getMode() & FsConstants.S_IFMT) {
            case FsConstants.S_IFDIR:
                mode |= S_IFDIR;
                break;
            case FsConstants.S_IFLNK:
                mode |= S_IFLNK;
                break;
            case FsConstants.S_IFREG:
                mode |= S_IFREG;
                break;
            case FsConstants.S_IFBLK:
                mode |= S_IFBLK;
                break;
            case FsConstants.S_IFFIFO:
                mode |= S_IFFIFO;
                break;
            case FsConstants.S_IFCHR:
                mode |= S_IFCHR;
                break;
            default:
                if (inode.getNodeId() == 1
                        || (StringUtils.isNotEmpty(inode.getObjName()) && inode.getObjName().endsWith("/"))) {
                    mode |= S_IFDIR;
                } else {
                    mode |= S_IFREG;
                }
                break;
        }

        return inode.setMode(mode);
    }

    public static void changeCifsMode(Inode inode, int cifsMode) {
        if ((inode.getCifsMode() & FILE_ATTRIBUTE_DIRECTORY) != 0) {
            inode.setCifsMode(cifsMode | FILE_ATTRIBUTE_DIRECTORY);
        } else {
            inode.setCifsMode(cifsMode);
        }
    }

    public static Mono<Boolean> checkOverWrite(MetaData metaData, boolean isMigrate, boolean[] isOverWrite, List<String> updateQuotaDir) {
        return FsUtils.lookup(metaData.bucket, metaData.key, null, false, -1, null)
                .flatMap(inode -> {
                    if (inode.getNodeId() > 0 && inode.getLinkN() > 1) {
                        ObjAttr objAttr = new ObjAttr();
                        objAttr.hasSize = 1;
                        objAttr.size = 0;
                        return Node.getInstance()
                                .setAttr(inode.getNodeId(), inode.getBucket(), objAttr, null)
                                .flatMap(inode1 -> {
                                    Inode metaInode = Inode.defaultInode(metaData);
                                    metaInode.setNodeId(inode.getNodeId());
                                    metaInode.setLinkN(inode.getLinkN());
                                    metaInode.setMode(inode.getMode());
                                    metaInode.setCifsMode(inode.getCifsMode());
                                    CifsUtils.setDefaultCifsMode(metaInode);
                                    metaInode.setCifsMode(CifsUtils.changeToHiddenCifsMode(metaInode.getObjName(), metaInode.getCifsMode(), false));
                                    metaInode.setGid(inode.getGid());
                                    metaInode.setUid(inode.getUid());
                                    metaInode.setMajorDev(inode.getMajorDev());
                                    metaInode.setMinorDev(inode.getMinorDev());
                                    PartInfo[] partInfos = metaData.partInfos;
                                    //针对自动分段对象的硬链接覆盖时使用reference存更新的etag
                                    if ((inode.getMode() & S_IFMT) != S_IFLNK && partInfos != null && partInfos.length > 0
                                            && partInfos[partInfos.length - 1].fileName.contains("partNum")) {
                                        Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                                        });
                                        if (sysMetaMap.get(ETAG) != null) {
                                            metaInode.setReference(sysMetaMap.get(ETAG));
                                        }
                                    }
                                    metaData.inode = inode.getNodeId();
                                    metaData.cookie = inode.getCookie();
                                    if (updateQuotaDir != null && !updateQuotaDir.isEmpty()) {
                                        metaInode.getXAttrMap().put(QUOTA_KEY, Json.encode(updateQuotaDir));
                                    }
                                    if (inode1.getUid() > 0) {
                                        addQuotaInfoToInode(metaInode, metaData.key);
                                    }
                                    metaData.tmpInodeStr = Json.encode(metaInode);
                                    isOverWrite[0] = true;
                                    return Mono.just(isMigrate);
                                });

                    }
                    //同名覆盖目录的情况异步删除原目录文件配额信息。
                    if (inode.getNodeId() > 0 && !inode.isDeleteMark() && (inode.getMode() & S_IFMT) == S_IFDIR) {
                        FSQuotaRealService.FS_QUOTA_EXECUTOR.schedule(() -> {
                            FSQuotaUtils.delQuotas(inode.getBucket(), metaData.key);
                        });
                    }
                    return Mono.just(isMigrate);
                });
    }

    public static Mono<Inode> updateSpeciDirTime(Inode dirInode, String objName, String bucketName) {
        if (dirInode != null && InodeUtils.isError(dirInode)) {
            return Node.getInstance().updateInodeTime(dirInode.getNodeId(), bucketName, System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), false, true, true)
                    .doOnError(e -> log.error("" + e));
        }

        return updateParentDirTime(objName, bucketName);
    }

    /**
     * 递归更新s3上传对象的父目录的元数据与时间
     * 如/a/b/c/obj格式的对象，父目录上传时无元数据，则更新最靠近obj对象的目录时间
     * 若父目录元数据都不存在，则更新根目录时间，文件端list会创建对应目录的元数据
     *
     * @param objName    当前对象名
     * @param bucketName 桶名
     **/
    public static Mono<Inode> updateParentDirTime(String objName, String bucketName) {
        String[] dirName = new String[]{""};
        String[] dirPrefix = objName.split("/");
        if (dirPrefix.length > 1) {
            if (objName.endsWith("/")) {
                dirName[0] = objName.substring(0, objName.length() - dirPrefix[dirPrefix.length - 1].length() - 2);
            } else {
                dirName[0] = objName.substring(0, objName.length() - dirPrefix[dirPrefix.length - 1].length() - 1);
            }
        }

        if (StringUtils.isEmpty(dirName[0])) {
            // 更新根目录时间
            return Node.getInstance().updateInodeTime(1, bucketName, System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), false, true, true)
                    .doOnError(e -> log.error("" + e));
        } else {
            // 更新父目录时间，先查是否存在父目录inode，若不存在则无需更新
            return FsUtils.lookup(bucketName, dirName[0], null, false, -1, null)
                    .flatMap(inode -> {
                        // 父目录存在inode，更新父目录的时间
                        if (!InodeUtils.isError(inode) && inode.getNodeId() != 0) {
                            return Node.getInstance().updateInodeTime(inode.getNodeId(), bucketName, System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), false, true, true);
                        }

                        // 父目录不存在inode，而且不存在meta，继续递归往上查找；/a/b/c -> /a/b -> /a，只需更新已存在的最靠近上传对象的目录时间即可
                        if (Utils.DEFAULT_META_HASH.equals(inode.getReference())) {
                            return updateParentDirTime(inode.getObjName(), bucketName);
                        }
                        return Mono.just(inode);
                    })
                    .doOnError(e -> log.error("" + e));
        }
    }

    public static Mono<Inode> findDirInode(String objName, String bucket) {
        String dirName = CifsUtils.getParentDirName(objName);

        if (StringUtils.isEmpty(dirName)) {
            return Node.getInstance().getInode(bucket, 1);
        } else {
            return FsUtils.lookup(bucket, dirName, null, false, -1, null);
        }
    }

    public static void updateInodeAtime(Inode inode) {
        if (isRelaUpdate(inode)) {
            inode.setAtime(System.currentTimeMillis() / 1000);
            inode.setAtimensec((int) (System.nanoTime() % ONE_SECOND_NANO));
            Node.getInstance().updateInodeTime(inode.getNodeId(), inode.getBucket(), inode.getAtime(), inode.getAtimensec(), true, false, false).subscribe();
        }
    }

    /***
     * 判断再getMeta时，是否需要更新getMeta结果
     * 如果获取到的第一份数据是文件的metadata，且get时inode还未恢复，则需将返回结果更改为其他节点获取到的metadata进行返回
     * @param oldMetaData 返回的第一份metadata
     * @param newMetaData 再次返回的metadata
     * @return 是否需要更改
     */
    public static boolean needChangeGetRes(MetaData oldMetaData, MetaData newMetaData) {
        if (oldMetaData.inode > 0
                && oldMetaData.inode == newMetaData.inode
                && oldMetaData.getVersionNum().equals(newMetaData.getVersionNum())
                && (StringUtils.isBlank(oldMetaData.tmpInodeStr) && StringUtils.isNotBlank(newMetaData.tmpInodeStr))
        ) {
            return true;
        }
        return false;
    }

    public static boolean isRequestRepeat(String objName, long optTime) {

        boolean[] needReturnSuccess = new boolean[1];
        INODE_TIME_MAP.compute(objName, (k, v) -> {
            if (v == null) {
                return optTime;
            }
            if (NFS.nfsDebug) {
                log.info("obj:{},optTime:{},v:{},delay:{}", objName, optTime, v, (optTime - v));
            }
            if (optTime - v >= IDLE_TIMEOUT * 1000L) {
                needReturnSuccess[0] = true;
            }
            return v;
        });
        return needReturnSuccess[0];
    }

    public static void deleteRequestInodeTimeMap(String objName, long optTime) {
        if (StringUtils.isNotBlank(objName)) {
            INODE_TIME_MAP.compute(objName, (k, v) -> {
                if (v == null) {
                    return null;
                }
                if (NFS.nfsDebug) {
                    log.info("repeat:obj:{},opt {},v:{},equal:{}", objName, optTime, v, optTime == v);
                }
                if (optTime != v) {
                    return v;
                }
                return null;
            });
        }
    }

    public static Mono<Inode> createDirMetaData(String objName, String bucketName, String dirACEs, MetaData metaData) {
        ReqInfo reqHeader = new ReqInfo();
        reqHeader.bucket = bucketName;
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(bucketName);
        int mode;
        int cifsMode;
        if (objName.endsWith("/")) {
            // s3创建的目录没有元数据，无法得知该目录属于哪个账户，因此转化为文件后，默认属于桶拥有者所有，且ugo权限默认777，不妨碍对象端dir/obj形式继续访问该目录
            if (StringUtils.isBlank(metaData.objectAcl)) {
                mode = OBJECT_TRANS_FILE_MODE | S_IFDIR;
            } else {
                mode = DEFAULT_MODE | S_IFDIR;
            }

            cifsMode = FILE_ATTRIBUTE_DIRECTORY;
        } else {
            mode = DEFAULT_MODE | S_IFREG;
            cifsMode = FILE_ATTRIBUTE_ARCHIVE;
        }

        Map<String, String> parameter = new HashMap<>();
        if (StringUtils.isNotBlank(dirACEs)) {
            parameter.put(NFS_ACE, dirACEs);
            //仅s3目录转换时设置
            parameter.put(TRANSFER_S3DIR, "1");
        }

        RpcCallHeader callHeader = new RpcCallHeader(null);

        if (StringUtils.isBlank(metaData.objectAcl)) {
            try {
                AuthUnix auth = new AuthUnix();
                auth.flavor = 1;
                String s3Account = reqHeader.bucketInfo.get(BUCKET_USER_ID);

                int[] uidAndGid = ACLUtils.getUidAndGid(s3Account);
                auth.setUid(uidAndGid[0]);
                auth.setGid(uidAndGid[1]);
                Set<Integer> gids = ACLUtils.s3IDToGids.get(s3Account);
                if (gids == null) {
                    auth.setGidN(1);
                    auth.setGids(new int[]{uidAndGid[1]});
                } else {
                    auth.setGidN(gids.size());
                    auth.setGids(gids.stream().mapToInt(Integer::intValue).toArray());
                }
                callHeader.auth = auth;
            } catch (Exception mapAuthErr) {
                log.error("map auth error ", mapAuthErr);
            }
        }

        return create(reqHeader, mode, cifsMode, objName, objName, -1, "", null, parameter, callHeader);
    }

    public static byte[] getModeStr(int mode) {
        byte[] rw = new byte[]{'-', '-', '-', '-', '-', '-', '-', '-', '-', '-'};
        if ((mode & S_IFDIR) != 0) {
            rw[0] = 'd';
        }

        if ((mode & 256) != 0) {
            rw[1] = 'r';
        }

        if ((mode & 128) != 0) {
            rw[2] = 'w';
        }

        if ((mode & 64) != 0) {
            rw[3] = 'x';
        }

        if ((mode & 32) != 0) {
            rw[4] = 'r';
        }

        if ((mode & 16) != 0) {
            rw[5] = 'w';
        }

        if ((mode & 8) != 0) {
            rw[6] = 'x';
        }

        if ((mode & 4) != 0) {
            rw[7] = 'r';
        }

        if ((mode & 3) != 0) {
            rw[8] = 'w';
        }

        if ((mode & 2) != 0) {
            rw[9] = 'x';
        }

        if ((mode & 512) != 0) {
            rw[9] = 't';
        }

        if ((mode & 1024) != 0) {
            rw[6] = 's';
        }

        if ((mode & 2048) != 0) {
            rw[3] = 's';
        }

        return rw;
    }

    public static String getVnodeFromObjectName(String object, StoragePool bucketPool, String bucket) {
        if (object.startsWith(ROCKS_INODE_PREFIX)) {
            return Utils.getVnode(object);
        } else {
            return bucketPool.getBucketVnodeId(bucket, object);
        }
    }

    public static String getVersionMetaKey(String object, String bucketVnode, String bucket, String versionId, String snapshotMark) {
        if (object.startsWith(ROCKS_INODE_PREFIX)) {
            return object;
        } else {
            return Utils.getVersionMetaDataKey(bucketVnode, bucket, object, versionId, snapshotMark);
        }
    }

    /**
     * 一天内保证能更新一次
     *
     * @param atime a
     * @return res
     */
    public static boolean needUpdateAtime(long atime) {
        if (atime <= 0) {
            return false;
        }
        long now = System.currentTimeMillis() / 1000;

        return now - atime > 24 * 60 * 60;
    }
}
