package com.macrosan.filesystem.cifs.api.smb2;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.LookupCache;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cache.WriteCache;
import com.macrosan.filesystem.cache.WriteCacheClient;
import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.call.smb2.FileEndInfo;
import com.macrosan.filesystem.cifs.call.smb2.NegprotCall;
import com.macrosan.filesystem.cifs.call.smb2.SetInfoCall;
import com.macrosan.filesystem.cifs.reply.smb2.SetInfoReply;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.*;
import com.macrosan.filesystem.lock.redlock.LockType;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.filesystem.utils.*;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.message.jsonmsg.FSIdentity;
import com.macrosan.message.jsonmsg.FSQuotaConfig;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.essearch.EsMetaTask;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NTStatus.*;
import static com.macrosan.filesystem.cifs.SMB2Header.SMB2_HDR_FLAG_ASYNC;
import static com.macrosan.filesystem.cifs.SMB2Header.SMB2_HDR_FLAG_REPLAY_OPERATION;
import static com.macrosan.filesystem.cifs.call.smb2.GetInfoCall.*;
import static com.macrosan.filesystem.cifs.types.smb2.FsQuotaInfo.FILE_VC_QUOTA_ENFORCE;
import static com.macrosan.filesystem.cifs.types.smb2.FsQuotaInfo.FILE_VC_QUOTA_TRACK;
import static com.macrosan.filesystem.cifs.types.smb2.SMB2FileId.needDeleteSet;
import static com.macrosan.filesystem.quota.FSQuotaConstants.FS_DIR_QUOTA;
import static com.macrosan.filesystem.utils.CheckUtils.cifsWritePermissionCheck;
import static com.macrosan.filesystem.utils.FSQuotaUtils.getQuotaBucketKey;
import static com.macrosan.filesystem.utils.FSQuotaUtils.getQuotaTypeKey;
import static com.macrosan.filesystem.utils.InodeUtils.isError;
import static com.macrosan.filesystem.utils.acl.ACLUtils.NFS_ACL_START;
import static com.macrosan.message.jsonmsg.Inode.*;

/**
 * @Author: WANG CHENXING
 * @Date: 2024/8/1
 * @Description:
 */
@Log4j2
public class SetInfo {
    private static final Node nodeInstance = Node.getInstance();

    public static final String IS_ALLOCATE = "1";

    private static final byte[] SET_INFO_FID = new byte[256];

    static {
        SET_INFO_FID[FSCC_FILE_ALLOCATION_INFORMATION & 0xff] = 1;
        SET_INFO_FID[FSCC_FILE_BASIC_INFORMATION & 0xff] = 1;
        SET_INFO_FID[FSCC_FILE_RENAME_INFORMATION & 0xff] = 1;
        SET_INFO_FID[FSCC_FILE_LINK_INFORMATION & 0xff] = 1;
        SET_INFO_FID[FSCC_FILE_END_OF_FILE_INFORMATION & 0xff] = 1;
        SET_INFO_FID[FSCC_FILE_DISPOSITION_INFORMATION & 0xff] = 1;
    }

    public static Mono<SMB2.SMB2Reply> setFsInfo(SMB2.SMB2Reply reply, Session session, SetInfoCall call) {
        SetInfoReply body = new SetInfoReply();

        switch (call.getFileInfoClass()) {
            case FSCC_FS_ATTRIBUTE_INFORMATION:
            case FSCC_FS_DEVICE_INFORMATION:
            case FSCC_FS_FULL_SIZE_INFORMATION:
            default:
                break;
        }

        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
        return Mono.just(reply);
    }

    /**
     * secInfo的fileInfo class只有0
     **/
    public static Mono<SMB2.SMB2Reply> setSecurityInfo(SMB2.SMB2Reply reply, Session session, SetInfoCall call, SMB2Header header) {
        SetInfoReply body = new SetInfoReply();
        CompoundRequest compoundRequest = header.getCompoundRequest();
        SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(compoundRequest);
        if (null == call.getInfo()) {
            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
            log.info("set secInfo type is not implemented, call: {}", call);
            return Mono.just(reply);
        }

        if (fileInfo == null) {
            reply.getHeader().setStatus(NTStatus.STATUS_FILE_CLOSED);
            log.info("fileInfo is null when process setSecInfo: {}", call);
            return Mono.just(reply);
        }

        return NFSBucketInfo.getBucketInfoReactive(fileInfo.getBucket())
                .flatMap(bucketInfo -> {
                    if (!cifsWritePermissionCheck(bucketInfo)) {
                        reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                        return Mono.just(reply);
                    } else {
                        SecurityInfo info = (SecurityInfo) call.getInfo();
                        Inode inode = fileInfo.openInode;

                        if (null == inode || InodeUtils.isError(inode)) {
                            reply.getHeader().setStatus(NTStatus.STATUS_IO_DEVICE_ERROR);
                            log.info("set security info fail, the inode is error: {}", null != inode ? inode.getLinkN() : null);
                            return Mono.just(reply);
                        }

                        String s3Id = session.getTreeAccount(header.getTid());
                        if (null == s3Id) {
                            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                            log.error("req account does not have identity info, please check and retry");
                            return Mono.just(reply);
                        }

                        FSIdentity identity = ACLUtils.getIdentityByS3IDAndInode(inode, s3Id);

                        if (null == identity) {
                            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                            log.error("req account does not have identity info, please check and retry");
                            return Mono.just(reply);
                        }

                        // 请求者sid
                        String reqSid = identity.getUserSid();
                        if (StringUtils.isBlank(reqSid)) {
                            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                            log.error("req account does not have SID, please check and retry");
                            return Mono.just(reply);
                        }

                        String ACEs = null;
                        String ownerSID = null;
                        String groupSID = null;
                        boolean update = false;
                        for (int i = 0; i < SCE_INFO_ARR.length; i++) {
                            int infoClass = SCE_INFO_ARR[i];
                            boolean isExist = (call.getAdditionalInfo() & infoClass) != 0;
                            if (isExist) {
                                switch (infoClass) {
                                    //设置当前inode的属主
                                    case OWNER_SECURITY_INFORMATION:
                                        if (CIFSACL.cifsACL) {
                                            log.info("【SET OWNER INF】: info: {}, req: {}, inoOwner: {}", info, s3Id, inode.getUid());
                                        }
                                        //将要更改为的属主
                                        if (null != info.getOwnerSID()) {
                                            ownerSID = SID.convertSIDToString(info.getOwnerSID());
                                            boolean isRootReq = ownerSID.startsWith(FSIdentity.USER_SID_PREFIX)
                                                    && ADMIN_S3ID.equals(s3Id) && ACLUtils.checkSID(ownerSID, true);
                                            boolean isOwner = (inode.getUid() == identity.getUid()) && ACLUtils.checkSID(ownerSID, true);

                                            if (isRootReq || isOwner) {
                                                update = true;
                                            } else {
                                                reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                                                log.error("account {} is not owner, can not change owner", s3Id);
                                                return Mono.just(reply);
                                            }
                                        } else {
                                            reply.getHeader().setStatus(NTStatus.STATUS_IO_DEVICE_ERROR);
                                            log.error("set security info fail, the ownerInfo is empty: obj: {}, info: {}", inode.getObjName(), info);
                                            return Mono.just(reply);
                                        }
                                        break;
                                    case GROUP_SECURITY_INFORMATION:
                                        if (CIFSACL.cifsACL) {
                                            log.info("【SET GROUP INF】: info: {}", info);
                                        }
                                        if (null != info.getGroupSID()) {
                                            groupSID = SID.convertSIDToString(info.getGroupSID());
                                            int changeGid = FSIdentity.getGidBySID(groupSID);
                                            //仅gid不同时才需要修改
                                            if (changeGid != inode.getGid()) {
                                                Set<Integer> group = ACLUtils.s3IDToGids.get(s3Id);
                                                //以nfs中的权限标准，root可以更改仍以属组；而owner只能更改到请求者所在的组
                                                boolean adminAccess = ADMIN_S3ID.equals(s3Id);
                                                boolean ownerAccess = inode.getUid() == identity.getUid() && null != group && group.contains(changeGid);
                                                if (groupSID.startsWith(FSIdentity.GROUP_SID_PREFIX) && adminAccess || ownerAccess && ACLUtils.checkSID(groupSID, false)) {
                                                    update = true;
                                                } else {
                                                    reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                                                    log.error("no permission change group, ownerAccess: {}", ownerAccess);
                                                    return Mono.just(reply);
                                                }
                                            }
                                        } else {
                                            reply.getHeader().setStatus(NTStatus.STATUS_IO_DEVICE_ERROR);
                                            log.error("set security info fail, the groupInfo is empty: obj: {}, info: {}", inode.getObjName(), info);
                                            return Mono.just(reply);
                                        }
                                        break;
                                    case DACL_SECURITY_INFORMATION:
                                        if (CIFSACL.cifsACL) {
                                            log.info("【SET DACL】info: {}", info);
                                        }
                                        if (null != info.getDACL() && null != info.getDACL().getAclList() && !info.getDACL().getAclList().isEmpty()) {
                                            if (ADMIN_S3ID.equals(s3Id) || inode.getUid() == identity.getUid()) {
                                                SMB2ACL dACL = info.getDACL();
                                                List<SMB2ACE> dACEs = dACL.getAclList();
                                                List<Inode.ACE> aceList = SMB2ACL.mapSMB2ACEToInodeACEs(dACEs);
                                                ACEs = Json.encode(aceList);
                                                update = true;
                                            } else {
                                                reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                                                log.error("account {} is not owner, can not change dACL", s3Id);
                                                return Mono.just(reply);
                                            }
                                        } else {
                                            reply.getHeader().setStatus(NTStatus.STATUS_IO_DEVICE_ERROR);
                                            log.error("set security info fail, the dACL is empty: obj: {}, info: {}", inode.getObjName(), info);
                                            return Mono.just(reply);
                                        }
                                        break;
                                    case SACL_SECURITY_INFORMATION:
                                        log.info("【SET SACL】info: {}", info);
                                        break;
                                    case LABEL_SECURITY_INFORMATION:
                                        log.info("【SET label ACL】info: {}", info);
                                        break;
                                    case ATTRIBUTE_SECURITY_INFORMATION:
                                        log.info("【SET attr ACL】info: {}", info);
                                        break;
                                    case SCOPE_SECURITY_INFORMATION:
                                        log.info("【SET scope ACL】info: {}", info);
                                        break;
                                    case BACKUP_SECURITY_INFORMATION:
                                        log.info("【SET backup ACL】info: {}", info);
                                        break;
                                    default:
                                        log.info("【default】 type: {}, info: {}", call.getAdditionalInfo(), info);
                                        break;
                                }
                            }
                        }

                        reply.setBody(body);
                        if (update) {
                            String finalACEs = ACEs;
                            String finalOwnerSID = ownerSID;
                            String finalGroupSID = groupSID;
                            return Mono.just(StringUtils.isNotBlank(ownerSID))
                                    .flatMap(b -> {
                                        if (b) {
                                            if (null != bucketInfo) {
                                                return Node.getInstance().updateCIFSACL(fileInfo.getBucket(), fileInfo.getInodeId(), finalACEs, finalOwnerSID, finalGroupSID, bucketInfo.get(BUCKET_USER_ID));
                                            }

                                            return Node.getInstance().updateCIFSACL(fileInfo.getBucket(), fileInfo.getInodeId(), finalACEs, finalOwnerSID, finalGroupSID, null);
                                        }

                                        return Node.getInstance().updateCIFSACL(fileInfo.getBucket(), fileInfo.getInodeId(), finalACEs, finalOwnerSID, finalGroupSID, null);
                                    })
                                    .map(i -> {
                                        if (InodeUtils.isError(i)) {
                                            reply.getHeader().setStatus(NTStatus.STATUS_IO_DEVICE_ERROR);
                                            log.info("set security info fail, the inode is error: {}", null != i ? i.getLinkN() : null);
                                            return reply;
                                        }

                                        reply.getHeader().status = STATUS_SUCCESS;
                                        return reply;
                                    });
                        }

                        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_SUCCESS);
                        return Mono.just(reply);
                    }
                });
    }

    public static Mono<SMB2.SMB2Reply> setFileInfo(SMB2Header header, SMB2.SMB2Reply reply, Session session, SetInfoCall call, CompoundRequest compoundRequest) {
        SetInfoReply body = new SetInfoReply();

        SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(compoundRequest);

        //需要fileInfo的接口，并且fileInfo是空的，返回STATUS_FILE_CLOSED
        if (fileInfo == null && SET_INFO_FID[call.getFileInfoClass() & 0xff] == 1) {
            reply.getHeader().setStatus(NTStatus.STATUS_FILE_CLOSED);
            return Mono.just(reply);
        }

        switch (call.getFileInfoClass()) {
            case FSCC_FILE_ALLOCATION_INFORMATION:
                return NFSBucketInfo.getBucketInfoReactive(fileInfo.getBucket())
                        .flatMap(bucketInfo -> {
                            if (!cifsWritePermissionCheck(bucketInfo)) {
                                reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                return Mono.just(reply);
                            } else {
                                FileAllocationInfo fileAllocationInfo = (FileAllocationInfo) call.getInfo();
                                ObjAttr allocateAttr = new ObjAttr();
                                allocateAttr.size = fileAllocationInfo.allocationSize;
                                allocateAttr.hasSize = 1;
                                reply.setBody(body);
                                return nodeInstance.setAttr(fileInfo.inodeId, fileInfo.bucket, allocateAttr, null, IS_ALLOCATE)
                                        .flatMap(inode -> {
                                            if (InodeUtils.isError(inode)) {
                                                return setInfoReturnErrorReply(reply, inode);
                                            }

                                            fileInfo.openInode = inode;
                                            return Mono.just(reply);
                                        });
                            }
                        });
            case FSCC_FILE_BASIC_INFORMATION:
                return NFSBucketInfo.getBucketInfoReactive(fileInfo.getBucket())
                        .flatMap(bucketInfo -> {
                            if (!cifsWritePermissionCheck(bucketInfo)) {
                                reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                return Mono.just(reply);
                            } else {
                                FileBasicInfo fileBasicInfo = (FileBasicInfo) call.getInfo();
                                fileInfo.updateTimeOnClose = false;
                                SMB2FileId.updateFileInfo(compoundRequest, call.getFileId(), fileInfo);
                                String bucketName = call.getFileId().getFileInfo(compoundRequest).getBucket();
                                long nodeId0 = call.getFileId().getFileInfo(compoundRequest).getInodeId();
                                ObjAttr basicObjAttr = new ObjAttr();
                                boolean needUpdate = false;
                                if (fileBasicInfo.creationTime > 0) {
                                    basicObjAttr.hasCreateTime = 1;
                                    basicObjAttr.createTime = (int) CifsUtils.SMBTimeToStamp(fileBasicInfo.creationTime);
                                    needUpdate = true;
                                }
                                if (fileBasicInfo.lastAccessTime > 0) {
                                    basicObjAttr.hasAtime = 2;
                                    basicObjAttr.atime = (int) CifsUtils.SMBTimeToStamp(fileBasicInfo.lastAccessTime);
                                    basicObjAttr.atimeNano = (int) CifsUtils.getSMBTimeNano(fileBasicInfo.lastAccessTime);
                                    needUpdate = true;
                                }
                                if (fileBasicInfo.lastWriteTime > 0) {
                                    basicObjAttr.hasMtime = 2;
                                    basicObjAttr.mtime = (int) CifsUtils.SMBTimeToStamp(fileBasicInfo.lastWriteTime);
                                    basicObjAttr.mtimeNano = (int) CifsUtils.getSMBTimeNano(fileBasicInfo.lastWriteTime);
                                    needUpdate = true;
                                }
                                if (fileBasicInfo.lastChangeTime > 0) {
                                    basicObjAttr.hasCtime = 2;
                                    int setCtime = (int) CifsUtils.SMBTimeToStamp(fileBasicInfo.lastChangeTime);
                                    if (basicObjAttr.mtime > 0) {
                                        basicObjAttr.ctime = basicObjAttr.mtime;
                                        basicObjAttr.ctimeNano = basicObjAttr.mtimeNano;
                                    } else {
                                        basicObjAttr.ctime = setCtime;
                                        basicObjAttr.ctimeNano = (int) CifsUtils.getSMBTimeNano(fileBasicInfo.lastChangeTime);
                                    }
                                    needUpdate = true;
                                }
                                if (fileBasicInfo.mode != 0) {
                                    basicObjAttr.hasCifsMode = 1;
                                    basicObjAttr.cifsMode = fileBasicInfo.mode;
                                    needUpdate = true;
                                }
                                reply.setBody(body);
                                if (!needUpdate) {
                                    return Mono.just(reply);
                                }

                                SMB2FileId smb2FileId = (header.getCompoundRequest() != null && header.getCompoundRequest().getFileId() != null) ? header.getCompoundRequest().getFileId() : call.getFileId();
                                boolean isSMB3 = header.getHandler().negprotInfo.getDialect() >= NegprotCall.SMB_3_0_0;

                                return WriteCache.isExistCache(nodeId0, isSMB3, smb2FileId)
                                        .flatMap(isExist -> {
                                            if (isExist) {
                                                return nodeInstance.getInode(bucketName, nodeId0)
                                                        .flatMap(inode -> {
                                                            if (InodeUtils.isError(inode)) {
                                                                throw new NFSException(STATUS_IO_DEVICE_ERROR, "find inode error.");
                                                            }
                                                            return (isSMB3
                                                                    ? WriteCacheClient.flush(inode, 0, 0, smb2FileId)
                                                                    : WriteCache.getCache(bucketName, nodeId0, 0, inode.getStorage(), true).flatMap(writeCache -> writeCache.nfsCommit(inode, 0, 0)));
                                                        });
                                            }
                                            return Mono.just(true);
                                        })
                                        .flatMap(res -> {
                                            return nodeInstance.setAttr(nodeId0, bucketName, basicObjAttr, null)
                                                    .flatMap(inode -> {
                                                        if (InodeUtils.isError(inode)) {
                                                            reply.getHeader().setStatus(STATUS_IO_DEVICE_ERROR);
                                                            return Mono.just(reply);
                                                        }
                                                        fileInfo.openInode = inode;
                                                        return Mono.just(reply);
                                                    });
                                        });
                            }
                        });

            case FSCC_FILE_RENAME_INFORMATION:
                return NFSBucketInfo.getBucketInfoReactive(fileInfo.getBucket())
                        .flatMap(bucketInfo -> {
                            if (!cifsWritePermissionCheck(bucketInfo)) {
                                reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                return Mono.just(reply);
                            } else {
                                FileRenameInfo fileRenameInfo = (FileRenameInfo) call.getInfo();
                                // rename；如果源文件不存在则不会触发setFileInfo rename
                                String newObj = new String(fileRenameInfo.fileName).replace("\\", "/");
                                String oldObj = fileInfo.getObj().replace("\\", "/");
                                long nodeId = fileInfo.getInodeId();
                                String bucket = fileInfo.getBucket();

                                if (newObj.getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH || CheckUtils.isFileNameTooLong(newObj)) {
                                    reply.getHeader().setStatus(STATUS_NAME_TOO_LONG);
                                    return Mono.just(reply);
                                }

                                reply.setBody(body);

                                //请求者的s3Id
                                String reqAccountId = session.getTreeAccount(header.getTid());
                                long dirNodeId = fileInfo.dirInode;

                                ReqInfo reqHeader = new ReqInfo();
                                reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(bucket);
                                reqHeader.bucket = bucket;
                                reqHeader.nfsHandler = new NFSHandler();
                                boolean[] needRemoveDelete = {false};

                                return Mono.just(true)
                                        .flatMap(b -> {
                                            if (null != bucketInfo && !bucketInfo.isEmpty() && NFSBucketInfo.isCIFSShare(bucketInfo)) {
                                                reqHeader.bucketInfo = bucketInfo;
                                            }

                                            //源本身就是根目录，此时权限不做判断
                                            if (nodeId == 1) {
                                                return Mono.just(true);
                                            }

                                            //开了nfs权限，桶需要具备删除权限，即写的权限；对象ACL同样需要具备写的权限，父目录nfs需要具备写的权限，rename端只需要比较写权限即可
                                            return CIFSACL.checkCIFSBucketACL(bucketInfo, reqAccountId, bucket, FsConstants.SMB2ACCESS_MASK.SET_FILE_WRITE_RIGHTS, true, -1)
                                                    .flatMap(bucketACLRes -> {
                                                        if (!bucketACLRes.var1) {
                                                            log.error("【cifs-setInfo-rename】account: {}, dose not have bucket acl permission of obj: {}", reqAccountId, dirNodeId);
                                                            return Mono.just(false);
                                                        }

                                                        //没有开CIFS权限开关，所有权限不做判断
                                                        if (!ACLUtils.CIFS_ACL_START) {
                                                            return Mono.just(true);
                                                        }

                                                        com.macrosan.utils.functional.Tuple2<Boolean, Long> cifsMountPerm = CIFSACL.isCIFSWritePermitted(bucketInfo, FsConstants.SMB2ACCESS_MASK.SET_FILE_WRITE_RIGHTS);
                                                        if (!cifsMountPerm.var1) {
                                                            log.error("【cifs-setInfo-rename】account: {}, dose not have nfs mount permission of obj: {}", reqAccountId, dirNodeId);
                                                            return Mono.just(false);
                                                        }

                                                        return nodeInstance.getInode(bucket, dirNodeId)
                                                                .flatMap(srcDirInode -> {
                                                                    if (InodeUtils.isError(srcDirInode)) {
                                                                        return Mono.just(false);
                                                                    }

                                                                    boolean nfsStart = NFSBucketInfo.isNFSShare(bucketInfo) && NFS_ACL_START;
                                                                    return CIFSACL.judgeDirDelete(srcDirInode, reqAccountId, 12, header, nfsStart, bucketInfo, needRemoveDelete);
                                                                });
                                                    });
                                        })
                                        .flatMap(pass -> {
                                            if (!pass) {
                                                reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                                return Mono.just(reply);
                                            }

                                            boolean[] caseSensitive = {SMB2.caseSensitive};
                                            if (bucketInfo.containsKey(BUCKET_CASE_SENSITIVE)) {
                                                //大小写敏感关闭，即大小写不敏感
                                                if ("0".equals(bucketInfo.get(BUCKET_CASE_SENSITIVE))) {
                                                    caseSensitive[0] = false;
                                                } else if ("1".equals(bucketInfo.get(BUCKET_CASE_SENSITIVE))) {
                                                    caseSensitive[0] = true;
                                                }
                                            }
                                            return rename(bucket, nodeId, oldObj, newObj, fileRenameInfo.replaceIfExists, header, reply, call, session, compoundRequest, reqHeader, needRemoveDelete[0], reqAccountId, caseSensitive[0]);
                                        });
                            }
                        });
            case FSCC_FILE_LINK_INFORMATION:
                return NFSBucketInfo.getBucketInfoReactive(fileInfo.getBucket())
                        .flatMap(bucketInfo -> {
                            if (!cifsWritePermissionCheck(bucketInfo)) {
                                reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                return Mono.just(reply);
                            } else {
                                FileLinkInfo fileLinkInfo = (FileLinkInfo) call.getInfo();
                                if (!fileLinkInfo.fileExist) {
                                    String linkName = new String(fileLinkInfo.fileName).replace("\\", "/");
                                    long sourceNodeId = fileInfo.getInodeId();
                                    reply.setBody(body);
                                    if (linkName.getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH || CheckUtils.isFileNameTooLong(linkName)) {
                                        reply.getHeader().setStatus(STATUS_NAME_TOO_LONG);
                                        return Mono.just(reply);
                                    }

                                    return nodeInstance.createHardLink(fileInfo.bucket, sourceNodeId, linkName)
                                            .flatMap(inode -> {
                                                if (InodeUtils.isError(inode)) {
                                                    return setInfoReturnErrorReply(reply, inode);
                                                }
                                                if (FILES_QUOTA_EXCCED_INODE.getLinkN() == inode.getLinkN()) {
                                                    reply.getHeader().setStatus(STATUS_QUOTA_EXCEEDED);
                                                }
                                                return Mono.just(reply).flatMap(i -> {
                                                    boolean esSwitch = ES_ON.equals(NFSBucketInfo.getBucketInfo(fileInfo.bucket).get(ES_SWITCH));
                                                    if (esSwitch) {
                                                        return (EsMetaTask.putLinkEsMeta(inode, inode.clone().setObjName(fileInfo.obj)))
                                                                .map(b0 -> reply);
                                                    }
                                                    return Mono.just(reply);
                                                });
                                            });
                                } else {
                                    reply.getHeader().setStatus(STATUS_OBJECT_NAME_COLLISION);
                                    return Mono.just(reply);
                                }
                            }
                        });
            case FSCC_FILE_END_OF_FILE_INFORMATION:
                return NFSBucketInfo.getBucketInfoReactive(fileInfo.getBucket())
                        .flatMap(bucketInfo -> {
                            if (!cifsWritePermissionCheck(bucketInfo)) {
                                reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                return Mono.just(reply);
                            } else {
                                FileEndInfo fileEndInfo = (FileEndInfo) call.getInfo();
                                ObjAttr objAttr = new ObjAttr();
                                objAttr.hasSize = 1;
                                objAttr.size = fileEndInfo.endOfFile;
                                reply.setBody(body);
                                return nodeInstance.setAttr(fileInfo.inodeId, fileInfo.bucket, objAttr, null)
                                        .flatMap(inode -> {
                                            if (InodeUtils.isError(inode)) {
                                                return setInfoReturnErrorReply(reply, inode);
                                            }

                                            if (inode.isDeleteMark()) {
                                                return Mono.just(reply);
                                            }

                                            fileInfo.openInode = inode;
                                            return Mono.just(reply);
                                        });
                            }
                        });
            case FSCC_FILE_DISPOSITION_INFORMATION:
                return NFSBucketInfo.getBucketInfoReactive(fileInfo.getBucket())
                        .flatMap(bucketInfo -> {
                            if (!cifsWritePermissionCheck(bucketInfo)) {
                                reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                return Mono.just(reply);
                            } else {
                                FileDispositionInfo fileDispositionInfo = (FileDispositionInfo) call.getInfo();
                                if (fileDispositionInfo.needDelete == (byte) 1) {
                                    return nodeInstance.getInode(fileInfo.bucket, fileInfo.inodeId)
                                            .flatMap(inode -> {
                                                if (InodeUtils.isError(inode)) {
                                                    return setInfoReturnErrorReply(reply, inode);
                                                }

                                                if (inode.isDeleteMark()) {
                                                    reply.setBody(body);
                                                    return Mono.just(reply);
                                                }

                                                if (inode.getObjName().endsWith("/")) {
                                                    return ReadDirCache.listAndCache(fileInfo.bucket, inode.getObjName(), 0, 1024, new NFSHandler(), inode.getNodeId(), null, inode.getACEs())
                                                            .flatMap(inodes -> {
                                                                if (!inodes.isEmpty()) {
                                                                    reply.getHeader().setStatus(STATUS_DIRECTORY_NOT_EMPTY);
                                                                    return Mono.just(reply);
                                                                }
                                                                needDeleteSet.add(fileInfo.inodeId);
                                                                reply.setBody(body);
                                                                return Mono.just(reply);
                                                            });
                                                }
                                                needDeleteSet.add(fileInfo.inodeId);
                                                reply.setBody(body);
                                                return Mono.just(reply);
                                            });
                                }
                                reply.setBody(body);
                                return Mono.just(reply);
                            }
                        });
            default:
                break;
        }

        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
        return Mono.just(reply);
    }

    /**
     * @param bucket 桶名
     * @param nodeId 待修改文件的nodeId
     * @param oldObj 待修改文件的源文件/目录名
     * @param newObj 将要修改成的目标文件/目录名
     * @param reply  返回的reply
     **/
    public static Mono<SMB2.SMB2Reply> rename(String bucket, long nodeId, String oldObj, String newObj, boolean canOverWrite,
                                              SMB2Header header, SMB2.SMB2Reply reply, SetInfoCall call, Session session, CompoundRequest compoundRequest, ReqInfo reqHeader, boolean dirSubDel, String reqAccount, boolean caseSensitive) {
        if (compoundRequest.getFileId() == null) {
            compoundRequest.setFileId(call.getFileId());
        }

        if ((header.flags & SMB2_HDR_FLAG_REPLAY_OPERATION) != 0) {
            // 重试请求
            if (Session.renameReplyMap.containsKey(call.getFileId())) {
                log.info("rename replay, {}", call.getFileId());
                compoundRequest.execNum = -compoundRequest.execNum;
                Session.renameReplyMap.put(call.getFileId(), Tuples.of(session, compoundRequest));
                reply.getHeader().flags = reply.getHeader().flags | SMB2_HDR_FLAG_ASYNC | SMB2_HDR_FLAG_REPLAY_OPERATION;
                reply.getHeader().status = NTStatus.STATUS_PENDING;
                return Mono.just(reply);
            }
        }

        return RedLockClient.lock(reqHeader, oldObj, LockType.WRITE, true, true)
                .flatMap(lock -> {
                    session.lock = reqHeader.lock;
                    return nodeInstance.getInode(bucket, nodeId);
                })
                .flatMap(inode -> {
                    // 如果源文件不存在，则重命名失败
                    if (InodeUtils.isError(inode)) {
                        log.error("rename input/output error: obj: {}, nodeId: {}, linkN: {}", oldObj, nodeId, inode.getLinkN());
                        return setInfoReturnErrorReply(reply, inode)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }

                    // 若父目录不具备cifs的删除子文件夹和子文件的权限，则需要检查源的删除权限
                    // 检查源是否具有 cifs 删除权限，对象ACL在s3中不限制删除操作，删除仅由桶ACL限制，因此此处暂时仅判断cifs本身是否具有删除权限
                    if (!dirSubDel && !CIFSACL.judgeCifsSrcDelete(reqHeader.bucketInfo, inode, reqAccount)) {
                        reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                        return Mono.just(reply)
                                .doOnNext(r -> releaseLock(reqHeader, r));
                    }

                    boolean isDir = StringUtils.isNotEmpty(inode.getObjName()) && inode.getObjName().endsWith("/");
                    AtomicBoolean overWrite = new AtomicBoolean(false);

                    String[] oldObjName0 = new String[1];
                    String[] newObjName0 = new String[1];

                    oldObjName0[0] = isDir ? oldObj + '/' : oldObj;
                    newObjName0[0] = isDir ? newObj + '/' : newObj;
                    return LookupCache.lookup(bucket, newObj, reqHeader, caseSensitive, true, -1, null)
                            .flatMap(newInode -> {
                                // 目标inode存在
                                if (!InodeUtils.isError(newInode)) {
                                    if (!canOverWrite) {
                                        reply.getHeader().setStatus(STATUS_OBJECT_NAME_COLLISION);
                                        return Mono.just(reply)
                                                .doOnNext(r -> releaseLock(reqHeader, r));
                                    }
                                    //如果源的名称和传入的oldObj不一致，说明是多客户端并发修改同一个源，此时源已重命名成功
                                    if (newInode.getNodeId() == inode.getNodeId() && newInode.getLinkN() == 1) {
                                        reply.getHeader().setStatus(STATUS_OBJECT_NAME_NOT_FOUND);
                                        log.info("rename not found: obj: {}, nodeId: {}, linkN: {}", oldObj, nodeId, inode.getLinkN());
                                        return Mono.just(reply)
                                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                    }
                                    overWrite.set(true);
                                }

                                //检查目标目录是否具备对象ACL写权限、nfs写权限和cifs创建权限
                                return CIFSACL.judgeCifsTarDirCreate(reqHeader.bucket, reqHeader.bucketInfo, newObj, header, overWrite.get(), newInode, reqAccount)
                                        .flatMap(pass -> {
                                            if (!pass) {
                                                reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                                return Mono.just(reply)
                                                        .doOnNext(r3 -> releaseLock(reqHeader, r3));
                                            }

                                            // 如果源为目录
                                            if (isDir) {
                                                // 如果更新后的目录名已经存在，则检查这个新的目录名之下是否还有与旧目录名称相同的dirInode
                                                return Mono.just(overWrite.get())
                                                        .flatMap(isOverWrite -> {
                                                            return FSQuotaUtils.canRename(bucket, nodeId, newInode)
                                                                    .flatMap(can -> {
                                                                        if (!can) {
                                                                            reply.getHeader().setStatus(STATUS_IO_DEVICE_ERROR);
                                                                            return Mono.just(reply)
                                                                                    .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                                        }
                                                                        if (isOverWrite) {
                                                                            String prefix = newInode.getObjName();
                                                                            return ReadDirCache.listAndCache(bucket, prefix, 0, 4096, reqHeader.nfsHandler, newInode.getNodeId(), null, newInode.getACEs())
                                                                                    .flatMap(list1 -> {
                                                                                        if (!list1.isEmpty()) {
                                                                                            log.error("directory already exists: {}", newObjName0[0]);
                                                                                            reply.getHeader().setStatus(STATUS_DIRECTORY_NOT_EMPTY);
                                                                                            return Mono.just(reply)
                                                                                                    .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                                                        }

                                                                                        MonoProcessor<SMB2.SMB2Reply> reNameReplyRes = MonoProcessor.create();
                                                                                        Mono.defer(() -> scanAndReName0(inode, bucket, newObjName0[0], oldObjName0[0], 0, reqHeader, reply, call, compoundRequest))
                                                                                                .doOnSubscribe(subscription -> {
                                                                                                    reqHeader.optCompleted = false;
                                                                                                    session.renameMap.put(oldObj, compoundRequest.getFileId());
                                                                                                })
                                                                                                .doOnNext(reply0 -> {
                                                                                                    reqHeader.optCompleted = true;
                                                                                                    session.renameMap.remove(oldObj);
                                                                                                    if (reqHeader.timeout) {
                                                                                                        log.info("rename end callback, {}", compoundRequest.getFileId());
                                                                                                        Tuple2<Session, CompoundRequest> tuple2 = Session.renameReplyMap.remove(compoundRequest.getFileId());
                                                                                                        if (tuple2 != null) {
                                                                                                            if (reply0.getHeader().sessionId != tuple2.getT1().sessionId) { // win端重新建立连接的重试请求
                                                                                                                reply0.getHeader().sessionId = tuple2.getT1().sessionId;
                                                                                                                reply0.getHeader().flags = reply0.getHeader().flags | SMB2_HDR_FLAG_REPLAY_OPERATION;
                                                                                                            }
                                                                                                            reply0.getHeader().flags = reply0.getHeader().flags | SMB2_HDR_FLAG_ASYNC;
                                                                                                            tuple2.getT1().getHandler().sendPendingList(reply0, tuple2.getT2());
                                                                                                        }
                                                                                                    }
                                                                                                    reNameReplyRes.onNext(reply0);
                                                                                                })
                                                                                                .subscribe();
                                                                                        return reNameReplyRes;
                                                                                    });
                                                                        } else {
                                                                            // 将oldInode视为dirInode，遍历该目录下的所有子目录与文件
                                                                            // 重命名时不需要再get新的inode，直接rename即可
                                                                            MonoProcessor<SMB2.SMB2Reply> reNameReplyRes = MonoProcessor.create();
                                                                            Mono.defer(() -> scanAndReName0(inode, bucket, newObjName0[0], oldObjName0[0], 0, reqHeader, reply, call, compoundRequest))
                                                                                    .doOnSubscribe(subscription -> {
                                                                                        reqHeader.optCompleted = false;
                                                                                        session.renameMap.put(oldObj, compoundRequest.getFileId());
                                                                                    })
                                                                                    .doOnNext(reply0 -> {
                                                                                        reqHeader.optCompleted = true;
                                                                                        session.renameMap.remove(oldObj);
                                                                                        if (reqHeader.timeout) {
                                                                                            log.info("rename end callback, {}", compoundRequest.getFileId());
                                                                                            Tuple2<Session, CompoundRequest> tuple2 = Session.renameReplyMap.remove(compoundRequest.getFileId());
                                                                                            if (tuple2 != null) {
                                                                                                if (reply0.getHeader().sessionId != tuple2.getT1().sessionId) { // win端重新建立连接的重试请求
                                                                                                    reply0.getHeader().sessionId = tuple2.getT1().sessionId;
                                                                                                    reply0.getHeader().flags = reply0.getHeader().flags | SMB2_HDR_FLAG_REPLAY_OPERATION;
                                                                                                }
                                                                                                reply0.getHeader().flags = reply0.getHeader().flags | SMB2_HDR_FLAG_ASYNC;
                                                                                                tuple2.getT1().getHandler().sendPendingList(reply0, tuple2.getT2());
                                                                                            }
                                                                                        }
                                                                                        reNameReplyRes.onNext(reply0);
                                                                                    })
                                                                                    .subscribe();
                                                                            return reNameReplyRes;
                                                                        }
                                                                    });
                                                        });
                                                // 如果源是文件
                                            } else {
                                                return renameFile(nodeId, newInode, oldObjName0[0], newObjName0[0], bucket, overWrite.get(), new AtomicInteger(0))
                                                        .flatMap(i -> {
                                                            if (InodeUtils.isError(i) || i.isDeleteMark()) {
                                                                if (i.getLinkN() == ERROR_INODE.getLinkN()) {
                                                                    reply.getHeader().setStatus(STATUS_IO_DEVICE_ERROR);
                                                                    log.error("rename file nodeId: {}, obj: {} to newObj: {} fail, overwirte: {}, newInode: {}", nodeId, oldObjName0[0], newObjName0[0], overWrite.get(), newInode);
                                                                } else {
                                                                    reply.getHeader().setStatus(STATUS_OBJECT_NAME_NOT_FOUND);
                                                                }

                                                                return Mono.just(reply)
                                                                        .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                            }

                                                            reply.getHeader().setStatus(STATUS_SUCCESS);
                                                            return Flux.just(oldObjName0[0], newObjName0[0])
                                                                    .flatMap(obj -> InodeUtils.findDirInode(obj, bucket))
                                                                    .collectList()
                                                                    .flatMap(dirInodes -> {
                                                                        call.getFileId().getFileInfo(compoundRequest).setObj(newObjName0[0]);
                                                                        return updateTime(dirInodes, reply);
                                                                    })
                                                                    .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                        });
                                            }
                                        });
                            });

                })
                .doFinally(s -> {
                    if (!reqHeader.optCompleted) {
                        log.info("rename running {}", compoundRequest.getFileId());
                        compoundRequest.execNum = -compoundRequest.execNum;
                        Session.renameReplyMap.put(compoundRequest.getFileId(), Tuples.of(session, compoundRequest));
                    }
                    reqHeader.timeout = true;
                });
    }

    public static Mono<SMB2.SMB2Reply> releaseLock(ReqInfo reqHeader, SMB2.SMB2Reply reply) {
        if (!reqHeader.lock.isEmpty()) {
            for (String key : reqHeader.lock.keySet()) {
                String value = reqHeader.lock.get(key);
                RedLockClient.unlock(reqHeader.bucket, key, value, true).subscribe();
            }
        }
        return Mono.just(reply);
    }

    /**
     * rename完成后更新fromDirFh与toDirFh代表的父级目录的mtime和ctime
     **/
    public static Mono<SMB2.SMB2Reply> updateTime(List<Inode> inodeList, SMB2.SMB2Reply reply) {
        // 如果两个inode的nodeId相等，只发送一次更改时间的请求
        if (inodeList.size() == 2 && !InodeUtils.isError(inodeList.get(0)) &&
                !InodeUtils.isError(inodeList.get(1)) && inodeList.get(0).getNodeId() == inodeList.get(1).getNodeId()) {
            return nodeInstance.updateInodeTime(inodeList.get(0).getNodeId(), inodeList.get(0).getBucket(), System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), false, true, true)
                    .map(inode -> reply)
                    .doOnError(e -> log.info("update time error 1", e))
                    .onErrorReturn(reply);
        } else {
            long stamp = System.currentTimeMillis() / 1000;
            int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
            return Flux.fromIterable(inodeList)
                    .flatMap(dirInode -> nodeInstance.updateInodeTime(dirInode.getNodeId(), dirInode.getBucket(), stamp, stampNano, false, true, true))
                    .collectList()
                    .map(list -> reply)
                    .doOnError(e -> log.info("update time error 2", e))
                    .onErrorReturn(reply);
        }
    }

    public static Mono<Inode> renameFile(long oldNodeId, Inode newInode, String oldObjName, String newObjName, String bucket, boolean isOverWrite, AtomicInteger renameNum) {
//        log.info("{}, {} --> {}", renameId, oldObjName, newObjName);
        if (renameNum != null) {
            int num = renameNum.incrementAndGet();
            if ((num % 100000) == 0) {
                log.info("rename {} --> {}, num:{}", oldObjName, newObjName, num);
            }
        }
        boolean esSwitch = ES_ON.equals(NFSBucketInfo.getBucketInfo(bucket).get(ES_SWITCH));
        return Mono.just(isOverWrite)
                .flatMap(b -> {
                    if (b) {
                        // mv a.txt b.txt，如果b.txt文件已存在，则需在改名前删除b.txt
                        return nodeInstance.deleteInode(newInode.getNodeId(), newInode.getBucket(), newInode.getObjName())
                                .flatMap(f -> esSwitch && !isError(f) ? EsMetaTask.delEsMeta(f, newInode, bucket, newObjName, newInode.getNodeId(), false).map(v -> f) : Mono.just(f))
                                .flatMap(inode1 -> {
                                    return InodeUtils.updateParentDirTime(newInode.getObjName(), bucket)
                                            .flatMap(r -> {
                                                return Mono.just(inode1);
                                            });
                                })
                                .map(i -> {
                                    if (InodeUtils.isError(i)) {
                                        log.error("rename: delete {} failed: {}", newObjName, i.getLinkN());
                                        return false;
                                    } else {
                                        return true;
                                    }
                                });
                    } else {
                        return Mono.just(true);
                    }
                })
                .flatMap(b0 -> {
                    if (!b0) {
                        return Mono.just(ERROR_INODE);
                    }

                    return nodeInstance.renameFile(oldNodeId, oldObjName, newObjName, bucket)
                            .flatMap(inode -> {
                                if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                    log.info("rename {}->{} fail: {}", oldObjName, newObjName, inode.getLinkN());
                                    return Mono.just(inode);
                                }
                                return Mono.just(inode).flatMap(i -> {
                                    if (esSwitch) {
                                        Inode cInode = inode.clone().setObjName(newObjName);
                                        return EsMetaTask.mvEsMeta(cInode, bucket, oldObjName, newObjName, oldNodeId);
                                    }
                                    return Mono.just(true);
                                }).map(a -> inode);
                            });
                });
    }

    /**
     * @param dirInode   当前的目录的完整路径 /test0/fio/old/
     * @param newObjName 当前目录所要更改成的新目录名 /test0/fio/new/
     * @param bucket     桶名
     * @param count      offset
     **/
    public static Mono<Inode> scanAndReName(Inode dirInode, String bucket, String newObjName, long count, NFSHandler nfsHandler, AtomicInteger renameNum) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        AtomicLong offset = new AtomicLong(count);
        UnicastProcessor<Boolean> listController = UnicastProcessor.create(Queues.<Boolean>unboundedMultiproducer().get());
        listController.concatMap(isEmpty -> {
                    // 如果是非空的话则继续往下遍历；如果是空的话结束遍历，直接更改当前目录本身
                    return scanAndReNamePart(dirInode, bucket, newObjName, offset, nfsHandler, renameNum)
                            .flatMap(list -> {
                                if (!list.isEmpty()) {
                                    listController.onNext(false);
                                    return Mono.just(false);
                                } else {
                                    listController.onComplete();
                                    return Mono.just(true);
                                }
                            })
                            .onErrorResume(throwable -> {
                                listController.onComplete();
                                res.onError(throwable);
                                return Mono.just(false);
                            });
                })
                .doOnComplete(() -> {
                    res.onNext(true);
                })
                .subscribe();
        listController.onNext(false);
        return res
                .flatMap(ignore -> renameFile(dirInode.getNodeId(), null, dirInode.getObjName(), newObjName, bucket, false, renameNum))
                .onErrorResume(throwable -> {
                    return Mono.just(ERROR_INODE);
                });
    }

    public static Mono<List<Inode>> scanAndReNamePart(Inode dirInode, String bucket, String newObjName, AtomicLong offset, NFSHandler nfsHandler, AtomicInteger renameNum) {
        String prefix = dirInode.getObjName();
        return ReadDirCache.listAndCache(bucket, prefix, offset.get(), 1 << 20, nfsHandler, dirInode.getNodeId(), null, dirInode.getACEs())
                .flatMap(list -> {
                    if (list.size() > 0) {
                        offset.set(list.get(list.size() - 1).getCookie());
                    }
                    return Mono.just(list);
                })
                .flatMapMany(Flux::fromIterable)
                .flatMap(inode -> {
                    String curOldName = inode.getObjName();
                    String[] nameArray = inode.getObjName().split("/");
                    String curNewName = newObjName + nameArray[nameArray.length - 1];
                    if ((inode.getMode() & S_IFMT) == S_IFDIR) {
                        // 如果是目录则进行递归
                        if (!curNewName.endsWith("/")) {
                            curNewName = curNewName + "/";
                        }
                        return scanAndReName(inode, bucket, curNewName, 0, nfsHandler, renameNum);
                    } else {
                        // 如果是文件则进行重命名
                        return renameFile(inode.getNodeId(), null, curOldName, curNewName, bucket, false, renameNum);
                    }
                })
                .collectList();
    }

    public static Mono<SMB2.SMB2Reply> scanAndReName0(Inode oldInode, String bucket, String newObjName, String oldObjName, long offset, ReqInfo reqHeader, SMB2.SMB2Reply reNameReply, SetInfoCall call, CompoundRequest compoundRequest) {
        AtomicInteger renameNum = new AtomicInteger();
        return scanAndReName(oldInode, bucket, newObjName, offset, reqHeader.nfsHandler, renameNum)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        log.info("rename {}->{} fail: {}", oldInode.getObjName(), newObjName, inode.getLinkN());
                    }
                    if (renameNum.get() >= 100000) {
                        log.info("renameNum {}", renameNum.get());
                    }
                    reNameReply.getHeader().setStatus(STATUS_SUCCESS);
                    return Flux.just(oldObjName, newObjName)
                            .flatMap(obj -> InodeUtils.findDirInode(obj, bucket))
                            .collectList()
                            .flatMap(dirInodes -> {
                                SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(compoundRequest);
                                if (fileInfo != null) {
                                    fileInfo.setObj(newObjName);
                                }
                                return updateTime(dirInodes, reNameReply);
                            })
                            .onErrorReturn(reNameReply)
                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                });
    }

    public static Mono<SMB2.SMB2Reply> setQuotaInfo(SMB2.SMB2Reply reply, Session session, SetInfoCall call, SMB2Header header) {
        SetInfoReply body = new SetInfoReply();
        FileQuotaInfo fileQuotaInfo = (FileQuotaInfo) call.getInfo();
        SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(header.getCompoundRequest());

        //需要fileInfo的接口，并且fileInfo是空的，返回STATUS_FILE_CLOSED
        if (fileInfo == null) {
            reply.getHeader().setStatus(NTStatus.STATUS_FILE_CLOSED);
            return Mono.just(reply);
        }
        String dirName = CifsUtils.getParentDirName(fileInfo.obj);

        switch (call.getFileInfoClass()) {
            case FSCC_FILE_QUOTA_INFORMATION:
                return setQuotaInfo0(reply, fileInfo, dirName, body, 0, fileQuotaInfo.quotaThreshold, fileQuotaInfo.quotaLimit, FILE_VC_QUOTA_ENFORCE | FILE_VC_QUOTA_TRACK, header, session);
        }
        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_INVALID_INFO_CLASS);
        return Mono.just(reply);
    }

    public static Mono<SMB2.SMB2Reply> setFsInfo(SMB2.SMB2Reply reply, Session session, SetInfoCall call, SMB2Header header) {
        SetInfoReply body = new SetInfoReply();
        FsQuotaInfo fileQuotaInfo = (FsQuotaInfo) call.getInfo();
        SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(header.getCompoundRequest());

        //需要fileInfo的接口，并且fileInfo是空的，返回STATUS_FILE_CLOSED
        if (fileInfo == null) {
            reply.getHeader().setStatus(NTStatus.STATUS_FILE_CLOSED);
            return Mono.just(reply);
        }
        String dirName = CifsUtils.getParentDirName(fileInfo.obj);
        switch (call.getFileInfoClass()) {
            case FSCC_FS_QUOTA_INFORMATION:
                return setQuotaInfo0(reply, fileInfo, dirName, body, -1, fileQuotaInfo.defaultQuotaThreshold, fileQuotaInfo.defaultQuotaLimit, fileQuotaInfo.fsControlFlags, header, session);
        }
        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_INVALID_INFO_CLASS);
        return Mono.just(reply);
    }

    public static Mono<SMB2.SMB2Reply> setQuotaInfo0(SMB2.SMB2Reply reply, SMB2FileId.FileInfo fileInfo, String dirName, SetInfoReply body, int sid, long quotaThreshold, long quotaLimit, int quotaFlag, SMB2Header header, Session session) {
        if (quotaLimit > 0 && quotaThreshold >= quotaLimit) {
            reply.getHeader().status = STATUS_ACCESS_DENIED;
            return Mono.just(reply);
        }
        return FsUtils.lookup(fileInfo.bucket, dirName, null, false, -1, null)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        reply.getHeader().status = STATUS_IO_DEVICE_ERROR;
                        return Mono.just(reply);
                    }
                    return RedisConnPool.getInstance().getReactive(REDIS_FS_QUOTA_INFO_INDEX)
                            .hget(getQuotaBucketKey(fileInfo.bucket), getQuotaTypeKey(fileInfo.bucket, inode.getNodeId(), FS_DIR_QUOTA, sid))
                            .defaultIfEmpty("")
                            .flatMap(configStr -> {
                                if (StringUtils.isNotBlank(configStr)) {
                                    FSQuotaConfig fsQuotaConfig = Json.decodeValue(configStr, FSQuotaConfig.class);
                                    fsQuotaConfig.setCapacitySoftQuota(quotaThreshold);
                                    fsQuotaConfig.setCapacityHardQuota(quotaLimit);
                                    fsQuotaConfig.setCifsQuotaFlags(quotaFlag);
                                    fsQuotaConfig.setModifyTime(System.currentTimeMillis());
                                    fsQuotaConfig.setModify(true);
                                    return Mono.just(fsQuotaConfig);
                                } else if (inode.getNodeId() == 1) {
                                    return FSQuotaRealService.getFsQuotaConfig(fileInfo.bucket, inode.getNodeId(), FS_DIR_QUOTA, sid)
                                            .flatMap(fsQuotaConfig -> {
                                                fsQuotaConfig.setCapacitySoftQuota(quotaThreshold);
                                                fsQuotaConfig.setCapacityHardQuota(quotaLimit);
                                                fsQuotaConfig.setCifsQuotaFlags(quotaFlag);
                                                fsQuotaConfig.setModify(true);
                                                fsQuotaConfig.setModifyTime(System.currentTimeMillis());
                                                return Mono.just(fsQuotaConfig);
                                            });
                                } else {
                                    long stamp = System.currentTimeMillis();
                                    FSQuotaConfig fsQuotaConfig = new FSQuotaConfig()
                                            .setBucket(fileInfo.bucket)
                                            .setDirName(dirName.endsWith("/") ? dirName : dirName + '/')
                                            .setQuotaType(FS_DIR_QUOTA)
                                            .setFilesSoftQuota(0L)
                                            .setFilesHardQuota(0L)
                                            .setCapacityHardQuota(quotaLimit)
                                            .setCapacitySoftQuota(quotaThreshold)
                                            .setUid(0)
                                            .setGid(0)
                                            .setNodeId(inode.getNodeId())
                                            .setStartTime(stamp)
                                            .setModifyTime(stamp)
                                            .setModify(false)
                                            .setCifsQuotaFlags(quotaFlag)
                                            .setFilesTimeLeft(0)
                                            .setBlockTimeLeft(0);
                                    return Mono.just(fsQuotaConfig);
                                }
                            })
                            .flatMap(fsQuotaConfig -> {
                                if (quotaFlag != 0) {
                                    fsQuotaConfig.setCifsQuotaFlags(quotaFlag);
                                }
                                return RedisConnPool.getInstance()
                                        .getReactive(REDIS_BUCKETINFO_INDEX)
                                        .hgetall(fileInfo.bucket)
                                        .flatMap(bucketInfo -> {
                                            if (bucketInfo.isEmpty()) {
                                                reply.getHeader().status = STATUS_ACCESS_DENIED;
                                                return Mono.just(reply);
                                            }
                                            String s3Id = session.getTreeAccount(header.getTid());
                                            String inodeOwnerS3Id = ACLUtils.getInodeOwner(inode);
                                            if (DEFAULT_USER_ID.equals(inodeOwnerS3Id) || inode.getNodeId() == 1) {
                                                inodeOwnerS3Id = bucketInfo.get("user_id");
                                            }
                                            //只有目录的所有者才能修改
                                            if (!s3Id.equals(inodeOwnerS3Id)) {
                                                reply.getHeader().status = STATUS_ACCESS_DENIED;
                                                return Mono.just(reply);
                                            }
                                            fsQuotaConfig.setS3AccountName(bucketInfo.get("user_name"));
                                            return FSQuotaRealService.setFsQuotaInfo(fsQuotaConfig, bucketInfo)
                                                    .onErrorResume(e -> Mono.just(-1))
                                                    .flatMap(res -> {
                                                        reply.setBody(body);
                                                        if (res == -2) {
                                                            reply.getHeader().status = STATUS_NO_SUCH_FILE;
                                                            return Mono.just(reply);
                                                        }
                                                        if (res < 0) {
                                                            reply.getHeader().status = STATUS_ACCESS_DENIED;
                                                            return Mono.just(reply);
                                                        }
                                                        reply.getHeader().status = STATUS_SUCCESS;
                                                        return Mono.just(reply);
                                                    });
                                        });

                            });
                });

    }

    public static Mono<SMB2.SMB2Reply> setInfoReturnErrorReply(SMB2.SMB2Reply reply, Inode inode) {

        if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
            reply.getHeader().setStatus(STATUS_IO_DEVICE_ERROR);
        } else if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
            reply.getHeader().setStatus(STATUS_OBJECT_NAME_NOT_FOUND);
        }
        return Mono.just(reply);
    }
}
