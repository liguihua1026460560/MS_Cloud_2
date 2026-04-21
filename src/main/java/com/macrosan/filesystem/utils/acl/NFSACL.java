package com.macrosan.filesystem.utils.acl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.SysConstants;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cifs.types.smb2.SID;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.NFSV3;
import com.macrosan.filesystem.nfs.RpcCallHeader;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.nfs.reply.AccessReply;
import com.macrosan.filesystem.nfs.reply.GetAclReply;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.FSIPACLUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.AccessKeyVO;
import com.macrosan.message.jsonmsg.FSIdentity;
import com.macrosan.message.jsonmsg.FSIpACL;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.iam.IamUtils;
import com.macrosan.utils.serialize.JsonUtils;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.ACLConstants.PROTO;
import static com.macrosan.filesystem.FsConstants.NFSACLType.*;
import static com.macrosan.filesystem.FsConstants.NFSACLType.NFSACL_GROUP;
import static com.macrosan.filesystem.FsConstants.NFSAccessAcl.*;
import static com.macrosan.filesystem.FsConstants.NFSAccessAcl.EXECUTE;
import static com.macrosan.filesystem.FsConstants.SMB2ACCESS_MASK.DELETE_SUB_FILE_AND_DIR;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.CONTAINER_INHERIT_ACE;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.OBJECT_INHERIT_ACE;
import static com.macrosan.filesystem.FsConstants.SMB2ACEType.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEType.SYSTEM_SCOPED_POLICY_ID_ACE_TYPE;
import static com.macrosan.filesystem.nfs.reply.AccessReply.*;
import static com.macrosan.filesystem.nfs.reply.AccessReply.GROUP_EXEC;
import static com.macrosan.filesystem.utils.acl.ACLUtils.*;
import static com.macrosan.filesystem.utils.acl.ACLUtils.createCallHeader;
import static com.macrosan.message.jsonmsg.Inode.getAndSetNFSMask;

/**
* @Author: WANG CHENXING
* @Date:   2025/8/25
* @Description: 
*/

@Log4j2
public class NFSACL {
    /**
     * 在nfs access中调用，用于获取当前 inode 的权限
     *
     * @return 返回当前 inode 具备的权限，该权限需要与请求的 access取交集
     **/
    public static Mono<Integer> getNFSAccess(Inode inode, ReqInfo reqHeader, RpcCallHeader callHeader) {
        String bucket = reqHeader.bucket;
        return NFSBucketInfo.getBucketInfoReactive(bucket)
                .flatMap(bucketInfo -> {

                    //为兼容旧版本权限，如果nfs权限未开启，则仅进行nfs挂载时共享权限的校验
                    if (!NFS_ACL_START) {
                        int readAndWrite = (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE);
                        int read = (READ | LOOK_UP | EXECUTE);
                        boolean mountFlag = CheckUtils.writePermissionCheck(bucketInfo);
                        int resRight = mountFlag ? readAndWrite : read;

                        if (inode.getNodeId() > 1 && null != callHeader.auth && callHeader.auth.flavor == 1) {
                            if (callHeader.getAuth() instanceof AuthUnix) {
                                AuthUnix auth = (AuthUnix) callHeader.getAuth();
                                int uid = auth.getUid();
                                //访问或所属用户不为root，且访问用户不为文件所属用户，只有只读权限
                                if (inode.getUid() != 0 && uid != 0 && uid != inode.getUid()) {
                                    resRight = FsConstants.NFSAccessAcl.READ;
                                }
                            }
                        }

                        return Mono.just(resRight);
                    }

                    if (null != bucketInfo) {
                        FSIpACL ipACL = FSIPACLUtils.getIpACL(bucketInfo, FSIPACLUtils.getClientIp(reqHeader.nfsHandler));
                        if (ipACL == null ) {
                            return Mono.just(0);
                        }
                        reqHeader.ipACL = ipACL;
                        if (isNotNfsIpAclSquash(reqHeader, callHeader)) {
                            nfsSquash(reqHeader, callHeader);
                        }
                    }

                    int[] uidAndGid = ACLUtils.getUidAndGid(callHeader);
                    Tuple2<FSIdentity, Boolean> tuple2 = getIdtAndJudgeByUidAndInode(inode, callHeader);
                    FSIdentity identity = tuple2.var1;
                    boolean isIdtPromote = tuple2.var2;

                    //判断桶ACL；当前账户是否包含该桶的权限
                    return checkBucketACL(bucketInfo, identity.getS3Id(), bucket)
                            .flatMap(bucketACLRes -> {
                                if (!bucketACLRes.var1) {
                                    log.error("uid: {}, gid: {}, dose not have bucket permission of obj {}:{}", uidAndGid[0], uidAndGid[1], inode.getNodeId(), inode.getObjName());
                                    return Mono.just(0);
                                }

                                int[] right = {bucketACLRes.var2};
                                //判断nfs挂载是公共读还是公共读写
                                boolean mountFlag = CheckUtils.writePermissionCheck(bucketInfo);
                                right[0] &= (mountFlag ? (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE) : (READ | LOOK_UP | EXECUTE));

                                //判断对象ACL与UGO、NFS ACL; cifs acl粒度比nfs acl小，因此不在access接口中判断
                                return Flux.just(
                                        parseObjACLToNFSRight(inode, identity.getS3Id(), callHeader.opt, bucketInfo, null),
                                        parseUGOAndNFSACLToRight(inode, createNfsHeader(callHeader.opt, identity, isIdtPromote, callHeader), null),
                                        CIFSACL.judgeSMBACLToNFSRight(inode, identity, callHeader.opt, NFSBucketInfo.isCIFSShare(bucketInfo)))
                                        .collectList()
                                        .map(list -> {
                                            right[0] &= list.get(0).var2;
                                            right[0] &= list.get(1).var2;
                                            right[0] &= list.get(2).var2;
                                            return right[0];
                                        });
                            });
                });
    }

    /**
     * 在nfs access中调用，用于获取当前 inode 的权限
     * 暂用于nfsv4简单权限校验，不校验cifs权限
     * @return 返回当前 inode 具备的权限，该权限需要与请求的 access取交集
     **/
    public static Mono<Integer> getNFSAccess0(Inode inode, ReqInfo reqHeader, RpcCallHeader callHeader) {
        String bucket = reqHeader.bucket;
        return NFSBucketInfo.getBucketInfoReactive(bucket)
                .flatMap(bucketInfo -> {

                    //为兼容旧版本权限，如果nfs权限未开启，则仅进行nfs挂载时共享权限的校验
                    if (!NFS_ACL_START) {
                        int readAndWrite = (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE);
                        int read = (READ | LOOK_UP | EXECUTE);
                        boolean mountFlag = CheckUtils.writePermissionCheck(bucketInfo);
                        int resRight = mountFlag ? readAndWrite : read;

                        if (inode.getNodeId() > 1 && null != callHeader.auth && callHeader.auth.flavor == 1) {
                            if (callHeader.getAuth() instanceof AuthUnix) {
                                AuthUnix auth = (AuthUnix) callHeader.getAuth();
                                int uid = auth.getUid();
                                //访问或所属用户不为root，且访问用户不为文件所属用户，只有只读权限
                                if (inode.getUid() != 0 && uid != 0 && uid != inode.getUid()) {
                                    resRight = FsConstants.NFSAccessAcl.READ;
                                }
                            }
                        }

                        return Mono.just(resRight);
                    }

                    if (null != bucketInfo) {
                        FSIpACL ipACL = FSIPACLUtils.getIpACL(bucketInfo, FSIPACLUtils.getClientIp(reqHeader.nfsHandler));
                        if (ipACL == null ) {
                            return Mono.just(0);
                        }
                        reqHeader.ipACL = ipACL;
                        if (isNotNfsIpAclSquash(reqHeader, callHeader)) {
                            nfsSquash(reqHeader, callHeader);
                        }
                    }

                    int[] uidAndGid = ACLUtils.getUidAndGid(callHeader);
                    Tuple2<FSIdentity, Boolean> tuple2 = getIdtAndJudgeByUidAndInode(inode, callHeader);
                    FSIdentity identity = tuple2.var1;
                    boolean isIdtPromote = tuple2.var2;

                    //判断桶ACL；当前账户是否包含该桶的权限
                    return checkBucketACL(bucketInfo, identity.getS3Id(), bucket)
                            .flatMap(bucketACLRes -> {
                                if (!bucketACLRes.var1) {
                                    log.error("uid: {}, gid: {}, dose not have bucket permission of obj {}:{}", uidAndGid[0], uidAndGid[1], inode.getNodeId(), inode.getObjName());
                                    return Mono.just(0);
                                }

                                int[] right = {bucketACLRes.var2};
                                //判断nfs挂载是公共读还是公共读写
                                boolean mountFlag = CheckUtils.writePermissionCheck(bucketInfo);
                                right[0] &= (mountFlag ? (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE) : (READ | LOOK_UP | EXECUTE));

                                //判断对象ACL与UGO、NFS ACL; cifs acl粒度比nfs acl小，因此不在access接口中判断
                                return Flux.just(
                                                parseObjACLToNFSRight(inode, identity.getS3Id(), callHeader.opt, bucketInfo, null),
                                                parseUGOAndNFSACLToRight(inode, createNfsHeader(callHeader.opt, identity, isIdtPromote, callHeader), null)
                                                )

                                        .collectList()
                                        .map(list -> {
                                            right[0] &= list.get(0).var2;
                                            right[0] &= list.get(1).var2;
                                            return right[0];
                                        });
                            });
                });
    }


    /**
     * 在nfs access中调用，用于获取当前 inode 的权限
     *
     * @param cifsJudgeFlag 用于cifs判断权限的标志位
     * @return 返回当前 inode 具备的权限，该权限需要与请求的 access取交集
     **/
    public static Mono<Boolean> judgeNFSOptAccess(Inode inode, ReqInfo reqHeader, RpcCallHeader callHeader, boolean writeFlag, Map<String, String> extraParam, int cifsJudgeFlag) {
        String bucket = reqHeader.bucket;

        // todo accessKey
        String accessKey = "";
        return NFSBucketInfo.getBucketInfoReactive(bucket)
                .flatMap(bucketInfo -> {
                    reqHeader.bucketInfo = bucketInfo;

                    //如果nfs权限未开启，则不进行权限校验
                    if (!NFS_ACL_START) {
                        boolean notPass = writeFlag && !CheckUtils.writePermissionCheck(bucketInfo);
                        return Mono.just(!notPass);
                    }

                    if (null != bucketInfo) {
                        FSIpACL ipACL = FSIPACLUtils.getIpACL(bucketInfo, FSIPACLUtils.getClientIp(reqHeader.nfsHandler));
                        if (ipACL == null) {
                            return Mono.just(false);
                        }
                        reqHeader.ipACL = ipACL;
                        if (isNotNfsIpAclSquash(reqHeader, callHeader)) {
                            nfsSquash(reqHeader, callHeader);
                        }
                    }

                    int[] uidAndGid = ACLUtils.getUidAndGid(callHeader);
                    Tuple2<FSIdentity, Boolean> tuple2 = getIdtAndJudgeByUidAndInode(inode, callHeader);
                    FSIdentity identity = tuple2.var1;
                    boolean isIdtPromote = tuple2.var2;

                    return findSecretAccessKeyByIamFS(bucket, inode.getObjName(), callHeader.opt, accessKey)
                            .flatMap(b -> {
                                if (!b) {
                                    log.error("uid: {}, gid: {}, dose not have iam permission of obj {}:{}", uidAndGid[0], uidAndGid[1], inode.getNodeId(), inode.getObjName());
                                    return Mono.just(false);
                                }

                                //判断桶ACL；当前账户是否包含该桶的权限
                                return checkBucketACL(bucketInfo, identity.getS3Id(), bucket)
                                        .flatMap(bucketACLRes -> {
                                            if (!bucketACLRes.var1) {
                                                log.error("uid: {}, gid: {}, dose not have bucket acl permission of obj {}:{}", uidAndGid[0], uidAndGid[1], inode.getNodeId(), inode.getObjName());
                                                return Mono.just(false);
                                            } else {
                                                //根据桶ACL返回的权限，判断当前操作是否通过
                                                boolean isDir = inode.getObjName().endsWith("/") || inode.getNodeId() == 1;
                                                boolean isBucketACLPass = isOptPass(bucketACLRes.var2, callHeader.opt, isDir, extraParam);
                                                if (!isBucketACLPass) {
                                                    log.error("uid: {}, gid: {}, dose not have bucket acl permission of obj {}:{}", uidAndGid[0], uidAndGid[1], inode.getNodeId(), inode.getObjName());
                                                    return Mono.just(false);
                                                }
                                            }

                                            //判断nfs挂载是公共读还是公共读写
                                            boolean mountPerm = CheckUtils.writePermissionCheck(bucketInfo);
                                            if (writeFlag && !mountPerm) {
                                                log.error("uid: {}, gid: {}, dose not have nfs permission of obj {}:{}", uidAndGid[0], uidAndGid[1], inode.getNodeId(), inode.getObjName());
                                                return Mono.just(false);
                                            }

                                            //判断对象ACL与UGO、NFS ACL与CIFS ACL
                                            return Flux.just(
                                                        parseObjACLToNFSRight(inode, identity.getS3Id(), callHeader.opt, bucketInfo, extraParam).var1(),
                                                        parseUGOAndNFSACLToRight(inode, createNfsHeader(callHeader.opt, identity, isIdtPromote, callHeader), extraParam).var1(),
                                                        notNeedJudgeCifs(callHeader.opt, false) ? true : CIFSACL.judgeSMBACLToBoolean(inode, identity, callHeader.opt, NFSBucketInfo.isCIFSShare(bucketInfo), extraParam, cifsJudgeFlag))
                                                    .collectList()
                                                    .map(list -> list.get(0) && list.get(1) & list.get(2));
                                        });
                            });
                });
    }

    /**
     * 在nfs access中调用，用于获取当前 inode 的权限
     * 暂时用于nfsv4简单做权限校验，不校验cifs权限
     * @param cifsJudgeFlag 用于cifs判断权限的标志位
     * @return 返回当前 inode 具备的权限，该权限需要与请求的 access取交集
     **/
    public static Mono<Boolean> judgeNFSOptAccess0(Inode inode, ReqInfo reqHeader, RpcCallHeader callHeader, boolean writeFlag, Map<String, String> extraParam, int cifsJudgeFlag) {
        String bucket = reqHeader.bucket;

        // todo accessKey
        String accessKey = "";
        return NFSBucketInfo.getBucketInfoReactive(bucket)
                .flatMap(bucketInfo -> {
                    reqHeader.bucketInfo = bucketInfo;

                    //如果nfs权限未开启，则不进行权限校验
                    if (!NFS_ACL_START) {
                        boolean notPass = writeFlag && !CheckUtils.writePermissionCheck(bucketInfo);
                        return Mono.just(!notPass);
                    }

                    if (null != bucketInfo) {
                        FSIpACL ipACL = FSIPACLUtils.getIpACL(bucketInfo, FSIPACLUtils.getClientIp(reqHeader.nfsHandler));
                        if (ipACL == null) {
                            return Mono.just(false);
                        }
                        reqHeader.ipACL = ipACL;
                        if (isNotNfsIpAclSquash(reqHeader, callHeader)) {
                            nfsSquash(reqHeader, callHeader);
                        }
                    }

                    int[] uidAndGid = ACLUtils.getUidAndGid(callHeader);
                    Tuple2<FSIdentity, Boolean> tuple2 = getIdtAndJudgeByUidAndInode(inode, callHeader);
                    FSIdentity identity = tuple2.var1;
                    boolean isIdtPromote = tuple2.var2;

                    return findSecretAccessKeyByIamFS(bucket, inode.getObjName(), callHeader.opt, accessKey)
                            .flatMap(b -> {
                                if (!b) {
                                    log.error("uid: {}, gid: {}, dose not have iam permission of obj {}:{}", uidAndGid[0], uidAndGid[1], inode.getNodeId(), inode.getObjName());
                                    return Mono.just(false);
                                }

                                //判断桶ACL；当前账户是否包含该桶的权限
                                return checkBucketACL(bucketInfo, identity.getS3Id(), bucket)
                                        .flatMap(bucketACLRes -> {
                                            if (!bucketACLRes.var1) {
                                                log.error("uid: {}, gid: {}, dose not have bucket acl permission of obj {}:{}", uidAndGid[0], uidAndGid[1], inode.getNodeId(), inode.getObjName());
                                                return Mono.just(false);
                                            } else {
                                                //根据桶ACL返回的权限，判断当前操作是否通过
                                                boolean isDir = inode.getObjName().endsWith("/") || inode.getNodeId() == 1;
                                                boolean isBucketACLPass = isOptPass(bucketACLRes.var2, callHeader.opt, isDir, extraParam);
                                                if (!isBucketACLPass) {
                                                    log.error("uid: {}, gid: {}, dose not have bucket acl permission of obj {}:{}", uidAndGid[0], uidAndGid[1], inode.getNodeId(), inode.getObjName());
                                                    return Mono.just(false);
                                                }
                                            }

                                            //判断nfs挂载是公共读还是公共读写
                                            boolean mountPerm = CheckUtils.writePermissionCheck(bucketInfo);
                                            if (writeFlag && !mountPerm) {
                                                log.error("uid: {}, gid: {}, dose not have nfs permission of obj {}:{}", uidAndGid[0], uidAndGid[1], inode.getNodeId(), inode.getObjName());
                                                return Mono.just(false);
                                            }

                                            //判断对象ACL与UGO、NFS ACL与CIFS ACL
                                            return Flux.just(
                                                            parseObjACLToNFSRight(inode, identity.getS3Id(), callHeader.opt, bucketInfo, extraParam).var1(),
                                                            parseUGOAndNFSACLToRight(inode, createNfsHeader(callHeader.opt, identity, isIdtPromote, callHeader), extraParam).var1()
                                                            )
                                                    .collectList()
                                                    .map(list -> list.get(0) && list.get(1));
                                        });
                            });
                });
    }

    /**
     * 置为 true 则不再判断该操作的 cifs 权限，一般不判断的接口其nfs权限与cifs 权限存在差异，需要单独判断
     *
     * @param isS3Opt 是否是s3的操作
     **/
    public static boolean notNeedJudgeCifs(int opt, boolean isS3Opt) {
        if (!CIFS_ACL_START) {
            return true;
        }
        boolean res = false;
        switch (opt) {
            //setattr
            case 2:
                break;
            //write
            case 7:
                //create
            case 8:
                //mkdir
            case 9:
                //mknod
            case 11:
                //commit
            case 21:
                break;
            //lookup
            case 3:
            //readlink
            case 5:
                //read
            case 6:
                break;
            //symlink
            case 10:
                break;
            //remove
            case 12:
            //rmdir
            case 13:
                if (isS3Opt) {
                    //s3端删除单独判断cifs
                    res = true;
                }
                //删除操作需要检查父目录是否存在cifs的删除子文件和子文件夹的权限，若存在则无需再判断子文件或子文件夹本身的删除权限
                break;
            //rename，仅在nfs端会判断，cifs权限不判断源目录，仅判断目标目录是否拥有创建权限
            case 14:
                res = true;
                break;
            //link
            case 15:
                res = true;
                break;
            //readDir
            case 16:
                //readDirPlu
            case 17:
                break;
            default:
        }

        return res;
    }

    /**
     * 判断cifs acl 被删除的文件或者目录本身是否具有delete权限
     **/
    public static boolean judgeCifsRemove(Map<String, String> bucketInfo, Inode inode, RpcCallHeader callHeader) {
        if (null == bucketInfo || bucketInfo.isEmpty() || !NFSBucketInfo.isCIFSShare(bucketInfo)) {
            return true;
        }

        if (!NFS_ACL_START) {
            return true;
        }

        if (InodeUtils.isError(inode) || (inode.getNodeId() != 1 && null == inode.getObjName())) {
            return true;
        }

        FSIdentity identity = ACLUtils.getIdentityByUidAndInode(inode, callHeader);
        boolean pass = CIFSACL.judgeSMBACLToBoolean(inode, identity, callHeader.opt, true, null, 0);
        return pass;
    }

    /**
     * 判断cifs acl 源文件是否具有删除权限，目标的父目录是否具有创建文件或者创建目录的权限
     **/
    public static boolean judgeCifsRename(Map<String, String> bucketInfo, Inode sourceInode, Inode sourceDirInode, Inode targetDirInode, RpcCallHeader callHeader, boolean isDir) {
        if (null == bucketInfo || bucketInfo.isEmpty() || !NFSBucketInfo.isCIFSShare(bucketInfo)) {
            return true;
        }

        if (!NFS_ACL_START) {
            return true;
        }

        if (InodeUtils.isError(sourceInode) || InodeUtils.isError(targetDirInode) ||
                (sourceInode.getNodeId() != 1 && null == sourceInode.getObjName()) ||
                (targetDirInode.getNodeId() != 1 && null == targetDirInode.getObjName())
        ) {
            return true;
        }

        Map<String, String> sourceParam = new HashMap<>(2);
        //toDir:指代待检查权限inode是否是目标的父目录
        //srcIsDir: 指代待重命名的inode是否是目录
        sourceParam.put("toDir", "0");
        sourceParam.put("isSrcDir", isDir ? "1" : "0");

        Map<String, String> targetParam = new HashMap<>(2);
        targetParam.put("toDir", "1");
        targetParam.put("isSrcDir", isDir ? "1" : "0");

        //首先判断源的父目录是否具有删除子文件和子文件夹的权限，如果有的话则不再判断源是否具有删除权限
//        FSIdentity identity = ACLUtils.userInfo.get(reqAccountID);
        FSIdentity idtSrcDir = ACLUtils.getIdentityByUidAndInode(sourceDirInode, callHeader);
        long access = CIFSACL.parseSMBACLToRight(sourceDirInode, idtSrcDir, 12, false).var2;
        boolean sourcePass = true;
        boolean existDelSub = (access & DELETE_SUB_FILE_AND_DIR) != 0;
        if (existDelSub) {
            sourcePass = true;
        } else {
            FSIdentity idtSrc = ACLUtils.getIdentityByUidAndInode(sourceInode, callHeader);
            sourcePass =  CIFSACL.judgeSMBACLToBoolean(sourceInode, idtSrc, callHeader.opt, true, sourceParam, 0);
        }

        FSIdentity idtTar = ACLUtils.getIdentityByUidAndInode(targetDirInode, callHeader);
        boolean targetPass =  CIFSACL.judgeSMBACLToBoolean(targetDirInode, idtTar, callHeader.opt, true, targetParam, 0);
        return sourcePass & targetPass;
    }

    public static Mono<Boolean> judgeUGOAndACL(Inode inode, RpcCallHeader callHeader, Map<String, String> bucketInfo, Map<String, String> extraParam) {
        if (!NFS_ACL_START) {
            return Mono.just(true);
        }

        int[] uidAndGid = ACLUtils.getUidAndGid(callHeader);
        String reqAccountID = ACLUtils.getS3IdByUid(uidAndGid[0]);
        return Flux.just(parseObjACLToNFSRight(inode, reqAccountID, callHeader.opt, bucketInfo, extraParam), parseUGOAndNFSACLToRight(inode, callHeader, extraParam))
                .collectList()
                .map(list -> list.get(0).var1 && list.get(1).var1);
    }

    /**
     * 根据inode的mode判断 ugo权限
     **/
    public static int judgeUGO(int mode, int type, boolean isDir) {
        int right = 0;
        switch (type) {
            // user
            case 0:
                if ((mode & USER_READ) != 0) {
                    right |= READ;
                }

                if ((mode & USER_WRITE) != 0) {
                    right |= (MODIFY | EXTEND | DELETE);
                }

                if ((mode & USER_EXEC) != 0) {
                    if (isDir) {
                        right |= LOOK_UP;
                    } else {
                        right |= EXECUTE;
                    }
                }
                break;
            // group
            case 1:
                if ((mode & GROUP_READ) != 0) {
                    right |= READ;
                }

                if ((mode & GROUP_WRITE) != 0) {
                    right |= (MODIFY | EXTEND | DELETE);
                }

                if ((mode & GROUP_EXEC) != 0) {
                    if (isDir) {
                        right |= LOOK_UP;
                    } else {
                        right |= EXECUTE;
                    }
                }
                break;
            // other
            case 2:
                if ((mode & AccessReply.OTHER_READ) != 0) {
                    right |= READ;
                }

                if ((mode & AccessReply.OTHER_WRITE) != 0) {
                    right |= (MODIFY | EXTEND | DELETE);
                }

                if ((mode & AccessReply.OTHER_EXEC) != 0) {
                    if (isDir) {
                        right |= LOOK_UP;
                    } else {
                        right |= EXECUTE;
                    }
                }
                break;
            default:
        }

        return right;
    }


    /**
     * 判断额外的NFS ACL权限
     *
     * @return <是否有 NFS ACL权限，NFS ACL 权限>
     **/
    public static Tuple2<Boolean, Integer> judgePOSIXACL(Map<Integer, Map<Integer, Integer>> aclMap, int reqUid, int reqGid, int[] reqGids, int type, boolean isDir) {
        int right = 0;

        Tuple2<Boolean, Integer> res = new Tuple2<>(false, right);
        if (aclMap == null) {
            return res;
        }

        int mask = -1;
        if (aclMap.get(NFSACL_CLASS) != null) {
            mask = aclMap.get(NFSACL_CLASS).get(0);
            //mask为0则无效，此时判断ugo权限
            if (mask == 0) {
                return res;
            }
        }

        switch (type) {
            // 检查是否设置了文件所属者的额外权限，等同于ugo权限，理论上这个值不需要存
            case 0:
                if (aclMap.get(NFSACL_USER_OBJ) != null && aclMap.get(NFSACL_USER_OBJ).get(reqUid) != null) {
                    int permission = aclMap.get(NFSACL_USER_OBJ).get(reqUid);
                    right |= addPOSIXRight(permission, mask, isDir);
                    res.var1 = true;
                    res.var2 = right;
                    if (ACLUtils.aclDebug) {
                        log.info("case: {}, id: {}, permission: {}, right: {}", type, reqUid, permission, right);
                    }
                }

                return res;
            // 检查是否设置了文件所属组的额外权限，等同于ugo权限，理论上这个值不需要存
            case 1:
                if (aclMap.get(NFSACL_GROUP_OBJ) != null && aclMap.get(NFSACL_GROUP_OBJ).get(reqGid) != null) {
                    int permission = aclMap.get(NFSACL_GROUP_OBJ).get(reqGid);
                    right |= addPOSIXRight(permission, mask, isDir);
                    res.var1 = true;
                    res.var2 = right;
                    if (ACLUtils.aclDebug) {
                        log.info("case: {}, id: {}, permission: {}, right: {}", type, reqGid, permission, right);
                    }
                }

                for (int reqGid0 : reqGids) {
                    if (aclMap.get(NFSACL_GROUP_OBJ) != null && aclMap.get(NFSACL_GROUP_OBJ).get(reqGid0) != null) {
                        int permission = aclMap.get(NFSACL_GROUP_OBJ).get(reqGid0);
                        right |= addPOSIXRight(permission, mask, isDir);
                        res.var1 = true;
                        res.var2 = right;
                        if (ACLUtils.aclDebug) {
                            log.info("case: {}, id: {}, permission: {}, right: {}", type, reqGid0, permission, right);
                        }
                    }
                }
                return res;
            // 检查是否设置了其它用户的额外权限，等同于ugo权限，理论上这个值不需要存
            case 2:
                if (aclMap.get(NFSACL_OTHER) != null && aclMap.get(NFSACL_OTHER).get(reqUid) != null) {
                    int permission = aclMap.get(NFSACL_OTHER).get(reqUid);
                    right |= addPOSIXRight(permission, mask, isDir);
                    res.var1 = true;
                    res.var2 = right;
                    if (ACLUtils.aclDebug) {
                        log.info("case: {}, id: {}, permission: {}, right: {}", type, reqUid, permission, right);
                    }
                }

                return res;
            // 检查是否设置了特定用户的额外权限
            case 3:
                if (aclMap.get(NFSACL_USER) != null && aclMap.get(NFSACL_USER).get(reqUid) != null) {
                    int permission = aclMap.get(NFSACL_USER).get(reqUid);
                    right |= addPOSIXRight(permission, mask, isDir);
                    res.var1 = true;
                    res.var2 = right;
                    if (ACLUtils.aclDebug) {
                        log.info("case: {}, id: {}, permission: {}, right: {}", type, reqUid, permission, right);
                    }
                }

                return res;
            // 检查是否设置了特定用户组的额外权限
            case 4:
                if (aclMap.get(NFSACL_GROUP) != null && aclMap.get(NFSACL_GROUP).get(reqGid) != null) {
                    int permission = aclMap.get(NFSACL_GROUP).get(reqGid);
                    right |= addPOSIXRight(permission, mask, isDir);
                    res.var1 = true;
                    res.var2 = right;
                    if (ACLUtils.aclDebug) {
                        log.info("case: {}, id: {}, permission: {}, right: {}", type, reqGid, permission, right);
                    }
                }

                for (int reqGid0 : reqGids) {
                    if (aclMap.get(NFSACL_GROUP) != null && aclMap.get(NFSACL_GROUP).get(reqGid0) != null) {
                        int permission = aclMap.get(NFSACL_GROUP).get(reqGid0);
                        right |= addPOSIXRight(permission, mask, isDir);
                        res.var1 = true;
                        res.var2 = right;
                        if (ACLUtils.aclDebug) {
                            log.info("case: {}, id: {}, permission: {}, right: {}", type, reqGid0, permission, right);
                        }
                    }
                }

                return res;
            default:
                return res;
        }
    }

    /**
     * 将ACEs从字符串链表转换成Map集合
     * <type, <uid/gid, permission>>
     **/
    public static Map<Integer, Map<Integer, Integer>> parseACEsToMap(List<Inode.ACE> curAcl) {
        Map<Integer, Map<Integer, Integer>> aclMap = new HashMap<>();
        if (null != curAcl && curAcl.size() > 0) {
            for (Inode.ACE ace : curAcl) {
                int type = ace.getNType();
                int id = ace.getId();
                int permission = ace.getRight();
                if (aclMap.containsKey(type) && null != aclMap.get(type)) {
                    aclMap.get(type).put(id, permission);
                } else {
                    Map<Integer, Integer> idToPermission = new HashMap<>();
                    idToPermission.put(id, permission);
                    aclMap.put(type, idToPermission);
                }
            }
        }

        return aclMap;
    }

    /**
     * 移除nfs acl中与ugo权限相对应的ace，将ace用mode来替代
     **/
    public static List<Inode.ACE> removeUGOACE(List<Inode.ACE> ACEs, Inode inode) {
        if (null == ACEs || ACEs.isEmpty()) {
            return null;
        }

        try {
            List<Inode.ACE> curAcl = ACEs;

            int mode = inode.getMode();
            Iterator<Inode.ACE> iterator = curAcl.listIterator();

            while (iterator.hasNext()) {
                Inode.ACE ace = iterator.next();
                int type = ace.getNType();
                int permission = ace.getRight();
                if (Arrays.stream(UGO_ACE_ARR).anyMatch(e -> e == type)) {
                    if (permission != parseModeToInt(mode, type)) {
                        mode = refreshMode(mode, type);
                        mode |= parsePermissionToMode(permission, type);
                    }
                    iterator.remove();
                }
            }

            int oldMode = inode.getMode();
            // man acl手册中，继承权限需与创建时的mode相与
            inode.setMode(mode & oldMode);
            return curAcl;
        } catch (Exception e) {
            log.error("remove ugo ace error: {}, ", ACEs, e);
            return null;
        }
    }

    public static int parseModeToInt(int mode, int type) {
        int right = 0;

        switch (type) {
            case NFSACL_USER_OBJ:
                if ((mode & USER_READ) != 0) {
                    right |= 0b100;
                }

                if ((mode & USER_WRITE) != 0) {
                    right |= 0b010;
                }

                if ((mode & USER_EXEC) != 0) {
                    right |= 0b001;
                }
                break;
            case NFSACL_GROUP_OBJ:
                if ((mode & GROUP_READ) != 0) {
                    right |= 0b100;
                }

                if ((mode & GROUP_WRITE) != 0) {
                    right |= 0b010;
                }

                if ((mode & GROUP_EXEC) != 0) {
                    right |= 0b001;
                }
                break;
            case NFSACL_OTHER:
                if ((mode & OTHER_READ) != 0) {
                    right |= 0b100;
                }

                if ((mode & OTHER_WRITE) != 0) {
                    right |= 0b010;
                }

                if ((mode & OTHER_EXEC) != 0) {
                    right |= 0b001;
                }
                break;
            default:
                break;
        }

        return right;
    }


    /**
     * 解析POSIX ACL的 permission，将permission转成ugo权限
     **/
    public static int parsePermissionToMode(int permission, int type) {
        int right = 0;

        switch (type) {
            case NFSACL_USER_OBJ:
                if ((permission & 0b100) != 0) {
                    right |= USER_READ;
                }

                if ((permission & 0b010) != 0) {
                    right |= USER_WRITE;
                }

                if ((permission & 0b001) != 0) {
                    right |= USER_EXEC;
                }
                break;
            case NFSACL_GROUP_OBJ:
                if ((permission & 0b100) != 0) {
                    right |= GROUP_READ;
                }

                if ((permission & 0b010) != 0) {
                    right |= GROUP_WRITE;
                }

                if ((permission & 0b001) != 0) {
                    right |= GROUP_EXEC;
                }
                break;
            case NFSACL_OTHER:
                if ((permission & 0b100) != 0) {
                    right |= OTHER_READ;
                }

                if ((permission & 0b010) != 0) {
                    right |= OTHER_WRITE;
                }

                if ((permission & 0b001) != 0) {
                    right |= OTHER_EXEC;
                }
                break;
            default:
                break;
        }

        return right;
    }

    /**
     * 清理mode的各种权限位
     **/
    public static int refreshMode(int mode, int type) {
        int right = mode;

        switch (type) {
            case NFSACL_USER_OBJ:
                right &= (REFRESH_USER_READ & REFRESH_USER_WRITE & REFRESH_USER_EXEC);
                break;
            case NFSACL_GROUP_OBJ:
                right &= (REFRESH_GROUP_READ & REFRESH_GROUP_WRITE & REFRESH_GROUP_EXEC);
                break;
            case NFSACL_OTHER:
                right &= (REFRESH_OTHER_READ & REFRESH_OTHER_WRITE & REFRESH_OTHER_EXEC);
                break;
            default:
                break;
        }

        return right;
    }

    /**
     * 解析POSIX ACL的 ACEs，将每个ACEs的权限转换附加至NFS ACCESS请求所需的right中
     * 对于目录来说: r->列举条目 read；w-> modify | extend | delete；x-> lookup
     * 对于文件来说: r->读取数据 read；w -> modify | extent；x -> execute
     **/
    public static int addPOSIXRight(int permission, int mask, boolean isDir) {
        int right = 0;
        if (mask != -1) {
            permission = permission & mask;
        }

        // 读
        if ((permission & 0b100) != 0) {
            right |= READ;
            if (ACLUtils.aclDebug) {
                log.info("【POSIX】read: {}", right);
            }
        }

        // 写
        if ((permission & 0b010) != 0) {
            right |= (MODIFY | EXTEND | DELETE);
            if (ACLUtils.aclDebug) {
                log.info("【POSIX】write: {}", right);
            }
        }

        // 执行
        if ((permission & 0b001) != 0) {
            if (isDir) {
                right |= LOOK_UP;
            } else {
                right |= EXECUTE;
            }
            if (ACLUtils.aclDebug) {
                log.info("【POSIX】exec: {}", right);
            }
        }

        return right;
    }

    /**
     * 判断当前账户具备什么样的权限，以及是否需要继续往下判断其它类型的权限
     * <p>
     * 1、请求的账户是桶的所有账户 --> 拥有全部的权限，可以继续往下判断，返回的access可以全部选上；
     * 2、请求的账户不是桶的所有账户，但是桶是公共读写，返回公共读写的权限；
     * 3、请求的账户不是桶的所有账户，但是桶是公共读，查看桶是否具有其它权限，如果有则说明是账户权限，查看当前请求的账户，是否在对应的表7的对应权限的列表中
     * 4、请求的账户不是桶的所有账户，桶是私有读写，查看桶是否具有其它权限，如果有则说明
     *
     * @param bucketInfo   桶信息
     * @param reqAccountID 请求的s3账户
     * @param bucket       桶
     * @return <boolean, int> 是否继续往下判断其它权限 true继续，false不继续， 返回的权限信息
     **/
    public static Mono<Tuple2<Boolean, Integer>> checkBucketACL(Map<String, String> bucketInfo, String reqAccountID, String bucket) {
        if (null == bucketInfo || reqAccountID == null) {
            return Mono.just(new Tuple2<>(false, 0));
        }

        int acl = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
        String userId = bucketInfo.get(BUCKET_USER_ID);

        if (reqAccountID.equals(userId) || reqAccountID.equals("0")) {
            return Mono.just(new Tuple2<>(true, (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE)));
        } else {
            if ((acl & PERMISSION_SHARE_READ_WRITE_NUM) != 0) {
                // 桶是公共读写
                return Mono.just(new Tuple2<>(true, (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE)));
            } else {
                boolean accountPermission = false;
                int right = 0;

                // 桶是公共读，则需要查看当前的账户是否具备其它的权限
                if ((acl & PERMISSION_SHARE_READ_NUM) != 0) {
                    accountPermission = true;
                    right |= (READ | LOOK_UP | EXECUTE);
                }

                // 如果请求账户是被授予了特定权限的账户，则去redis表7中相应的桶权限 set 查找是否具备相关权限
                Mono<Tuple2<Boolean, Integer>> rightMono = Mono.just(new Tuple2<>(accountPermission, right));
                if ((acl & PERMISSION_FULL_CON_NUM) != 0) {
                    rightMono = rightMono.flatMap(t -> {
                        return ACLUtils.pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucket + "_" + PERMISSION_FULL_CON, reqAccountID)
                                .map(b -> {
                                    if (b) {
                                        t.var1 = true;
                                        t.var2 = (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE);
                                    }
                                    return t;
                                });
                    });
                }

                if ((acl & PERMISSION_WRITE_NUM) != 0) {
                    rightMono = rightMono.flatMap(t -> {
                        return ACLUtils.pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucket + "_" + PERMISSION_WRITE, reqAccountID)
                                .map(b -> {
                                    if (b) {
                                        t.var1 = true;
                                        t.var2 = t.var2 | (MODIFY | EXTEND | DELETE | EXECUTE | LOOK_UP);
                                    }
                                    return t;
                                });
                    });
                }

                if ((acl & PERMISSION_READ_NUM) != 0) {
                    rightMono = rightMono.flatMap(t -> {
                        return ACLUtils.pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucket + "_" + PERMISSION_READ, reqAccountID)
                                .map(b -> {
                                    if (b) {
                                        t.var1 = true;
                                        t.var2 = t.var2 | (READ | LOOK_UP | EXECUTE);
                                    }
                                    return t;
                                });
                    });
                }

                return rightMono;
            }
        }
    }


    /**
     * 将对象ACL权限转换至文件权限
     *
     * @return <当前操作是否通过权限，当前操作的权限>
     **/
    public static Tuple2<Boolean, Integer> parseObjACLToNFSRight(Inode inode, String reqAccountID, int opt, Map<String, String> bucketInfo, Map<String, String> extraParam) {
        String objACL = inode.getObjAcl();
        int right = 0;

        Tuple2<Boolean, Integer> res = new Tuple2<>(false, right);

        // 如果inode不存在，或者inode是目录，或者桶信息存在问题，则相当于没有设置对象ACL，返回最大权限
        if (InodeUtils.isError(inode) || inode.getNodeId() == 1 || inode.getObjName().endsWith("/")
                || null == bucketInfo || bucketInfo.isEmpty() || StringUtils.isBlank(bucketInfo.get(BUCKET_USER_ID))) {
            res.var1 = true;
            res.var2 = (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE);
            if (ACLUtils.aclDebug) {
                log.info("【objACL】 res: {}", res);
            }
            return res;
        }

        try {
            Map<String, String> objAclMap = new HashMap<>();
            boolean isOldInode = false;
            if (objACL == null || StringUtils.isBlank(inode.getObjAcl())) {
                // 如果不存在对象ACL，说明是旧数据，但正常情况下老数据可以在getInode时通过mergeObjAcl获得owner字段，因此当前为不正常情况，
                // 此时设置为公共读写
                objAclMap.put("owner", bucketInfo.get(BUCKET_USER_ID));
                objAclMap.put("acl", String.valueOf(OBJECT_PERMISSION_SHARE_READ_WRITE_NUM));
                isOldInode = true;
            } else {
                //如果存在对象ACL，则进行解析
                objAclMap = Json.decodeValue(objACL, new TypeReference<Map<String, String>>() {
                });
            }

            String ownerID = objAclMap.get("owner");
            int ownerACL = Integer.parseInt(objAclMap.get("acl"));

            boolean isOwner = reqAccountID.equals(ownerID);
            boolean isRoot = reqAccountID.equals(FsConstants.ADMIN_S3ID);
            boolean isProIdtOwner = false;
            //对象acl中旧版本数据按正常规则判断权限；因为cifs端原本有限制，只有桶拥有者可以挂载
            //cifs端没有配uid，reqs3Id就是原本的owner；配了还是原本的owner
            //nfs端没有配moss账户，新版本创的文件就是匿名，如果是匿名就要提升，nfs端创建的文件

            //【既不是拥有者也不是root访问，检查文件是否是同一个账户在未配置uid或者moss账户情况下创建的】
            if (!isOwner && !isRoot) {
                //cifs端访问nfs没有配置moss账户时创建的东西
                //root创建，uid=0，owner=桶拥有者   -----> 如果是桶拥有者对应的账户请求，允许访问
                //非root创建，uid=other，owner=匿名 -----> 如果是uid=other访问，允许访问
                //旧版本则为 uid=0, owner=桶拥有者   -----> 如果是桶拥有者对应的账户请求，允许访问
                //以上几种情况，cifs端s3Id配了uid，均要能够允许访问
                //其实相当于请求的身份和被访问的inode，uid或者s3Id有一个相等，就允许访问
                if (!ACLUtils.checkNewInode(inode) && userInfo.containsKey(reqAccountID) && userInfo.get(reqAccountID).getUid() > 0) {
                    int reqUid = userInfo.get(reqAccountID).getUid();
                    if (inode.getUid() == reqUid) {
                        isProIdtOwner = true;
                    }
                }
            }

            if (isOwner || isRoot || isProIdtOwner) {
                // 在对象ACL中，对象的所属账户具有对该对象的全部权限
                res.var1 = true;
                res.var2 = (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE);
                if (ACLUtils.aclDebug) {
                    log.info("【objACL】 res: {}", res);
                }
                return res;
            } else {
                // 如果是其它账户访问，则需要检查对象的ACL
                // 首先判断对象是否是公共读写，如果是公共读写则无需判断具体账户的权限
                if ((ownerACL & OBJECT_PERMISSION_SHARE_READ_WRITE_NUM) != 0) {
                    res.var1 = true;
                    res.var2 = (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE);
                    if (ACLUtils.aclDebug) {
                        log.info("【objACL】 res: {}", res);
                    }
                    return res;
                } else {
                    if ((ownerACL & OBJECT_PERMISSION_SHARE_READ_NUM) != 0) {
                        right |= (READ | LOOK_UP | EXECUTE);
                    }

                    // objAcl对其它账户的权限转换成nfs acl权限，存储于inode的ACEs中
                    objAclMap.remove("owner");
                    objAclMap.remove("acl");
                    objAclMap.remove("bucketName");
                    objAclMap.remove("keyName");

                    // 检查请求的账户是否具有其它权限，对象端即使在cloudBerry设置了write权限，则直接会设置full权限
                    for (String key : objAclMap.keySet()) {
                        String curOwner = key.split("-")[1];
                        if (curOwner.equals(reqAccountID)) {
                            String permission = objAclMap.get(key);
                            switch (permission) {
                                case OBJECT_PERMISSION_READ:
                                    right |= (READ | LOOK_UP | EXECUTE);
                                    break;
                                case OBJECT_PERMISSION_READ_CAP:
                                case OBJECT_PERMISSION_WRITE_CAP:
                                    break;
                                case OBJECT_PERMISSION_FULL_CON:
                                    right |= (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE);
                                    break;
                                default:
                                    break;
                            }
                        }
                    }

                    boolean isDir = inode.getObjName().endsWith("/");
                    res.var1 = isOptPass(right, opt, isDir, extraParam);
                    res.var2 = right;
                    if (ACLUtils.aclDebug) {
                        log.info("【objACL】 res: {}", res);
                    }
                    return res;
                }
            }
        } catch (Exception e) {
            log.error("parse s3 objACL to inode aces error", e);
            return res;
        }
    }


    /**
     * 根据文件的 UGO 权限和 NFS ACL 权限计算得到最终请求的用户对该文件的权限
     * UGO权限与 NFS ACL并非简单的互补关系；NFS ACL权限具备更高的优先级，即在对特定用户设置了 NFS ACL之后，优先判断 NFS ACL，若没有设置
     * NFS ACL，再判断UGO权限
     * <p>
     * 需要注意以下规则：
     * UGO权限：
     * (1) owner无权限，group有权限，则 group中用户访问时具有权限；
     * (2) 一个用户在获取 UGO 权限时，U、G、O三个权限为互补权限；
     * <p>
     * NFS ACL权限：
     * (1) 以用户->用户组->其它用户的层级顺序判断权限；
     * (2) 每个层级顺序中优先判断 NFS ACL权限，如果 NFS ACL没有设置权限，再判断 UGO 权限；如果当前层级包含有 NFS ACL权限，判断完之后【无需】
     * 再判断之后层级的权限；如果当前层级没有 NFS ACL权限，则判断 UGO 权限，并继续往之后的层级判断；
     * (3) 仅当当前层级中effective mask，也就是mask的值为---，才会继续往后判断
     * (4) 判断 NFS ACL权限时需要将 mask 与对应权限相与，最高权限不可超过 mask
     *
     * @param inode      判断权限的inode
     * @param callHeader 包含用户uid，gid以及调用操作opt的 header
     * @return <是否通过权限判断，权限>
     **/
    public static Tuple2<Boolean, Integer> parseUGOAndNFSACLToRight(Inode inode, RpcCallHeader callHeader, Map<String, String> extraParam) {
        int right = 0;
        Tuple2<Boolean, Integer> res = new Tuple2<>(false, right);

        //AUTH_UNIX
        if (inode.getNodeId() >= 1 && callHeader.auth.flavor == 1) {
            int uid = ((AuthUnix) (callHeader.auth)).getUid();
            int gid = ((AuthUnix) (callHeader.auth)).getGid();
            int[] gids = ((AuthUnix) (callHeader.auth)).getGids();
            List<Integer> gidList = Arrays.stream(gids)
                    .boxed()
                    .collect(Collectors.toList());

            if (uid == 0) {
                // root用户具备所有权限
                res.var1 = true;
                res.var2 = (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE);
                if (ACLUtils.aclDebug) {
                    log.info("【objACL】 res: {}", res);
                }
                return res;
            } else {
                //如果uid>0，但objACL中的owner为default_id，说明这个inode是moss不存在的uid创建的，此时创建的数据仍然应当被视为是uid创建的数据
                //开了权限后必须给moss账户配置对应的uid才能够在s3和cifs端继续访问
                int checkUid = inode.getUid();
                int checkGid = inode.getGid();
                if (StringUtils.isNotBlank(inode.getObjAcl())) {
                    Map<String, String> objAclMap = Json.decodeValue(inode.getObjAcl(), new TypeReference<Map<String, String>>() {
                    });
                    String ownerID = objAclMap.get("owner");
                    if (DEFAULT_USER_ID.equals(ownerID)) {
                        //仅s3操作，使用匿名账户访问时，若此时inode.getUid对应的账户已经配置完成，则不允许s3匿名账户再访问，若还没有配置完成，则
                        //允许s3的匿名账户访问
                        //检查当前inode.getUid是否已经配置完成有对应的moss账户，此处checkUid不可能等于0
                        boolean hasMatch = checkUid != 65534 && uidToS3ID.containsKey(checkUid);
                        if (!hasMatch && null != extraParam && ACLUtils.ProtoType.S3.name().equals(extraParam.get(PROTO))) {
                            checkUid = 65534;
                            checkGid = 65534;
                        }
                    }
                }

                boolean isDir = inode.getObjName().endsWith("/") || inode.getNodeId() == 1;

                Map<Integer, Map<Integer, Integer>> map = null;
                if (null != inode.getACEs() && !inode.getACEs().isEmpty()) {
                    if (null != inode.getACEs() && !inode.getACEs().isEmpty()) {
                        //如果是cifs ace，则仅把赋予特定用户或用户组的权限进行翻译；其余更细粒度的cifs权限在cifs acl中判断；cifs acl采用累加原则，而nfs acl判断有顺序，因此同时判断时，会把权限缩小
                        if (CIFSACL.isExistCifsACE(inode)) {
                            map = NFSACL.parseACEsToMap(CIFSACL.transferCifsAceToNfs(inode, isDir));
                        } else {
                            map = NFSACL.parseACEsToMap(inode.getACEs());
                        }
                    }
                }

                int UGOMode = inode.getMode() & 4095;
                // user
                Tuple2<Boolean, Integer> userNfsAcl = judgePOSIXACL(map, uid, gid, gids, 3, isDir);
                // 文件拥有者本身权限只看ugo中的user权限
                if (userNfsAcl.var1 && uid != checkUid) {
                    right |= userNfsAcl.var2;
                    if (ACLUtils.aclDebug) {
                        log.info("【access】speci user: {}", right);
                    }
                    res.var1 = isOptPass(right, callHeader.opt, isDir, extraParam);
                    res.var2 = right;
                    return res;
                } else {
                    if (uid == checkUid) {
                        right |= judgeUGO(UGOMode, 0, isDir);
                        res.var1 = isOptPass(right, callHeader.opt, isDir, extraParam);
                        res.var2 = right;
                        if (ACLUtils.aclDebug) {
                            log.info("【access】user: {}", right);
                        }
                        return res;
                    }
                }

                // group: 根据man acl手册，组权限先查看匹配的nfs acl权限，与原本的ugo组权限取并集，再与mask取交集；若无nfs acl，则看
                // ugo组权限
                Tuple2<Boolean, Integer> groupNfsAcl = judgePOSIXACL(map, uid, gid, gids, 4, isDir);
                if (groupNfsAcl.var1) {
                    right |= groupNfsAcl.var2;
                    if (ACLUtils.aclDebug) {
                        log.info("【access】speci group: {}", right);
                    }

                    if (gid == checkGid || gidList.contains(checkGid)) {
                        //此时一定存在mask权限，需要与mask权限取交集
                        int mask = judgeGroupWithMask(map, isDir);
                        right |= (judgeUGO(UGOMode, 1, isDir) & mask);
                        if (ACLUtils.aclDebug) {
                            log.info("【access】group: {}", right);
                        }
                    }

                    res.var1 = isOptPass(right, callHeader.opt, isDir, extraParam);
                    res.var2 = right;
                    return res;
                } else {
                    if (gid == checkGid || gidList.contains(checkGid)) {

                        int mask = judgeGroupWithMask(map, isDir);
                        right |= (judgeUGO(UGOMode, 1, isDir) & mask);
                        if (ACLUtils.aclDebug) {
                            log.info("【access】group: {}", right);
                        }
                    }
                }

                // other
                if (uid != checkUid && gid != checkGid && !gidList.contains(checkGid)) {
                    right |= judgeUGO(UGOMode, 2, isDir);
                    if (ACLUtils.aclDebug) {
                        log.info("【access】other: {}, uid: {}, mode: {}, UGOMode: {}, inode: {}", right, uid, inode.getMode(), UGOMode, inode);
                    }
                }

                res.var1 = isOptPass(right, callHeader.opt, isDir, extraParam);
                res.var2 = right;
                if (ACLUtils.aclDebug) {
                    log.info("【nfs】 res: {}", res);
                }
                return res;
            }
        } else {
            res.var1 = true;
            res.var2 = (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE);
            if (ACLUtils.aclDebug) {
                log.info("【nfs】 res: {}", res);
            }
            return res;
        }
    }

    public static int judgeGroupWithMask(Map<Integer, Map<Integer, Integer>> aclMap, boolean isDir) {
        int mask = -1;
        if (null != aclMap && aclMap.get(NFSACL_CLASS) != null) {
            mask = aclMap.get(NFSACL_CLASS).get(0);
        }

        //如果不存在mask，返回满权限
        if (mask < 0) {
            return isDir? (READ | MODIFY | EXTEND | DELETE | LOOK_UP) : (READ | MODIFY | EXTEND | DELETE | EXECUTE);
        }

        int right = 0;
        //如果存在mask，将mask转成具体的access权限
        if ((mask & 0b100) != 0) {
            right |= READ;
        }

        // 写
        if ((mask & 0b010) != 0) {
            right |= (MODIFY | EXTEND | DELETE);
        }

        // 执行
        if ((mask & 0b001) != 0) {
            if (isDir) {
                right |= LOOK_UP;
            } else {
                right |= EXECUTE;
            }
        }

        return right;
    }

    /**
     * 判断当前 nfs opt是否具有权限
     *
     * @param right      传入的 access 权限
     * @param opt        nfs操作
     * @param isDir      是否是目录
     * @param extraParam 额外的附加参数，可用于判断setattr的权限
     **/
    public static boolean isOptPass(int right, int opt, boolean isDir, Map<String, String> extraParam) {
        boolean pass = false;
        boolean read = false;
        boolean write = false;
        boolean execute = false;

        if (isDir) {
            if ((right & READ) != 0) {
                read = true;
                if (ACLUtils.aclDebug) {
                    log.info("right: {}, read: {}, isDir: {}", right, read, isDir);
                }
            }

            if ((right & MODIFY) != 0 && (right & EXTEND) != 0 && (right & DELETE) != 0) {
                write = true;
                if (ACLUtils.aclDebug) {
                    log.info("right: {}, write: {}, isDir: {}", right, write, isDir);
                }
            }

            if ((right & LOOK_UP) != 0) {
                execute = true;
                if (ACLUtils.aclDebug) {
                    log.info("right: {}, lookup: {}, isDir: {}", right, execute, isDir);
                }
            }
        } else {
            if ((right & READ) != 0) {
                read = true;
                if (ACLUtils.aclDebug) {
                    log.info("right: {}, read: {}, isDir: {}", right, read, isDir);
                }
            }

            if ((right & MODIFY) != 0 && (right & EXTEND) != 0 && (right & DELETE) != 0) {
                write = true;
                if (ACLUtils.aclDebug) {
                    log.info("right: {}, write: {}, isDir: {}", right, write, isDir);
                }
            }

            if ((right & EXECUTE) != 0) {
                execute = true;
                if (ACLUtils.aclDebug) {
                    log.info("right: {}, execute: {}, isDir: {}", right, execute, isDir);
                }
            }
        }

        try {
            switch (opt) {
                //setattr
                case 2:
                    if (null != extraParam && null != extraParam.get("objAttr")) {
                        ObjAttr attr = Json.decodeValue(extraParam.get("objAttr"), ObjAttr.class);
                        if (attr.hasSize != 0) {
                            pass = isDir ? (write && execute) : write;
                        } else {
                            pass = true;
                        }
                    }

                    break;
                //write
                case 7:
                    //create
                case 8:
                    //mkdir
                case 9:
                    //mknod
                case 11:
                    //commit
                case 21:
                    pass = isDir ? (write && execute) : write;
                    break;
                //lookup
                case 3:
                    pass = isDir ? execute : true;
                    break;
                //readlink
                case 5:
                    //read
                case 6:
                    pass = isDir ? execute : read;
                    break;
                //symlink
                case 10:
                    //remove
                case 12:
                    //rmdir
                case 13:
                    //rename
                case 14:
                    pass = isDir ? (write && execute) : true;
                    break;
                //link
                case 15:
                    pass = isDir ? (write && execute) : (read && write);
                    break;
                //readDir
                case 16:
                    //readDirPlu
                case 17:
                    pass = isDir ? (read && execute) : true;
                    break;
                default:
                    pass = true;
            }
        } catch (Exception e) {
            log.info("judge opt:{}, right: {}, isDir: {}, extraParam: {} error", opt, right, isDir, extraParam, e);
        }

        if (ACLUtils.aclDebug) {
            log.info("opt: {}, pass: {}, read: {}, write: {}, execute: {}", opt, pass, read, write, execute);
        }

        return pass;
    }


    static Mono<Boolean> findSecretAccessKeyByIamFS(String bucket, String object, int opt, String accessKey) {
        if (ACLUtils.aclDebug) {
            log.info("bucket: {}, obj: {}, opt: {}, accessKey: {}", bucket, object, opt, accessKey);
        }
        // 判断如果是账户而不是用户，则直接略过
        if (StringUtils.isBlank(accessKey)) {
            return Mono.just(true);
        }

        if (!NFSV3.NFSV3_ACTION_MAP.containsKey(opt)) {
            return Mono.just(false);
        }

        String action = NFSV3.NFSV3_ACTION_MAP.get(opt);
        return ACLUtils.iamPool.getReactive(SysConstants.REDIS_IAM_INDEX)
                .get(accessKey)
                .flatMap(accessKeyValue -> {
                    AccessKeyVO accessKeyVO = JsonUtils.toObject(AccessKeyVO.class, accessKeyValue.getBytes());
                    String userId = accessKeyVO.getUserId();

                    String resource = "mrn::moss:::";
                    if (StringUtils.isNotBlank(bucket)) {
                        resource += bucket;
                    }

                    if (StringUtils.isNotBlank(object)) {
                        resource += "/" + object;
                    }

                    return IamUtils.checkFsAuth(userId, action, resource)
                            .map(i -> {
                                if (i == 1 || i == 4) {
                                    return false;
                                } else if (i == 2) {
                                    return true;
                                } else {
                                    return false;
                                }
                            });
                });
    }

    /**
     * 将ace中的mask值映射至ugo的group权限中
     **/
    public static int mapACLMaskToMode(List<Inode.ACE> ACEs) {
        Tuple2<Integer, List<Inode.ACE>> tuple2 = getAndSetNFSMask(ACEs, 0, false);
        return parsePermissionToMode(tuple2.var1, NFSACL_GROUP_OBJ);
    }

    public static boolean isNotNfsIpAclSquash(ReqInfo reqHeader, RpcCallHeader callHeader) {
        if (callHeader.auth.flavor != 1) {
            return true;
        }
        AuthUnix authUnix = (AuthUnix) callHeader.auth;
        boolean notChange = true;
        if (reqHeader.ipACL != null) {
            String squash = reqHeader.ipACL.getSquash();
            int[] uidAndGid = ACLUtils.getUidAndGid(reqHeader.bucketInfo.getOrDefault("user_id", "0"));
            int anonuid = uidAndGid[0];
            int anongid = uidAndGid[1];
            if ("root_squash".equals(squash)) {
                if (authUnix.getUid() == 0) {
                    authUnix.setUid(anonuid);
                    authUnix.setGid(anongid);
                    authUnix.setGidN(1);
                    authUnix.setGids(new int[]{anongid});
                    notChange = false;
                }
            } else if ("all_squash".equals(squash)) {
                authUnix.setUid(anonuid);
                authUnix.setGid(anongid);
                authUnix.setGidN(1);
                authUnix.setGids(new int[]{anongid});
                notChange = false;
            }
        }
        return notChange;
    }

    /**
     * no_root_squash: root用户访问时，将被映射成root用户，具备root权限
     * root_squash: root用户访问时，将被映射成匿名用户 uid=65534，具备other 权限
     * all_squash: 所有用户访问时，将被映射成匿名用户，具备other 权限
     * auth_unix的uid和gid映射
     */
    public static void nfsSquash(ReqInfo reqHeader, RpcCallHeader callHeader) {
        if (callHeader.auth.flavor != 1) {
            return;
        }
        AuthUnix authUnix = (AuthUnix) callHeader.auth;
        String squash = "1";
        int anonuid = 65534;
        int anongid = 65534;
        try {
            //默认squash 种类默认为 root_squash，与s3权限对齐，不允许有超级用户的存在
            squash = reqHeader.bucketInfo.getOrDefault("squash", "1");
            anonuid = Integer.parseInt(reqHeader.bucketInfo.getOrDefault("anonuid", "65534"));
            anongid = Integer.parseInt(reqHeader.bucketInfo.getOrDefault("anongid", "65534"));
        } catch (Exception e) {
            log.error("NFS squash error", e);
        }
        // 0 no_root_squash, 1 root_squash, 2 all_squash
        if ("1".equals(squash)) {
            if (authUnix.getUid() == 0) {
                authUnix.setUid(anonuid);
                authUnix.setGid(anongid);
                authUnix.setGidN(1);
                authUnix.setGids(new int[]{anongid});
            }
        } else if ("2".equals(squash)) {
            authUnix.setUid(anonuid);
            authUnix.setGid(anongid);
            authUnix.setGidN(1);
            authUnix.setGids(new int[]{anongid});
        }
    }

    public static List<Inode.ACE> parseACLtoNfsACL(Inode inode) {
        List<Inode.ACE> resList = new LinkedList<>();

        try {
            boolean isDir = inode.getNodeId() == 1 || (null != inode.getObjName() && inode.getObjName().endsWith("/"));

            for (int i = 0; i < UGO_ACE_ARR.length; i++) {
                int id = 0;
                int type = UGO_ACE_ARR[i];
                if (type == NFSACL_USER_OBJ) {
                    id = inode.getUid();
                } else if (type == NFSACL_GROUP_OBJ) {
                    id = inode.getGid();
                }

                Inode.ACE ace = new Inode.ACE(type, NFSACL.parseModeToInt(inode.getMode(), type), id);
                resList.add(ace);
            }

            //获取inode对应的属主和属组
            String inoOwnerSID = FSIdentity.getUserSIDByUid(inode.getUid());
            String inoGroupSID = FSIdentity.getGroupSIDByGid(inode.getGid());

            if (null != inode.getACEs() && !inode.getACEs().isEmpty()) {
                List<Inode.ACE> curAcl = inode.getACEs();
                boolean existCifsAce = false;
                boolean existCifsInherit = false;
                for (Inode.ACE ace : curAcl) {
                    //ace只有可能存在一种，判断属于nfs还是cifs
                    if (CIFSACL.isCifsACE(ace)) {
                        existCifsAce = true;
                        byte cType = ace.getCType();
                        short inheritFlag = ace.getFlag();
                        String sid = ace.getSid();
                        long access = ace.getMask();
                        switch (cType) {
                            case ACCESS_ALLOWED_ACE_TYPE:
                                if (inode.getObjName().endsWith("/") && (inheritFlag & OBJECT_INHERIT_ACE) != 0 || (inheritFlag & CONTAINER_INHERIT_ACE) != 0) {
                                    //存在继承权限
                                    existCifsInherit = true;
                                    if (inoOwnerSID.equals(sid)) {
                                        Inode.ACE nfsAce = new Inode.ACE(DEFAULT_NFSACL_USER_OBJ, CIFSACL.cifsAccessToUgo(access, isDir), FSIdentity.getUidBySID(sid));
                                        resList.add(nfsAce);
                                    } else if (inoGroupSID.equals(sid)) {
                                        Inode.ACE nfsAce = new Inode.ACE(DEFAULT_NFSACL_GROUP_OBJ, CIFSACL.cifsAccessToUgo(access, isDir), FSIdentity.getGidBySID(sid));
                                        resList.add(nfsAce);
                                    } else if (SID.EVERYONE.getDisplayName().equals(sid)) {
                                        Inode.ACE nfsAce = new Inode.ACE(DEFAULT_NFSACL_OTHER, CIFSACL.cifsAccessToUgo(access, isDir), 0);
                                        resList.add(nfsAce);
                                    } else if (sid.startsWith(FSIdentity.USER_SID_PREFIX)) {
                                        Inode.ACE nfsAce = new Inode.ACE(DEFAULT_NFSACL_USER, CIFSACL.cifsAccessToUgo(access, isDir), FSIdentity.getUidBySID(sid));
                                        resList.add(nfsAce);
                                    } else if (sid.startsWith(FSIdentity.GROUP_SID_PREFIX)) {
                                        Inode.ACE nfsAce = new Inode.ACE(DEFAULT_NFSACL_GROUP, CIFSACL.cifsAccessToUgo(access, isDir), FSIdentity.getGidBySID(sid));
                                        resList.add(nfsAce);
                                    }
                                } else {
                                    //不存在继承权限
                                    if (inoOwnerSID.equals(sid) || inoGroupSID.equals(sid) || SID.EVERYONE.getDisplayName().equals(sid)) {
                                        break;
                                    } else {
                                        if (sid.startsWith(FSIdentity.USER_SID_PREFIX)) {
                                            Inode.ACE nfsAce = new Inode.ACE(NFSACL_USER, CIFSACL.cifsAccessToUgo(access, isDir), FSIdentity.getUidBySID(sid));
                                            resList.add(nfsAce);
                                        } else if (sid.startsWith(FSIdentity.GROUP_SID_PREFIX)) {
                                            Inode.ACE nfsAce = new Inode.ACE(NFSACL_GROUP, CIFSACL.cifsAccessToUgo(access, isDir), FSIdentity.getGidBySID(sid));
                                            resList.add(nfsAce);
                                        }
                                    }
                                }
                                break;
                            case ACCESS_DENIED_ACE_TYPE:
                                break;
                            case ACCESS_ALLOWED_OBJECT_ACE_TYPE:
                            case ACCESS_DENIED_OBJECT_ACE_TYPE:
                            case SYSTEM_AUDIT_ACE_TYPE:
                            case SYSTEM_ALARM_ACE_TYPE:
                            case ACCESS_ALLOWED_COMPOUND_ACE_TYPE:
                            case SYSTEM_AUDIT_OBJECT_ACE_TYPE:
                            case SYSTEM_ALARM_OBJECT_ACE_TYPE:
                            case ACCESS_ALLOWED_CALLBACK_ACE_TYPE:
                            case ACCESS_DENIED_CALLBACK_ACE_TYPE:
                            case ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE:
                            case ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE:
                            case SYSTEM_AUDIT_CALLBACK_ACE_TYPE:
                            case SYSTEM_ALARM_CALLBACK_ACE_TYPE:
                            case SYSTEM_AUDIT_CALLBACK_OBJECT_ACE_TYPE:
                            case SYSTEM_ALARM_CALLBACK_OBJECT_ACE_TYPE:
                            case SYSTEM_MANDATORY_LABEL_ACE_TYPE:
                            case SYSTEM_RESOURCE_ATTRIBUTE_ACE_TYPE:
                            case SYSTEM_SCOPED_POLICY_ID_ACE_TYPE:
                            default:
                                log.info("SMB2ACE: class: {} is not implemented, obj: {}", cType, inode.getObjName());
                        }
                    } else {
                        int ntype = ace.getNType();
                        int id = ace.getId();
                        int permission = ace.getRight();

                        Inode.ACE nfsAce = new Inode.ACE(ntype, permission, id);
                        GetAclReply.AclEntry entry = new GetAclReply.AclEntry(ntype, id, permission);
                        if (ntype > 4096) {
                            resList.add(nfsAce);
                        } else if (ntype != NFSACL_USER_OBJ && ntype != NFSACL_GROUP_OBJ && ntype != NFSACL_OTHER) {
                            resList.add(nfsAce);
                        }
                    }
                }

                if (existCifsAce) {
                    if (existCifsInherit) {
                        Inode.ACE nfsAce = new Inode.ACE(DEFAULT_NFSACL_CLASS, 7, 0);
                        resList.add(nfsAce);
                    }

                    Inode.ACE nfsAce = new Inode.ACE(NFSACL_CLASS, 7, 0);
                    resList.add(nfsAce);
                }
            }
        } catch (Exception e) {
            log.error("parse acl to nfs acl error", e);
        }

        return resList;
    }

}
