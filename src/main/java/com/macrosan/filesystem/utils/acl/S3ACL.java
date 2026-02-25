package com.macrosan.filesystem.utils.acl;

import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.Utils;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.nfs.RpcCallHeader;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.FSIdentity;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.macrosan.filesystem.FsConstants.ACLConstants.PROTO;
import static com.macrosan.filesystem.FsConstants.SMB2ACCESS_MASK.DELETE_SUB_FILE_AND_DIR;
import static com.macrosan.filesystem.utils.acl.ACLUtils.*;

/**
 * @Author: WANG CHENXING
 * @Date: 2025/8/25
 * @Description:
 */

@Log4j2
public class S3ACL {
    /**
     * 用于检查可追加对象本身是否有文件的写权限
     **/
    public static Mono<Boolean> checkMetaAppendACL(MetaData meta, String bucket, String reqID, int opt, int startFs) {
        if (meta.inode > 0) {
            if (!ACLUtils.NFS_ACL_START && !CIFS_ACL_START) {
                return Mono.just(true);
            }

            if (null == meta.tmpInodeStr) {
                log.error("check append acl fail, inode is null: {}:{}, reqID: {}, bucket: {}", meta.getInode(), meta.getKey(), reqID, bucket);
                throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
            }

            try {
                Inode inode = Json.decodeValue(meta.tmpInodeStr, Inode.class);
                if (InodeUtils.isError(inode) || inode.getNodeId() <= 0) {
                    throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
                }

                if (judgeInodeFSACL(reqID, inode, opt, startFs, 0)) {
                    return Mono.just(true);
                } else {
                    throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
                }

            } catch (Exception e) {
                if (!(e instanceof MsException)) {
                    log.error("check append acl fail, inode is error: {}:{}, reqID: {}, bucket: {}", meta.getInode(), meta.getKey(), reqID, bucket, e);
                }
                throw e;
            }


        } else {
            return Mono.just(true);
        }
    }

    /**
     * 用于检查可追加对象本身是否有文件的写权限
     **/
    public static Mono<Boolean> checkInodeAppendACL(Inode inode, String bucket, String reqID, int opt, int startFs) {
        if (!ACLUtils.NFS_ACL_START && !CIFS_ACL_START) {
            return Mono.just(true);
        }

        try {
            if (InodeUtils.isError(inode) || inode.getNodeId() <= 0) {
                throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
            }

            if (judgeInodeFSACL(reqID, inode, opt, startFs, 0)) {
                return Mono.just(true);
            } else {
                throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
            }

        } catch (Exception e) {
            if (!(e instanceof MsException)) {
                log.error("check append acl fail, inode is error: {}:{}, reqID: {}, bucket: {}", inode.getNodeId(), inode.getObjName(), reqID, bucket, e);
            }
            throw e;
        }
    }

    /**
     * 写操作创建目录的元数据不创建inode，opt: 7-put/copy, opt: 12-deleteObj
     * 读操作允许创建inode，opt: 3-headObj, 6-getObj, 16-listObj
     **/
    public static Mono<Boolean> checkFSACL(String objName, String bucketName, String reqID, Inode[] dirInodes, int opt, boolean checkSelf, int startFs, int cifsJudgeFlag) {
        boolean notCreate = false;
        if (StringUtils.isBlank(objName) || (StringUtils.isNotBlank(objName) && objName.endsWith("/"))) {
            if (opt == 7 || opt == 12) {
                notCreate = true;
            }
        }

        if (checkSelf) {
            return checkInodeACL(objName, bucketName, reqID, opt, startFs)
                    .map(b -> {
                        if (!b) {
                            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
                        }

                        return true;
                    });
        }

        return checkDirACLRec(objName, bucketName, reqID, dirInodes, notCreate, opt, startFs, cifsJudgeFlag)
                .map(tuple2 -> {
                    Set<Boolean> set = (Set<Boolean>) tuple2.var1;
                    if (set.contains(false)) {
                        throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
                    }

                    return true;
                });
    }

    public static Mono<Boolean> checkInodeACL(String obj, String bucket, String reqID, int opt, int startFs) {
        Mono<Inode> findInode = null;
        if (StringUtils.isBlank(obj)) {
            findInode = Node.getInstance().getInode(bucket, 1L);
        } else {
            findInode = FsUtils.lookup(bucket, obj, null, false, -1, null);
        }

        return findInode
                .map(inode -> {
                    if (!InodeUtils.isError(inode) && inode.getNodeId() != 0) {
                        return judgeInodeFSACL(reqID, inode, opt, startFs, 0);
                    }

                    return true;
                });
    }

    /**
     * 递归向上查找最近的父目录inode权限，如果不存在父目录则创建
     * 如果put为目录，则不创建inode，仅查找最近的父目录inode权限
     * 对于 dir1/dir2/dir3，dir1设置了权限，而dir3设置了允许权限，dir1设置不允许权限，dir3仍然能够上传对象问题，不做处理，因为原生nfs端同样存在
     * 同一个账户可以上传，其它账户上传时则会被默认的目录权限拦住，但如果是读操作，默认权限会允许
     **/
    public static Mono<Tuple2<Set<Boolean>, Inode>> checkDirACLRec(String objName, String bucketName, String reqID, Inode[] dirInodes, boolean notCreate, int opt, int startFs, int cifsJudgeFlag) {
        String[] dirName = new String[]{""};
        String[] dirPrefix = objName.split("/");
        if (dirPrefix.length > 1) {
            if (objName.endsWith("/")) {
                dirName[0] = objName.substring(0, objName.length() - dirPrefix[dirPrefix.length - 1].length() - 2);
            } else {
                dirName[0] = objName.substring(0, objName.length() - dirPrefix[dirPrefix.length - 1].length() - 1);
            }
        }

        Set<Boolean> set = new HashSet<>();

        if (StringUtils.isEmpty(dirName[0])) {
            return Node.getInstance().getInode(bucketName, 1L)
                    .map(root -> {
                        if (null != dirInodes) {
                            dirInodes[0] = root;
                        }
                        set.add(judgeInodeFSACL(reqID, root, opt, startFs, cifsJudgeFlag));
                        return new Tuple2<>(set, root);
                    });
        } else {
            return FsUtils.lookup(bucketName, dirName[0], null, false, -1, null)
                    .flatMap(inode -> {
                        // 父目录存在inode，则判断父目录的权限
                        if (!InodeUtils.isError(inode) && inode.getNodeId() != 0) {
                            if (null != dirInodes) {
                                dirInodes[0] = inode;
                            }
                            set.add(judgeInodeFSACL(reqID, inode, opt, startFs, cifsJudgeFlag));
                            if (ACLUtils.aclDebug) {
                                log.info("check rec: obj: {}, inode: {}, set: {}", dirName[0], inode, set);
                            }
                            return Mono.just(new Tuple2<>(set, inode));
                        }

                        // 如果是上传的目录，则不递归创建inode；如果是上传的对象，则会依次创建父目录inode
                        // 父目录不存在inode，而且不存在meta，继续递归往上查找；/a/b/c -> /a/b -> /a
                        if (Utils.DEFAULT_META_HASH.equals(inode.getReference())) {
                            return checkDirACLRec(inode.getObjName(), bucketName, reqID, dirInodes, notCreate, opt, startFs, cifsJudgeFlag)
                                    .flatMap(tuple2 -> {
                                        Inode newDirInode = (Inode) tuple2.var2;
                                        set.addAll((Set<Boolean>) tuple2.var1);

                                        //上传目录只判断最近的已存在的父目录inode
                                        if (notCreate) {
                                            set.add(true);
                                            return Mono.just(new Tuple2<>(set, newDirInode));
                                        } else {
                                            return Node.getInstance().createS3Inode(newDirInode.getNodeId(), bucketName, inode.getObjName(), inode.getVersionId(), inode.getReference(), newDirInode.getACEs())
                                                    .map(i -> {
                                                        set.add(judgeInodeFSACL(reqID, i, opt, startFs, cifsJudgeFlag));
                                                        return new Tuple2<>(set, i);
                                                    });
                                        }
                                    });
                        }

                        //一开始metaHash不存在, linkN是 not found，此时应该继续往上找，且只找而不创建，从而能够让第一个obj能够put
                        if (inode.getLinkN() == Inode.NOT_FOUND_INODE.getLinkN()) {
                            return checkDirACLRec(dirName[0], bucketName, reqID, dirInodes, notCreate, opt, startFs, cifsJudgeFlag);
                        }

                        set.add(true);
                        // 未预期的错误
                        return Mono.just(new Tuple2<>(set, Inode.ERROR_INODE));
                    });
        }
    }

    /**
     * 判断metaData本身的文件权限
     **/
    public static void judgeMetaFSACL(String reqID, MetaData meta, int opt, int startFs) {
        try {
            if (meta.inode > 0 && null != meta.tmpInodeStr) {
                Inode inode = Json.decodeValue(meta.tmpInodeStr, Inode.class);
                if (!judgeInodeFSACL(reqID, inode, opt, startFs, 0)) {
                    throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
                }
            }
        } catch (Exception e) {
            if (e instanceof MsException) {
                throw e;
            } else {
                log.error("judge Meta FS ACL error, key: {}, reqID: {}, opt: {}", meta.getKey(), reqID, opt, e);
            }
        }
    }

    /**
     * s3对应接口的操作对应为nfs端的操作
     * putObj/copy - write - opt 7
     * getObj - read - opt 6
     * delObj - remove - opt 12
     * headObj - lookup - opt 3
     * listObj - readDir - opt 16
     *
     * @param reqID 请求的s3账户Id
     * @param inode 检查的inode
     * @param opt   请求的操作
     * @param startFS 当前已开启的文件协议种类
     *                0b001 --- nfs
     *                0b010 --- cifs
     *                0b100 --- ftp
     **/
    public static boolean judgeInodeFSACL(String reqID, Inode inode, int opt, int startFS, int cifsJudgeFlag) {
        if (!ACLUtils.NFS_ACL_START && !CIFS_ACL_START) {
            return true;
        }

        FSIdentity identity = ACLUtils.getIdentityByS3IDAndInode(inode, reqID);
        RpcCallHeader reqHeader = ACLUtils.createCallHeader(opt, identity);

        Map<String, String> extraParam = new HashMap<>();
        extraParam.put(PROTO, ACLUtils.ProtoType.S3.name());

        boolean nfsPass = true;
        boolean cifsPass = true;
        if (ACLUtils.NFS_ACL_START && checkNFSv3Start(startFS)) {
            Tuple2<Boolean, Integer> tuple2 = NFSACL.parseUGOAndNFSACLToRight(inode, reqHeader, extraParam);
            nfsPass = tuple2.var1;
        }

        if (CIFS_ACL_START && checkCifsStart(startFS) && !NFSACL.notNeedJudgeCifs(opt, true)) {
            cifsPass = CIFSACL.judgeSMBACLToBoolean(inode, identity, opt, true, null, cifsJudgeFlag);
        }

        return nfsPass & cifsPass;
    }


    public static void judgeDeleteCifsACL(String reqID, MetaData meta, int opt, int startFs, Inode dirInode) {
        try {
            if (meta.inode > 0 && null != meta.tmpInodeStr) {
                Inode inode = Json.decodeValue(meta.tmpInodeStr, Inode.class);

                if (!CIFS_ACL_START) {
                    return;
                }

                boolean cifsPass = true;

                if (checkCifsStart(startFs)) {
                    boolean dirContainDelSub = false;
                    if (null != dirInode && !InodeUtils.isError(dirInode)) {
                        FSIdentity identity = ACLUtils.getIdentityByS3IDAndInode(dirInode, reqID);
                        long dirAccess = CIFSACL.parseSMBACLToRight(dirInode, identity, opt, false).var2;
                        if ((dirAccess & DELETE_SUB_FILE_AND_DIR) != 0) {
                            dirContainDelSub = true;
                        }
                    }

                    if (dirContainDelSub) {
                        cifsPass = true;
                    } else {
                        FSIdentity identity = ACLUtils.getIdentityByS3IDAndInode(inode, reqID);
                        cifsPass = CIFSACL.judgeSMBACLToBoolean(inode, identity, opt, true, null, 0);
                    }
                }

                if (!cifsPass) {
                    throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
                }
            }
        } catch (Exception e) {
            if (e instanceof MsException) {
                throw e;
            } else {
                log.error("judge Meta FS ACL error, key: {}, reqID: {}, opt: {}", meta.getKey(), reqID, opt, e);
            }
        }
    }
}
