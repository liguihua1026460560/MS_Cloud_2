package com.macrosan.filesystem.utils.acl;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.SID;
import com.macrosan.filesystem.cifs.types.smb2.SMB2ACE;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.RpcCallHeader;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.FSIdentity;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.ACLConstants.CONTAIN_ACL;
import static com.macrosan.filesystem.FsConstants.ACLConstants.NFS_DIR_SUB_DEL_ACL;
import static com.macrosan.filesystem.FsConstants.NFSACLType.*;
import static com.macrosan.filesystem.FsConstants.NFSACLType.NFSACL_OTHER;
import static com.macrosan.filesystem.FsConstants.NFSAccessAcl.*;
import static com.macrosan.filesystem.FsConstants.NFSAccessAcl.EXECUTE;
import static com.macrosan.filesystem.FsConstants.NTStatus.STATUS_ACCESS_DENIED;
import static com.macrosan.filesystem.FsConstants.SMB2ACCESS_MASK.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACCESS_MASK.DELETE;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEType.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEType.SYSTEM_SCOPED_POLICY_ID_ACE_TYPE;
import static com.macrosan.filesystem.FsConstants.S_IFLNK;
import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.filesystem.cifs.call.smb2.CreateCall.*;
import static com.macrosan.filesystem.cifs.call.smb2.CreateCall.FILE_OVERWRITE_IF;
import static com.macrosan.filesystem.nfs.reply.AccessReply.*;
import static com.macrosan.filesystem.utils.acl.ACLUtils.*;
import static com.macrosan.filesystem.utils.acl.ACLUtils.aclDebug;
import static com.macrosan.message.jsonmsg.FSIdentity.GROUP_SID_PREFIX;

/**
 * @Author: WANG CHENXING
 * @Date: 2025/8/25
 * @Description:
 */

@Log4j2
public class CIFSACL {
    public static boolean cifsACL = false;

    /**
     * cifs端mount时检查桶acl权限
     **/
    public static Mono<Boolean> checkCIFSBucketMount(Map<String, String> bucketInfo, Session session, String bucket) {
        int acl = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
        if ((session.flags & 0b1) == 0b1) { // 匿名
            // 0 (默认)该桶不允许匿名访问, 1 guest ok 该桶允许匿名访问, 2 guest only 该桶仅允许匿名访问(类似 NFS all_squash)
            if (("1".equals(bucketInfo.get("guest")) || "2".equals(bucketInfo.get("guest"))) && ((acl & PERMISSION_SHARE_READ_WRITE_NUM) != 0 || (acl & PERMISSION_SHARE_READ_NUM) != 0)) {
                return Mono.just(true);
            } else {
                return Mono.just(false);
            }
        } else { // 非匿名
            String[] findId = {null};
            return pool.getReactive(REDIS_USERINFO_INDEX).exists(session.account)
                    .flatMap(exist0 -> {
                        if (exist0 > 0) {
                            return pool.getReactive(REDIS_USERINFO_INDEX).hget(session.account, USER_DATABASE_NAME_ID)
                                    .flatMap(reqAccountID -> {
                                        findId[0] = reqAccountID;
                                        String userId = bucketInfo.get(BUCKET_USER_ID);
                                        if (reqAccountID.equals(userId) || (acl & PERMISSION_SHARE_READ_WRITE_NUM) != 0 || (acl & PERMISSION_SHARE_READ_NUM) != 0) {
                                            return Mono.just(true);
                                        } else {
                                            // 如果请求账户是被授予了特定权限的账户，则去redis表7中相应的桶权限 set 查找是否具备相关权限
                                            Mono<Boolean> rightMono = Mono.just(false);
                                            if ((acl & PERMISSION_FULL_CON_NUM) != 0) {
                                                rightMono = rightMono.flatMap(t -> {
                                                    return pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucket + "_" + PERMISSION_FULL_CON, reqAccountID)
                                                            .map(b -> {
                                                                if (b) {
                                                                    if (cifsACL) {
                                                                        log.info("【cifs mount bucket acl1】 t: {}, b: {}", t, b);
                                                                    }
                                                                    return true;
                                                                }
                                                                return t;
                                                            });
                                                });
                                            }
                                            if ((acl & PERMISSION_WRITE_NUM) != 0) {
                                                rightMono = rightMono.flatMap(t -> {
                                                    return pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucket + "_" + PERMISSION_WRITE, reqAccountID)
                                                            .map(b -> {
                                                                if (b) {
                                                                    if (cifsACL) {
                                                                        log.info("【cifs mount bucket acl2】 t: {}, b: {}", t, b);
                                                                    }
                                                                    return true;
                                                                }
                                                                return t;
                                                            });
                                                });
                                            }
                                            if ((acl & PERMISSION_READ_NUM) != 0) {
                                                rightMono = rightMono.flatMap(t -> {
                                                    return pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucket + "_" + PERMISSION_READ, reqAccountID)
                                                            .map(b -> {
                                                                if (b) {
                                                                    if (cifsACL) {
                                                                        log.info("【cifs mount bucket acl3】 t: {}, b: {}", t, b);
                                                                    }
                                                                    return true;
                                                                }
                                                                return t;
                                                            });
                                                });
                                            }
                                            return rightMono
                                                    .doOnNext(t -> {
                                                        if (cifsACL) {
                                                            log.info("【cifs mount bucket acl】 t: {}", t);
                                                        }
                                                    });
                                        }
                                    });
                        }
                        return Mono.just(false);
                    })
                    .map(bucketAclRes -> {
                        if (bucketAclRes) {
                            //如果桶ACL权限通过，则校验当前cifs权限开关是否已经开启，若已经开启，则请求挂载的非匿名账户必须有配置的uid，若没有则不允许挂载
                            if (CIFS_ACL_START) {
                                if (StringUtils.isNotBlank(findId[0])) {
                                    FSIdentity identity = userInfo.get(findId[0]);
                                    if (null == identity || identity.getUid() == 0) {
                                        log.error("The account: {}:{} does not match any file system ID", session.account, findId[0]);
                                        return false;
                                    }
                                }
                            }
                        }

                        return bucketAclRes;
                    });
        }
    }

    /**
     * cifs端检查桶acl权限
     *
     * @param bucket 桶
     * @param bucketInfo 桶信息
     * @param reqAccountID 请求的账户名
     * @param access 请求的权限
     * @param isExist 用于判断当前create请求访问的inode是否存在，存在则打开，不存在则创建
     * @return <是否通过桶acl判断，桶acl检查后所具备的权限>
     **/
    public static Mono<Tuple2<Boolean, Long>> checkCIFSBucketACL(Map<String, String> bucketInfo, String reqAccountID, String bucket, long access, boolean isExist, int createDisPosition) {
        Tuple2<Boolean, Long> res = new Tuple2<>(false, 0L);

        if (null == bucketInfo || reqAccountID == null) {
            return Mono.just(res);
        }

        int acl = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
        String userId = bucketInfo.get(BUCKET_USER_ID);

        if (reqAccountID.equals(userId) || reqAccountID.equals("0")) {
            return Mono.just(new Tuple2<>(true, SET_FILE_ALL_RIGHTS));
        } else {
            if ((acl & PERMISSION_SHARE_READ_WRITE_NUM) != 0) {
                // 桶是公共读写
                return Mono.just(new Tuple2<>(true, SET_FILE_ALL_RIGHTS));
            } else {
                long right = 0;

                // 桶是公共读，仅允许读权限；写权限需要查看账户权限
                if ((acl & PERMISSION_SHARE_READ_NUM) != 0) {
                    right |= (FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS
                            | FsConstants.SMB2ACCESS_MASK.SET_FILE_EXEC_RIGHTS);
                }

                // 如果请求账户是被授予了特定权限的账户，则去redis表7中相应的桶权限 set 查找是否具备相关权限
                Mono<Long> rightMono = Mono.just(right);
                if ((acl & PERMISSION_FULL_CON_NUM) != 0) {
                    rightMono = rightMono.flatMap(t -> {
                        return pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucket + "_" + PERMISSION_FULL_CON, reqAccountID)
                                .map(b -> {
                                    if (b) {
                                        if (cifsACL) {
                                            log.info("【cifs bucket acl1】 t: {}", t);
                                        }
                                        return t | FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS
                                                | FsConstants.SMB2ACCESS_MASK.SET_FILE_WRITE_RIGHTS
                                                | FsConstants.SMB2ACCESS_MASK.SET_FILE_EXEC_RIGHTS;
                                    }
                                    return t;
                                });
                    });
                }

                if ((acl & PERMISSION_WRITE_NUM) != 0) {
                    rightMono = rightMono.flatMap(t -> {
                        return pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucket + "_" + PERMISSION_WRITE, reqAccountID)
                                .map(b -> {
                                    if (b) {
                                        if (cifsACL) {
                                            log.info("【cifs bucket acl2】 t: {}, after: {}", t, t | FsConstants.SMB2ACCESS_MASK.SET_FILE_WRITE_RIGHTS);
                                        }
                                        return t | FsConstants.SMB2ACCESS_MASK.SET_FILE_WRITE_RIGHTS;
                                    }
                                    return t;
                                });
                    });
                }

                if ((acl & PERMISSION_READ_NUM) != 0) {
                    rightMono = rightMono.flatMap(t -> {
                        return pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucket + "_" + PERMISSION_READ, reqAccountID)
                                .map(b -> {
                                    if (b) {
                                        if (cifsACL) {
                                            log.info("【cifs bucket acl3】 t: {}, after: {}", t, t | FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS);
                                        }
                                        return t | FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS
                                                | FsConstants.SMB2ACCESS_MASK.SET_FILE_EXEC_RIGHTS;
                                    }
                                    return t;
                                });
                    });
                }

                return rightMono
                        .map(t -> {
                            if (CIFSACL.cifsACL) {
                                log.info("【cifs-check bucket acl】 req: {}, permission: {}, res: {}, isExist: {}, createDisposition: {}", access, t, (t & access) < access, isExist, createDisPosition);
                            }

                            if (CIFS_ACL_START) {
                                //cifs权限开启后，仍然按照原本的判断逻辑判断权限
                                if ((t & access) < access) {
                                    return new Tuple2<>(false, t);
                                } else {
                                    // t & access = access;
                                    return new Tuple2<>(true, t);
                                }
                            } else {
                                //cifs权限关闭后，需要限制创建权限
                                if (isExist) {
                                    //打开文件或目录，则直接比较请求的权限即可
                                    if ((t & access) < access) {
                                        return new Tuple2<>(false, t);
                                    } else {
                                        // t & access = access;
                                        return new Tuple2<>(true, t);
                                    }
                                } else {
                                    //创建文件或目录，需要比较是否有创建权限
                                    boolean pass = false;
                                    switch (createDisPosition) {
                                        case FILE_OPEN:
                                        case FILE_OVERWRITE:
                                            break;
                                        case FILE_CREATE:
                                        case FILE_OPEN_IF:
                                        case FILE_OVERWRITE_IF:
                                            boolean mkDir = (t & MKDIR_OR_APPEND_DATA) != 0;
                                            boolean mkFile = (t & CREATE_OR_WRITE_DATA) != 0;
                                            pass = mkDir && mkFile;
                                            break;
                                        default:
                                            break;
                                    }

                                    return new Tuple2<>(pass, t);
                                }
                            }
                        });
            }
        }
    }

    /**
     * 类比nfs中的access实现，判断cifs create请求中的desiredAccess字段，观察所请求的inode是否具有所有的权限
     * @param reqAccess create请求中的desiredAccess值，即请求的权限，类似于nfs access请求中的access
     * @param inode 当前需判断权限的inode，可能为父目录，也可能为目录本身
     * @param extraInode 需要额外判断inode，一般为当前访问inode的父目录inode
     * @param reqAccountId 当前请求的s3账户，非id
     * @param bucket 桶名
     * @param createDisPosition 用于判断当前create的操作，是open、create等
     * @param createOption 用于判断当前create请求访问的inode是否是目录
     * @param isExist 用于判断当前create请求访问的inode是否存在，存在则打开，不存在则创建
     * @param header 用于获取messageId，以便于问题定位
     **/
    public static <T> Mono<Boolean> judgeCIFSAccess(long reqAccess, Inode inode, Inode extraInode, String reqAccountId, String bucket, int createDisPosition, int createOption, boolean isExist, SMB2Header header, Session session) {
        long[] access = {parseGenericAccess(reqAccess)};
        return NFSBucketInfo.getBucketInfoReactive(bucket)
                .flatMap(bucketInfo -> {
                    if ((session.flags & 0b1) == 0b1 && !"1".equals(bucketInfo.get("guest")) && !"2".equals(bucketInfo.get("guest"))) {
                        return Mono.just(false);
                    }

                    //判断桶ACL；当前账户是否包含该桶的权限
                    return checkCIFSBucketACL(bucketInfo, reqAccountId, bucket, access[0], isExist, createDisPosition)
                            .flatMap(bucketACLRes -> {
                                if (!bucketACLRes.var1) {
                                    log.error("【cifs】account: {}, dose not have bucket acl permission of obj: {}", reqAccountId, inode.getObjName());
                                    return Mono.just(false);
                                }

                                //如果cifs权限未开启，则不进行权限校验
                                if (!CIFS_ACL_START) {
                                    long right = FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS | FsConstants.SMB2ACCESS_MASK.SET_FILE_EXEC_RIGHTS;
                                    boolean isCreate = false;
                                    if (!isExist) {
                                        switch (createDisPosition) {
                                            case FILE_OPEN:
                                            case FILE_OVERWRITE:
                                                break;
                                            case FILE_CREATE:
                                            case FILE_OPEN_IF:
                                            case FILE_OVERWRITE_IF:
                                                isCreate = true;
                                                break;
                                            default:
                                                break;
                                        }
                                    }
                                    boolean notPass = (((right & access[0]) < access[0]) || isCreate) && !CheckUtils.cifsWritePermissionCheck(bucketInfo);
                                    return Mono.just(!notPass);
                                }

                                AtomicBoolean nfsStart = new AtomicBoolean(false);
                                nfsStart.set(NFSBucketInfo.isNFSShare(bucketInfo) && NFS_ACL_START);

                                Tuple2<Boolean, Long> cifsMountPerm = isCIFSWritePermitted(bucketInfo, access[0]);
                                if (!cifsMountPerm.var1) {
                                    log.error("【cifs】account: {}, dose not have cifs mount permission of obj: {}", reqAccountId, inode.getObjName());
                                    return Mono.just(false);
                                }

                                if (cifsACL) {
                                    log.info("【cifs】inode: {}, dirInode: {}, messageId: {}, position: {}", inode, extraInode, header.getMessageId(), createDisPosition);
                                }

                                // 删除请求是否需要判断父目录的权限
                                // 1) nfs开启时需判断nfs的父目录权限
                                // 2) nfs未开启时，若父目录不具备cifs的"删除子文件和子文件夹"权限，则cifs删除需要看本身是否具备删除权限；
                                // 若父目录具备"删除子文件和子文件夹"权限，则删除请求不再看本身是否具备删除权限
                                boolean[] needJudgeParent = {isExist && ((access[0] & DELETE) != 0)};
                                boolean[] needRemoveDelete = {false};
                                Mono<Boolean> passMono = null;
                                if (needJudgeParent[0]) {
                                    passMono = judgeDirDelete(extraInode, reqAccountId, 12, header, nfsStart.get(), bucketInfo, needRemoveDelete);
                                } else {
                                    passMono = Mono.just(true);
                                }

                                //判断对象ACL与UGO、NFS ACL、CIFS ACL
                                return passMono
                                        .flatMap(dirPass -> {
                                            if (!dirPass) {
                                                return Mono.just(false);
                                            }

                                            return Mono.just((inode.getMode() & S_IFMT) == S_IFLNK)
                                                    .flatMap(isSymLink -> {
                                                        if (isSymLink) {
                                                            return FsUtils.lookup(bucket, inode.getReference(), null, false, -1, null);
                                                        }

                                                        return Mono.just(inode);
                                                    })
                                                    .flatMap(checkInode -> {
                                                        FSIdentity identity = ACLUtils.getIdentityByS3IDAndInode(checkInode, reqAccountId);

                                                        return parseMultiAccess(checkInode, reqAccountId, identity, 4, header, true, nfsStart.get(), bucketInfo, access)
                                                                .map(right -> {
                                                                    //区分目录和文件，创建目录时，access一般为read access；
                                                                    //创建文件时，access会携带写权限，因此如果父目录不具备写权限，则无法创建；
                                                                    //cifs删除时，不看父目录的删除子文件夹与子文件的权限，只看要删除文件本身是否有删除权限
                                                                    //因此，与nfs保持一致，r、w、x翻译时均具备delete权限
                                                                    //但是，如果父目录无写权限，在nfs端是不允许删除的，解决办法是可以在nfs端处理，增加是否开启cifs
                                                                    //的判断，如果开启，则进行cifs的逻辑判断，如果未开启，则只判断nfs部分的权限，一经开启不再允许关闭
                                                                    long mixRight = bucketACLRes.var2 & cifsMountPerm.var2 & right;
                                                                    boolean res = true;
                                                                    if (isExist) {
                                                                        //inode已经存在，打开文件或目录则直接比较请求的权限
                                                                        //参考win原生，读取属性与其它属性同时被请求时，需忽略
                                                                        long filterAccess = filterAccess(access[0], inode, identity, needRemoveDelete);
                                                                        if ((mixRight & filterAccess) < filterAccess) {
                                                                            res = false;
                                                                        }
                                                                        if (cifsACL) {
                                                                            log.info("【cifs】req: {}, right: {}, access: {}, {}, position: {}, option:{}, obj: {}, exist: {}, res: {}, ino: {}, messgeId: {}", reqAccountId, mixRight, access, mixRight & access[0], createDisPosition, createOption, checkInode.getObjName(), isExist, res, checkInode.getNodeId(), header.getMessageId());
                                                                        }
                                                                        return res;
                                                                    } else {
                                                                        //inode不存在，若需要创建inode，则仅根据是否具有创建权限来判断
                                                                        //用于判断要创建的是目录还是文件
                                                                        boolean isDir = (createOption & FILE_DIRECTORY_FILE) != 0;
                                                                        switch (createDisPosition) {
                                                                            case FILE_OPEN:
                                                                                break;
                                                                            case FILE_CREATE:
                                                                            case FILE_OPEN_IF:
                                                                            case FILE_OVERWRITE:
                                                                            case FILE_OVERWRITE_IF:
                                                                                if (isDir) {
                                                                                    //创建目录，当前除了根据请求的access判断，还要根据是否具备MKDIR_OR_APPEND_DATA判断
                                                                                    res = (mixRight & MKDIR_OR_APPEND_DATA) != 0;
                                                                                } else {
                                                                                    res = (mixRight & CREATE_OR_WRITE_DATA) != 0;
                                                                                }
                                                                                break;
                                                                            default:
                                                                                break;
                                                                        }

                                                                        if (cifsACL) {
                                                                            log.info("【cifs】req: {}, right: {}, access: {}, {}, position: {}, option:{}, isDir: {}, exist: {}, res: {}, ino: {}, messgeId: {}", reqAccountId, mixRight, access, mixRight & access[0], createDisPosition, createOption, isDir, isExist, res, checkInode.getNodeId(), header.getMessageId());
                                                                        }
                                                                        return res;
                                                                    }
                                                                });
                                                    });
                                        });
                            });
                });
    }

    /**
     * 过滤请求的access，若access中包含读取属性：
     * 1) 仅包含读取属性，则不修改access
     * 2) 包含读取属性时包含其它权限位，则将access中请求的读取属性剔除
     * 3) 文件拥有者访问本身创建的文件时，直接具备读取权限，因此剔除access请求中的read_control
     *
     * @param access 请求的权限位
     * @return 返回的权限值
     **/
    public static long filterAccess(long access, Inode inode, FSIdentity identity, boolean[] needRemoveDelete) {
        boolean isOwnCurIno = false;
        long resAccess = access;

        //parseGenericAccess中已经剔除不参与权限判断的权限位
        boolean existReadAttr = (resAccess & READ_ATTR) != 0;
        //包含read attr
        if (existReadAttr) {
            resAccess &= CLEAR_READ_ATTR_ACCESS;
        }

        if (inode.getUid() == 0) {
            isOwnCurIno = identity.getUid() == 0;
        } else {
            //nfs中可能存在uid>0，但是为匿名用户的情况，此时应将ino的所属转为匿名
            //请求者也是经过检查的identity，因此可以直接进行比较
            int inoUid = inode.getUid();
            isOwnCurIno = identity.getUid() == inoUid;
        }

        if (isOwnCurIno) {
            resAccess = resAccess & CLEAR_READ_CONTROL_ACCESS & CLEAR_WRITE_DACL_ACCESS & CLEAR_WRITE_OWNER_ACCESS;
        }

        if (needRemoveDelete[0]) {
            resAccess &= CLEAR_DELETE_ACCESS;
        }

        //不包含
        return resAccess;
    }

    /**
     * win端发送create请求，一般不携带generic access
     * linux端发送create请求，写操作会携带generic access
     **/
    public static long parseGenericAccess(long reqAccess) {
        long access = reqAccess & CLEAR_GENERIC_ACCESS
                & CLEAR_SYNCHRONIZE_ACCESS
                & CLEAR_MAXIMUM_ALLOWED_ACCESS
                & CLEAR_ACCESS_SYSTEM_SECURITY_ACCESS;

        long genericAccess = reqAccess - access;
        if ((genericAccess & GENERIC_READ) != 0) {
            access |= SET_GENERIC_READ;
        }

        if ((genericAccess & GENERIC_WRITE) != 0) {
            access |= SET_GENERIC_WRITE;
        }

        if ((genericAccess & GENERIC_EXECUTE) != 0) {
            access |= SET_GENERIC_EXEC;
        }

        if ((genericAccess & GENERIC_ALL) != 0) {
            access |= SET_GENERIC_ALL;
        }

        return access;
    }

    /**
     * 若cifs只设置读取权限、更改权限、获得所有权或删除权限，则无对应的nfs ugo权限，此时无需判断nfs权限，而应该直接判断cifs权限
     **/
    public static boolean needJudgeNfs(long access) {
        long reqAccess = access;
        reqAccess &= (CLEAR_READ_CONTROL_ACCESS & CLEAR_WRITE_DACL_ACCESS & CLEAR_WRITE_OWNER_ACCESS & CLEAR_READ_ATTR_ACCESS
                & CLEAR_SYNCHRONIZE_ACCESS & CLEAR_MAXIMUM_ALLOWED_ACCESS & CLEAR_ACCESS_SYSTEM_SECURITY_ACCESS);

        if (reqAccess > 0) {
            return true;
        }

        return false;
    }

    /**
     * 分别判断 对象acl、nfs acl和 cifs acl，删除时不判断对象acl权限，因为对象acl本身就不限制 s3删除
     *
     * @param nfsStart 当前桶是否开启了nfs，若未开启则不判断nfs权限
     * @param reqAccess 用于储存cifs端 create时请求的权限，如果包含的权限为“读取权限”、“更改权限”、“获得所有权”、“删除”的组合，不包含其余权限
     *                  则此时不再额外判断nfs 权限，因为理论上这些权限不受rwx控制
     **/
    public static <T> Mono<Long> parseMultiAccess(Inode inode, String reqAccountId, FSIdentity identity, int opt, SMB2Header header, boolean isParent, boolean nfsStart, Map<String, String> bucketInfo, long[] reqAccess) {
        return Flux.just(
                new Tuple2<>(((reqAccess[0] & DELETE) != 0) ? new Tuple2<>(true, ALL_RIGHT) : NFSACL.parseObjACLToNFSRight(inode, reqAccountId, opt, bucketInfo,null), true),
                new Tuple2<>(nfsStart && needJudgeNfs(reqAccess[0])? NFSACL.parseUGOAndNFSACLToRight(inode, createCallHeader(opt, identity), null) : new Tuple2<>(true, ALL_RIGHT), true),
                new Tuple2<>(parseSMBACLToRight(inode, identity, opt, nfsStart), false))
                .collectList()
                .map(list -> {
                    long right = SET_FILE_ALL_RIGHTS;

                    for (int i = 0; i < list.size(); i++) {
                        Tuple2<T, Boolean> cur = (Tuple2<T, Boolean>) list.get(i);
                        if (cur.var2) {
                            //s3权限与nfs权限转成cifs权限
                            Tuple2<Boolean, Integer> t = (Tuple2<Boolean, Integer>) cur.var1;
                            right &= parseNFSRightToCIFSRight(inode, t.var2, isParent);
                            if (cifsACL) {
                                log.info("【cifs】cifs acl1: req: {}, {}, curNFSRight: {}, curCifsRight: {}, right: {}, ino: {}, messgeId: {}", reqAccountId, cur.var2, t.var2, parseNFSRightToCIFSRight(inode, t.var2, isParent), right, inode.getNodeId(), header.getMessageId());
                            }
                        } else {
                            //smb acl
                            Tuple2<Boolean, Long> t = (Tuple2<Boolean, Long>) cur.var1;
                            right &= t.var2;
                            if (cifsACL) {
                                log.info("【cifs】cifs acl2: req: {}, {}, curCifsRight: {}, right: {}, ino: {}, messgeId: {}", reqAccountId, cur.var2, t.var2, right, inode.getNodeId(), header.getMessageId());
                            }
                        }
                    }

                    return right;
                });
    }

    /**
     * 检查nfs的父目录是否具备删除权限，以及是否具备cifs的删除子文件和子文件夹的权限
     **/
    public static <T> Mono<Boolean> judgeDirDelete(Inode dirInode, String reqAccountId, int opt, SMB2Header header, boolean nfsStart, Map<String, String> bucketInfo, boolean[] needRemoveDelete) {
        FSIdentity identity = ACLUtils.getIdentityByS3IDAndInode(dirInode, reqAccountId);

        return Flux.just(
                new Tuple2<>(NFSACL.parseObjACLToNFSRight(dirInode, reqAccountId, opt, bucketInfo,null), 1),
                new Tuple2<>(nfsStart? NFSACL.parseUGOAndNFSACLToRight(dirInode, createCallHeader(opt, identity), null) : new Tuple2<>(true, ALL_RIGHT), 2),
                new Tuple2<>(parseSMBACLToRight(dirInode, identity, opt, nfsStart), 3))
                .collectList()
                .map(list -> {
                    boolean pass = true;
                    for (int i = 0; i < list.size(); i++) {
                        Tuple2<T, Integer> cur = (Tuple2<T, Integer>) list.get(i);
                        if (cur.var2 == 3) {
                            Tuple2<Boolean, Long> t = (Tuple2<Boolean, Long>) cur.var1;
                            //对cifs父目录的删除子文件和子文件夹权限判断结果
                            long curAccess = t.var2;
                            boolean canRemoveSub = (curAccess & DELETE_SUB_FILE_AND_DIR) != 0;

                            //含有该权限
                            if (canRemoveSub) {
                                needRemoveDelete[0] = true;
                            }
                        } else {
                            //对象acl和nfs的权限判断结果
                            pass &= list.get(i).var1.var1;
                        }
                    }

                    if (cifsACL) {
                        log.info("【nDel】cifs acl: req: {}, ino: {}, messgeId: {}, t1: {}, t2: {}", reqAccountId, dirInode.getNodeId(), header.getMessageId(), list.get(0), list.get(1));
                    }

                    return pass;
                });
    }

    /**
     * 在setInfo中的重命名中检查源是否具备删除权限
     *
     * @param inode 源的inode
     **/
    public static boolean judgeCifsSrcDelete(Map<String, String> bucketInfo, Inode inode, String reqS3Id) {
        if (null == bucketInfo || bucketInfo.isEmpty()) {
            return true;
        }

        if (!CIFS_ACL_START || !NFSBucketInfo.isCIFSShare(bucketInfo)) {
            return true;
        }

        if (InodeUtils.isError(inode) || (inode.getNodeId() != 1 && null == inode.getObjName())) {
            return true;
        }

        FSIdentity identity = ACLUtils.getIdentityByS3IDAndInode(inode, reqS3Id);

        boolean pass =  CIFSACL.judgeSMBACLToBoolean(inode, identity, 12, true, null, 0);
        if (cifsACL) {
            log.info("【cifs-rename】srcObj: {}, ino: {}, right: {}", inode.getObjName(), inode.getNodeId(), pass);
        }

        return pass;
    }

    /**
     * 在setInfo中的重命名中检查目标的父目录是否具备创建权限
     *
     * @param isOverWrite 重命名是否发生覆盖，若是覆盖则需要有被覆盖文件或者目录的删除权限
     * @param overInode 将要被覆盖的 inode
     **/
    public static Mono<Boolean> judgeCifsTarDirCreate(String bucket, Map<String, String> bucketInfo, String targetObj, SMB2Header header, boolean isOverWrite, Inode overInode, String reqS3Id) {
        if (null == bucketInfo || bucketInfo.isEmpty()) {
            return Mono.just(true);
        }

        if (!CIFS_ACL_START || !NFSBucketInfo.isCIFSShare(bucketInfo)) {
            return Mono.just(true);
        }

        return InodeUtils.findDirInode(targetObj, bucket)
                .flatMap(tarDirInode -> {
                    if (InodeUtils.isError(tarDirInode)) {
                        log.info("dirInode not found: target: {}", tarDirInode);
                        return Mono.just(false);
                    }

                    Mono<Boolean> passMono = null;
                    //若发生覆盖写，则检查目标父目录是否具有删除子文件或者子文件的权限；若父目录具备则直接通过，若不具备则看被覆盖文件是否具有被删除权限
                    boolean[] needRemoveDelete = {false};
                    if (isOverWrite) {
                        passMono = CIFSACL.judgeDirDelete(tarDirInode, reqS3Id, 12, header, NFSBucketInfo.isNFSShare(bucketInfo) && NFS_ACL_START, bucketInfo, needRemoveDelete);
                    } else {
                        passMono = Mono.just(true);
                    }

                    return passMono
                            .flatMap(dirPass -> {
                                if (!dirPass) {
                                    return Mono.just(false);
                                } else {
                                    //检查覆盖写权限，父目录未设置删除子文件或子文件夹的权限，此时检查被覆盖文件是否具有删除本身的权限
                                    if (isOverWrite && !needRemoveDelete[0] && !judgeCifsSrcDelete(bucketInfo, overInode, reqS3Id)) {
                                        return Mono.just(false);
                                    }
                                }

                                //请求检查目标父目录是否具备创建子文件或者子文件夹的权限
                                long[] reqAccess = {MKDIR_OR_APPEND_DATA | CREATE_OR_WRITE_DATA};
                                FSIdentity tarDirCheckId = ACLUtils.getIdentityByS3IDAndInode(tarDirInode, reqS3Id);
                                return parseMultiAccess(tarDirInode, tarDirCheckId.getS3Id(), tarDirCheckId, 4, header, true, NFSBucketInfo.isNFSShare(bucketInfo), bucketInfo, reqAccess)
                                        .map(right -> {
                                            boolean res = true;
                                            boolean createDir = (StringUtils.isNotBlank(targetObj) && targetObj.endsWith("/")) ||
                                                    StringUtils.isBlank(targetObj);

                                            if (createDir) {
                                                //目标是创建一个目录，则看父目录是否具备创建目录的权限
                                                res = (right & MKDIR_OR_APPEND_DATA) != 0;
                                            } else {
                                                res = (right & CREATE_OR_WRITE_DATA) != 0;
                                            }

                                            if (cifsACL) {
                                                log.info("【cifs-rename】target: {}, isDir: {}, right: {}", targetObj, createDir, right);
                                            }

                                            return res;
                                        });
                            });

                });

    }

    /**
     * 判断cifs共享权限
     * @param bucketInfo 桶信息
     * @param access 请求的权限
     * @return <请求是否通过，权限值>
     **/
    public static Tuple2<Boolean, Long> isCIFSWritePermitted(Map<String, String> bucketInfo, long access) {
        //判断cifs挂载是公共读还是公共读写
        boolean mountPerm = CheckUtils.cifsWritePermissionCheck(bucketInfo);
        long right = 0;
        if (mountPerm) {
            right = FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS | FsConstants.SMB2ACCESS_MASK.SET_FILE_WRITE_RIGHTS
                    | FsConstants.SMB2ACCESS_MASK.SET_FILE_EXEC_RIGHTS;
        } else {
            right = FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS | FsConstants.SMB2ACCESS_MASK.SET_FILE_EXEC_RIGHTS;
        }

        if (cifsACL) {
            log.info("【cifs mountPerm】 right: {}, final: {}, reqAccess: {}", right, right & access, access);
        }
        if ((right & access) < access) {
            return new Tuple2<>(false, 0L);
        } else {
            // t & access = access;
            return new Tuple2<>(true, right);
        }
    }

    /**
     * putObj/copy - write - opt 7
     * getObj - read - opt 6
     * delObj - remove - opt 12
     * headObj - lookup - opt 3
     * listObj - readDir - opt 16
     *
     * @param isOpenCifs 是否要判断cifs权限，若桶未开cifs，则无需判断cifs权限
     **/
    public static boolean judgeSMBACLToBoolean(Inode inode, FSIdentity identity, int opt, boolean isOpenCifs, Map<String, String> extraParam, int judgeFlag) {
        if (!isOpenCifs || !CIFS_ACL_START) {
            return true;
        }

        boolean res = true;
        boolean isDir = inode.getNodeId() == 1 || (StringUtils.isNotEmpty(inode.getObjName()) && inode.getObjName().endsWith("/"));
        long access = parseSMBACLToRight(inode, identity, opt, false).var2;

        switch (opt) {
            //setattr
            case 2:
                if (null != extraParam && null != extraParam.get("objAttr")) {
                    ObjAttr attr = Json.decodeValue(extraParam.get("objAttr"), ObjAttr.class);
                    if (attr.hasSize != 0 && !isDir) {
                        res = (access & CREATE_OR_WRITE_DATA) != 0
                                && (access & MKDIR_OR_APPEND_DATA) != 0
                                && (access & WRITE_ATTR) != 0
                                && (access & WRITE_X_ATTR) != 0;
                    } else {
                        res = true;
                    }
                } else {
                    res = true;
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
                if (isDir) {
                    //当前判断的为目录权限，目录是否有创建文件的权限或者创建子目录的权限
                    if ((judgeFlag & FsConstants.CIFSJudge.IS_DIR) != 0) {
                        res = (access & MKDIR_OR_APPEND_DATA) != 0;
                    } else if ((judgeFlag & FsConstants.CIFSJudge.IS_FILE) != 0) {
                        res = (access & CREATE_OR_WRITE_DATA) != 0;
                    }
                } else {
                    //当前判断的为文件权限
                    res = (access & CREATE_OR_WRITE_DATA) != 0
                        && (access & MKDIR_OR_APPEND_DATA) != 0
                        && (access & WRITE_ATTR) != 0
                        && (access & WRITE_X_ATTR) != 0;
                }
                break;
            //lookup
            case 3:
                //cifs中测试没有LIST_DIR_OR_EXEC_FILE权限仍然能够进入目录操作
                res = true;
                break;
            //readlink
            case 5:
                //read
            case 6:
                if (isDir) {
                    res = (access & LOOKUP_DIR_OR_READ_DATA) != 0;
                } else {
                    res = (access & LOOKUP_DIR_OR_READ_DATA) != 0
                            && (access & READ_ATTR) != 0
                            && (access & READ_X_ATTR) != 0
                            && (access & READ_CONTROL) != 0;
                }
                break;
            //symlink
            case 10:
                res = true;
                break;
            //remove
            case 12:
                //rmdir
            case 13:
                if (null == extraParam || extraParam.isEmpty()) {
                    //无特殊指定的情况下，仅判断当前inode是否具有删除本身的权限
                    res = (access & DELETE) != 0;
                } else if (null != extraParam && extraParam.containsKey(NFS_DIR_SUB_DEL_ACL)) {
                    //nfs端判断目录是否具备删除子文件和子文件夹的权限时
                    if ((access & DELETE_SUB_FILE_AND_DIR) != 0) {
                        extraParam.put(NFS_DIR_SUB_DEL_ACL, CONTAIN_ACL);
                    }
                    //此时无论目录是否具备删除子文件和子文件夹的权限，父目录的权限判断均为通过
                    res = true;
                }

                break;
            //rename，仅在nfs端会判断，cifs权限不判断源目录，仅判断目标目录是否拥有创建权限
            case 14:
                if (null == extraParam || extraParam.isEmpty()) {
                    res = true;
                } else {
                    boolean isToDir = "1".equals(extraParam.get("toDir"));
                    if (isToDir) {
                        boolean isSrcDir = "1".equals(extraParam.get("isSrcDir"));
                        if (isSrcDir) {
                            res = (access & MKDIR_OR_APPEND_DATA) != 0;
                        } else {
                            res = (access & CREATE_OR_WRITE_DATA) != 0;
                        }
                    } else {
                        res = (access & DELETE) != 0;
                    }
                }
                break;
            //link
            case 15:
                res = true;
                break;
            //readDir
            case 16:
                //readDirPlu
            case 17:
                res = isDir ? (access & LOOKUP_DIR_OR_READ_DATA) != 0 : true;
                break;
            default:
                res = true;
        }

        if (CIFSACL.cifsACL) {
            log.info("judge cifs acl: opt: {}, identity: {}, res: {}", opt, identity, res);
        }

        return res;
    }

    /**
     * 在nfs access接口中调用，主要用于判断cifs的高级权限，如读权限，nfs对应r，而 cifs中会对应读数据、读属性、读扩展属性等权限，
     * 因此在access接口中额外检查此类高级权限，且当前该函数仅针对于文件，目录权限不做处理
     * @param isOpenCifs 是否要判断cifs权限，若桶未开cifs，则无需判断cifs权限
     **/
    public static Tuple2<Boolean, Integer> judgeSMBACLToNFSRight(Inode inode, FSIdentity identity, int opt, boolean isOpenCifs) {
        if (!isOpenCifs || !CIFS_ACL_START) {
            return new Tuple2<>(true, (READ | LOOK_UP | MODIFY | EXTEND | FsConstants.NFSAccessAcl.DELETE | EXECUTE));
        }

        boolean isDir = inode.getNodeId() == 1 || (StringUtils.isNotEmpty(inode.getObjName()) && inode.getObjName().endsWith("/"));

        if (isDir) {
            return new Tuple2<>(true, (READ | LOOK_UP | MODIFY | EXTEND | FsConstants.NFSAccessAcl.DELETE | EXECUTE));
        }

        int res = 0;
        long access = parseSMBACLToRight(inode, identity, opt, false).var2;

        //检查是否具有读权限
        boolean read = (access & LOOKUP_DIR_OR_READ_DATA) != 0
                && (access & READ_ATTR) != 0
                && (access & READ_X_ATTR) != 0
                && (access & READ_CONTROL) != 0;

        if (read) {
            res |= READ;
        }

        //检查是否具有写权限
        boolean write = (access & CREATE_OR_WRITE_DATA) != 0
                && (access & MKDIR_OR_APPEND_DATA) != 0
                && (access & WRITE_ATTR) != 0
                && (access & WRITE_X_ATTR) != 0;

        if (write) {
            res |= (MODIFY | EXTEND);
        }

        //执行权限不检查
        if (isDir) {
            res |= LOOK_UP;
        } else {
            res |= EXECUTE;
        }

        if (aclDebug) {
            log.info("judge cifs acl: opt: {}, identity: {}, res: {}", opt, identity, res);
        }

        return new Tuple2<>(true, res);
    }

    /**
     * 根据inode中的ace，返回smb acl权限，如果原本存在的是 nfs ace，则需要进行翻译
     **/
    public static Tuple2<Boolean, Long> parseSMBACLToRight(Inode inode, FSIdentity identity, int opt, boolean nfsStart) {
        //开了nfs，同时不存在cifs ace的情况下可不判断cifs acl
        if (!isExistCifsACE(inode) && nfsStart) {
            return new Tuple2<>(true, FsConstants.SMB2ACCESS_MASK.SET_FILE_ALL_RIGHTS);
        }

        //请求者的账户信息
        String reqAccountId = identity.getS3Id();
        String userSID = identity.getUserSid();
        String groupSID = identity.getGroupSid();
        Set<Integer> groupSet = s3IDToGids.get(reqAccountId);

        //inode的属主与属组信息
        String inoOwner = FSIdentity.getUserSIDByUid(inode.getUid());
        String inoGroup = FSIdentity.getGroupSIDByGid(inode.getGid());

        long right = 0;
        Tuple2<Boolean, Long> res = new Tuple2<>(true, right);

        if ("0".equals(reqAccountId)) {
            //管理员拥有全部权限
            right |= (FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS
                    | FsConstants.SMB2ACCESS_MASK.SET_FILE_WRITE_RIGHTS
                    | FsConstants.SMB2ACCESS_MASK.SET_FILE_EXEC_RIGHTS);
        } else {
            //满权限，每拒绝一个权限，减去一部分
            long restAccess = FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS
                    | FsConstants.SMB2ACCESS_MASK.SET_FILE_WRITE_RIGHTS
                    | FsConstants.SMB2ACCESS_MASK.SET_FILE_EXEC_RIGHTS;

            //todo: 非拥有者查看dacl中权限；需要考虑拒绝权限
            List<Inode.ACE> ACEs = new LinkedList<>();
            int nfsEffectMask = -1;

            if (null == inode.getACEs() || inode.getACEs().isEmpty()) {
                // 不存在ACEs字段，仅存在mode
                ACEs = parseModeToCIFSACL(inode, -1);
            } else {
                ACEs = inode.getACEs();

                // 存在ACEs，但是ACEs是nfs ace
                if (isExistNfsACE(inode)) {
                    nfsEffectMask = ACLUtils.getNFSMask(inode.getACEs());
                    List<Inode.ACE> ugoACEs = parseModeToCIFSACL(inode, nfsEffectMask);
                    // mode权限和nfs acl设置给本身的权限可能不一致，比如mode rw-，而user 本身rwx，最后导致权限放大，但是在nfs ugo和
                    // nfs acl的权限判断中已经判断过，因此权限放大也没有关系
                    ACEs.addAll(ugoACEs);
                }
            }

            //0b100 --> user；0b010 --> group；0b001 --> everyone
            byte existCifsUGO = 0;

            //cifs目前看采用累加原则，不像nfs那样有遍历顺序，查看时nfs ace与cifs ace不可能共存
            for (Inode.ACE ace : ACEs) {
                if (isCifsACE(ace)) {
                    short flag = ace.getFlag();

                    //仅用于继承的权限不参与判断
                    if ((flag & INHERIT_ONLY_ACE) != 0) {
                        continue;
                    }

                    byte cType = ace.getCType();
                    String aceSid = ace.getSid();
                    long aceAccess = ace.getMask();
                    //是否是拥有者 | 所在主组 | 所在组 | 所有用户的权限
                    boolean isOwner = userSID.equals(aceSid);
                    boolean isMasterGroup = groupSID.equals(aceSid);
                    boolean isGroupMember = isGroupMember(groupSet, aceSid);
                    boolean isEveryone = aceSid.equals(SID.EVERYONE.getDisplayName());

                    boolean needJudge = isOwner || isMasterGroup || isGroupMember || isEveryone;

                    switch (cType) {
                        case ACCESS_ALLOWED_ACE_TYPE:
                            if (needJudge) {
                                right |= aceAccess;
                            }

                            if (inoOwner.equals(aceSid)) {
                                existCifsUGO |= 0b100;
                            }

                            if (inoGroup.equals(aceSid)) {
                                existCifsUGO |= 0b010;
                            }

                            if (isEveryone) {
                                existCifsUGO |= 0b001;
                            }

                            break;
                        case ACCESS_DENIED_ACE_TYPE:
                            if (needJudge) {
                                restAccess -= (aceAccess & CLEAR_SYNCHRONIZE_ACCESS);
                            }
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
                    //将nfs ace转成 cifs ace判断
                    int nType = ace.getNType();
                    int id = ace.getId();
                    int permission = ace.getRight();

                    if (nType > 4096) {
                        continue;
                    }

                    switch (nType) {
                        case NFSACL_USER:
                            if (nfsEffectMask > -1) {
                                permission &= nfsEffectMask;
                            }

                            if (FSIdentity.getUserSIDByUid(id).equals(userSID)) {
                                long cifsRight = ugoToCIFSAccess(permission);
                                right |= cifsRight;
                            }
                            break;
                        case NFSACL_GROUP:
                            if (nfsEffectMask > -1) {
                                permission &= nfsEffectMask;
                            }

                            if (FSIdentity.getGroupSIDByGid(id).equals(groupSID) || null != groupSet && groupSet.contains(id)) {
                                long cifsRight = ugoToCIFSAccess(permission);
                                right |= cifsRight;
                            }
                            break;
                        case NFSACL_CLASS:
                            //不判断mask权限，因为nfs acl中已经判断过了
                            break;
                    }
                }
            }

            //检查是否有漏判断ugo权限，如果有遗漏则补充判断
            if (existCifsUGO < 7) {
                int mode = inode.getMode();
                if ((existCifsUGO & 0b100) == 0) {
                    int userRight = NFSACL.parseModeToInt(mode, NFSACL_USER_OBJ);
                    long userAccess = ugoToCIFSAccess(userRight);
                    String aceSid = FSIdentity.getUserSIDByUid(inode.getUid());
                    if (userSID.equals(aceSid)) {
                        right |= userAccess;
                    }
                }

                if ((existCifsUGO & 0b010) == 0) {
                    int groupRight = NFSACL.parseModeToInt(mode, NFSACL_GROUP_OBJ);
                    long groupAccess = ugoToCIFSAccess(nfsEffectMask > -1 ? (groupRight & nfsEffectMask) : groupRight);
                    String aceSid = FSIdentity.getGroupSIDByGid(inode.getGid());
                    if (groupSID.equals(aceSid)) {
                        right |= groupAccess;
                    }
                }

                if ((existCifsUGO & 0b001) == 0) {
                    int otherRight = NFSACL.parseModeToInt(mode, NFSACL_OTHER);
                    long domainAccess = ugoToCIFSAccess(otherRight);
                    right |= domainAccess;
                }
            }

            right &= restAccess;
        }

        res.var2 = right;

        return res;
    }

    /**
     * 判断当前请求用户的组是否有sid这个成员
     **/
    public static boolean isGroupMember(Set<Integer> groupSet, String sid) {
        //检查是否为组sid
        if (!sid.startsWith(GROUP_SID_PREFIX) || null == groupSet || groupSet.isEmpty()) {
            return false;
        }

        boolean res = false;
        for (Integer gid : groupSet) {
            String groupSID = FSIdentity.getGroupSIDByGid(gid);
            if (groupSID.equals(sid)) {
                res = true;
            }
        }

        return res;
    }

    /**
     * 判断inode的ACEs中是否含有cifs的ACE，如果没有则无需再做cifs的权限的判断，仅nfs端的权限判断即可
     * @param inode
     * @return 是否存在cifs的ACE
     **/
    public static boolean isExistCifsACE(Inode inode) {
        if (null == inode || null == inode.getACEs() || inode.getACEs().isEmpty()) {
            return false;
        }

        boolean res = false;
        for (Inode.ACE ace : inode.getACEs()) {
            if (isCifsACE(ace)) {
                res = true;
                break;
            }
        }

        return res;
    }

    public static boolean isExistCifsACE(List<Inode.ACE> aceList) {
        if (null == aceList || aceList.isEmpty()) {
            return false;
        }

        boolean res = false;
        for (Inode.ACE ace : aceList) {
            if (isCifsACE(ace)) {
                res = true;
                break;
            }
        }

        return res;
    }

    public static boolean isExistNfsACE(Inode inode) {
        if (null == inode || null == inode.getACEs() || inode.getACEs().isEmpty()) {
            return false;
        }

        boolean res = false;
        for (Inode.ACE ace : inode.getACEs()) {
            if (isNfsACE(ace)) {
                res = true;
                break;
            }
        }

        return res;
    }

    public static boolean isExistNfsACE(List<Inode.ACE> aceList) {
        if (null == aceList || aceList.isEmpty()) {
            return false;
        }

        boolean res = false;
        for (Inode.ACE ace : aceList) {
            if (isNfsACE(ace)) {
                res = true;
                break;
            }
        }

        return res;
    }

    /**
     * 判断ACE是否含有cifs的权限，如果没有则无需再做cifs的权限的判断
     * @param ace
     * @return 是否存在cifs的权限
     **/
    public static boolean isCifsACE(Inode.ACE ace) {
        if (null == ace) {
            return false;
        }

        boolean res = false;
        byte cType = ace.getCType();
        short flag = ace.getFlag();
        String sid = ace.getSid();
        long mask = ace.getMask();
        if (cType > 0 || flag > 0 || StringUtils.isNotBlank(sid) || mask > 0) {
            res = true;
        }

        return res;
    }

    public static boolean isNfsACE(Inode.ACE ace) {
        if (null == ace) {
            return false;
        }

        boolean res = false;
        int nType = ace.getNType();
        int id = ace.getId();
        int permission = ace.getRight();
        if (nType > 0 || id > 0 || permission > 0) {
            res = true;
        }

        return res;
    }

    /**
     * 不存在ACEs字段，即为旧数据，或者为NFS创建的未设置nfs acl的数据的情况下
     * 将mode权限视为cifs权限，进行以下翻译：
     * inode:uid 对应的user权限 -> cifs 拥有者权限
     * inode:gid 对应的group权限 -> cifs 指定组权限
     * inode: other权限 -> cifs 域用户组权限
     *
     * 仅mode转换，没有设置继承权限，则flag=0
     *
     * @param inode
     * @param effectMask nfs acl中的最大生效权限，作用于 NFSACL_USER、NFSACL_GROUP_OBJ和 NFSACL_GROUP，
     *                   若为-1则无需进行计算
     **/
    public static List<Inode.ACE> parseModeToCIFSACL(Inode inode, int effectMask) {
        int mode = inode.getMode();
        List<Inode.ACE> res = new LinkedList<>();
        //拥有者权限
        int userRight = NFSACL.parseModeToInt(mode, NFSACL_USER_OBJ);
        long userAccess = ugoToCIFSAccess(userRight);
        String userSid = FSIdentity.getUserSIDByUid(inode.getUid());
        Inode.ACE userACE = new Inode.ACE((byte) 0, (short) 0, userSid, userAccess);
        res.add(userACE);

        //组权限
        int groupRight = NFSACL.parseModeToInt(mode, NFSACL_GROUP_OBJ);
        long groupAccess = ugoToCIFSAccess(effectMask > -1 ? (groupRight & effectMask) : groupRight);
        String groupSid = FSIdentity.getGroupSIDByGid(inode.getGid());
        Inode.ACE groupACE = new Inode.ACE((byte) 0, (short) 0, groupSid, groupAccess);
        res.add(groupACE);

        //其它用户权限 todo everyOne考虑更换为域用户
        int otherRight = NFSACL.parseModeToInt(mode, NFSACL_OTHER);
        long domainAccess = ugoToCIFSAccess(otherRight);
        String domainSid = SID.EVERYONE.getDisplayName();
        Inode.ACE domainACE = new Inode.ACE((byte) 0, (short) 0, domainSid, domainAccess);
        res.add(domainACE);

        return res;
    }

    /**
     * 将rwx的ugo权限转为cifs权限
     **/
    public static long ugoToCIFSAccess(int right) {
        long access = 0;
        //写权限
        if ((right & 0b100) != 0) {
            access |= FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS;
        }

        //读权限
        if ((right & 0b010) != 0) {
            access |= FsConstants.SMB2ACCESS_MASK.SET_FILE_WRITE_RIGHTS;
        }

        //执行权限
        if ((right & 0b001) != 0) {
            access |= FsConstants.SMB2ACCESS_MASK.SET_FILE_EXEC_RIGHTS;
        }

        return access;
    }

    public static void refreshModeByCifsAce(Inode inode, Inode.ACE cifsACE) {
        String sid = cifsACE.getSid();
        long access = cifsACE.getMask();

        //获取inode对应的属主和属组
        String inoOwnerSID = FSIdentity.getUserSIDByUid(inode.getUid());
        String inoGroupSID = FSIdentity.getGroupSIDByGid(inode.getGid());

        boolean isDir = inode.getNodeId() == 1 || (null != inode.getObjName() && inode.getObjName().endsWith("/"));

        if (inoOwnerSID.equals(sid)) {
            //对应user权限
            int mode = inode.getMode();
            mode = CIFSACL.refreshModeByCifsAccess(access, mode, NFSACL_USER_OBJ, isDir);
            inode.setMode(mode);
        } else if (inoGroupSID.equals(sid)) {
            //对应group权限
            int mode = inode.getMode();
            mode = CIFSACL.refreshModeByCifsAccess(access, mode, NFSACL_GROUP_OBJ, isDir);
            inode.setMode(mode);
        } else if (SID.EVERYONE.getDisplayName().equals(sid)) {
            //对应other权限
            int mode = inode.getMode();
            mode = CIFSACL.refreshModeByCifsAccess(access, mode, NFSACL_OTHER, isDir);
            inode.setMode(mode);
        }
    }

    /**
     * 遍历所有cifs acl中访问控制的项，统计允许权限和拒绝权限，最后设置对应的 mode
     **/
    public static void updateUGOACE(List<Inode.ACE> ACEs, Inode inode) {
        if (null == ACEs || ACEs.isEmpty()) {
            return;
        }

        try {
            List<Inode.ACE> curAcl = ACEs;

            boolean isDir = inode.getNodeId() == 1 || (null != inode.getObjName() && inode.getObjName().endsWith("/"));

            //分别存储要修正的user、group和other权限
            long[] allowAccess = {-1, -1, -1};
            long[] deniedAccess = {-1, -1, -1};

            //获取inode对应的属主和属组
            String inoOwnerSID = FSIdentity.getUserSIDByUid(inode.getUid());
            String inoGroupSID = FSIdentity.getGroupSIDByGid(inode.getGid());

            int mode = inode.getMode();
            Iterator<Inode.ACE> iterator = curAcl.listIterator();

            while (iterator.hasNext()) {
                Inode.ACE ace = iterator.next();
                statisticAccess(allowAccess, deniedAccess, ace, inoOwnerSID, inoGroupSID);
            }

            //根据ace修正mode权限
            if (allowAccess[0] > -1 || allowAccess[1] > -1 || allowAccess[2] > -1
                    || deniedAccess[0] > -1 || deniedAccess[1] > -1 || deniedAccess[2] > -1) {
                for (int i = 0; i < UGO_ACE_ARR.length; i++) {
                    long curAllow = allowAccess[i];
                    long curDenied = deniedAccess[i];
                    int type = UGO_ACE_ARR[i];
                    if (curAllow > -1 || curDenied > -1) {
                        int curRight = NFSACL.parseModeToInt(mode, type);
                        long curAccess = CIFSACL.ugoToCIFSAccess(curRight);
                        if (curAllow == -1 && curDenied > -1) {
                            curAccess = 0;
                        } else if (curAllow > -1 && curDenied == -1) {
                            curAccess = curAllow;
                        } else {
                            curAccess = curAllow - curDenied;
                            curAccess = curAccess <= 0 ? 0 : curAccess;
                        }

                        mode = CIFSACL.refreshModeByCifsAccess(curAccess, mode, type, isDir);
                        inode.setMode(mode);
                    }
                }
            }
        } catch (Exception e) {
            log.error("update ugo ace error: {}, ", ACEs, e);
        }
    }

    /**
     * 根据cifs权限修改ugo权限
     * 翻译时cifs 权限粒度向大范围翻译
     * 有 CIFS: LOOKUP_DIR_OR_READ_DATA -> NFS: R
     * 有 CIFS: CREATE_OR_WRITE_DATA 或 MKDIR_OR_APPEND_DATA 或 DELETE_SUB_FILE_AND_DIR -> NFS: W
     * 有 CIFS: LIST_DIR_OR_EXEC_FILE -> NFS: X
     *
     **/
    public static int refreshModeByCifsAccess(long access, int oriMode, int type, boolean isDir) {
        int mode = oriMode;
        switch (type) {
            case NFSACL_USER_OBJ:
                mode = NFSACL.refreshMode(mode, NFSACL_USER_OBJ);
                if ((access & LOOKUP_DIR_OR_READ_DATA) != 0) {
                    mode |= USER_READ;
                }

                if (isDir) {
                    if ((access & CREATE_OR_WRITE_DATA) != 0 || (access & MKDIR_OR_APPEND_DATA) != 0 || (access & DELETE_SUB_FILE_AND_DIR) != 0) {
                        mode |= USER_WRITE;
                    }
                } else {
                    if ((access & CREATE_OR_WRITE_DATA) != 0 || (access & MKDIR_OR_APPEND_DATA) != 0) {
                        mode |= USER_WRITE;
                    }
                }

                if ((access & LIST_DIR_OR_EXEC_FILE) != 0) {
                    mode |= USER_EXEC;
                }
                break;
            case NFSACL_GROUP_OBJ:
                mode = NFSACL.refreshMode(mode, NFSACL_GROUP_OBJ);
                if ((access & LOOKUP_DIR_OR_READ_DATA) != 0) {
                    mode |= GROUP_READ;
                }

                if (isDir) {
                    if ((access & CREATE_OR_WRITE_DATA) != 0 || (access & MKDIR_OR_APPEND_DATA) != 0 || (access & DELETE_SUB_FILE_AND_DIR) != 0) {
                        mode |= GROUP_WRITE;
                    }
                } else {
                    if ((access & CREATE_OR_WRITE_DATA) != 0 || (access & MKDIR_OR_APPEND_DATA) != 0) {
                        mode |= GROUP_WRITE;
                    }
                }

                if ((access & LIST_DIR_OR_EXEC_FILE) != 0) {
                    mode |= GROUP_EXEC;
                }
                break;
            case NFSACL_OTHER:
                mode = NFSACL.refreshMode(mode, NFSACL_OTHER);
                if ((access & LOOKUP_DIR_OR_READ_DATA) != 0) {
                    mode |= OTHER_READ;
                }

                if (isDir) {
                    if ((access & CREATE_OR_WRITE_DATA) != 0 || (access & MKDIR_OR_APPEND_DATA) != 0 || (access & DELETE_SUB_FILE_AND_DIR) != 0) {
                        mode |= OTHER_WRITE;
                    }
                } else {
                    if ((access & CREATE_OR_WRITE_DATA) != 0 || (access & MKDIR_OR_APPEND_DATA) != 0) {
                        mode |= OTHER_WRITE;
                    }
                }

                if ((access & LIST_DIR_OR_EXEC_FILE) != 0) {
                    mode |= OTHER_EXEC;
                }
                break;
            default:
                break;
        }

        return mode;
    }

    public static int cifsAccessToUgo(long access, boolean isDir) {
        int right = 0;
        if ((access & LOOKUP_DIR_OR_READ_DATA) != 0) {
            right |= 0b100;
        }

        if (isDir) {
            if ((access & CREATE_OR_WRITE_DATA) != 0 || (access & MKDIR_OR_APPEND_DATA) != 0 || (access & DELETE_SUB_FILE_AND_DIR) != 0) {
                right |= 0b010;
            }
        } else {
            if ((access & CREATE_OR_WRITE_DATA) != 0 || (access & MKDIR_OR_APPEND_DATA) != 0) {
                right |= 0b010;
            }
        }

        if ((access & LIST_DIR_OR_EXEC_FILE) != 0) {
            right |= 0b001;
        }

        return right;
    }

    /**
     * 将nfs 中access请求的权限转换为 cifs acl中的 access
     **/
    public static long parseNFSRightToCIFSRight(Inode inode, int nfsRight, boolean isParent) {
        long cifsRight = 0;

        try {
            boolean read = false;
            boolean write = false;
            boolean execute = false;
            boolean isDir =  inode.getNodeId() == 1 || inode.getObjName().endsWith("/");

            if (isDir) {
                if ((nfsRight & FsConstants.NFSAccessAcl.READ) != 0) {
                    read = true;
                    if (aclDebug) {
                        log.info("right: {}, read: {}, isDir: {}", nfsRight, read, isDir);
                    }
                }

                if ((nfsRight & FsConstants.NFSAccessAcl.MODIFY) != 0 && (nfsRight & FsConstants.NFSAccessAcl.EXTEND) != 0 && (nfsRight & FsConstants.NFSAccessAcl.DELETE) != 0) {
                    write = true;
                    if (aclDebug) {
                        log.info("right: {}, write: {}, isDir: {}", nfsRight, write, isDir);
                    }
                }

                if ((nfsRight & FsConstants.NFSAccessAcl.LOOK_UP) != 0) {
                    execute = true;
                    if (aclDebug) {
                        log.info("right: {}, lookup: {}, isDir: {}", nfsRight, execute, isDir);
                    }
                }
            } else {
                if ((nfsRight & FsConstants.NFSAccessAcl.READ) != 0) {
                    read = true;
                    if (aclDebug) {
                        log.info("right: {}, read: {}, isDir: {}", nfsRight, read, isDir);
                    }
                }

                if ((nfsRight & FsConstants.NFSAccessAcl.MODIFY) != 0 && (nfsRight & FsConstants.NFSAccessAcl.EXTEND) != 0 && (nfsRight & FsConstants.NFSAccessAcl.DELETE) != 0) {
                    write = true;
                    if (aclDebug) {
                        log.info("right: {}, write: {}, isDir: {}", nfsRight, write, isDir);
                    }
                }

                if ((nfsRight & FsConstants.NFSAccessAcl.EXECUTE) != 0) {
                    execute = true;
                    if (aclDebug) {
                        log.info("right: {}, execute: {}, isDir: {}", nfsRight, execute, isDir);
                    }
                }
            }

            if (read) {
                cifsRight |= FsConstants.SMB2ACCESS_MASK.SET_FILE_READ_RIGHTS;
            }

            if (write) {
                cifsRight |= FsConstants.SMB2ACCESS_MASK.SET_FILE_WRITE_RIGHTS;
            }

            if (execute) {
                cifsRight |= FsConstants.SMB2ACCESS_MASK.SET_FILE_EXEC_RIGHTS;
            }
        } catch (Exception e) {
            log.error("parse nfs right to cifs error, isParent: {}, inode: {} ", isParent, inode, e);
        }

        return cifsRight;
    }

    /**
     * 把cifs的ace转成nfs的，根据sid判断是用户本身的ugo，还是其他用户权限或者其他用户组权限
     * 该函数仅用于nfs ugo权限和nfs acl权限的判断时，把cifs给其它账户或账户组设置的权限转为nfs 权限
     * 此函数不处理拒绝权限，所有cifs拒绝权限统一在cifs权限判断逻辑中处理
     * @param inode 从inode中获取用户和主组信息
     * @param isDir 判断的对象是文件还是目录
     **/
    public static List<Inode.ACE> transferCifsAceToNfs(Inode inode, boolean isDir) {
        if (null == inode || InodeUtils.isError(inode) || null == inode.getACEs() || inode.getACEs().isEmpty()) {
            return null;
        }

        String inodeOwner = FSIdentity.getUserSIDByUid(inode.getUid());
        String groupOwner = FSIdentity.getGroupSIDByGid(inode.getGid());

        List<Inode.ACE> nfsACEs = new LinkedList<>();
        for (Inode.ACE ace : inode.getACEs()) {
            //拒绝类型ace不在 nfs ugo与 nfs acl中判断
            if (ace.getCType() == ACCESS_DENIED_ACE_TYPE) {
                continue;
            }

            String aceSID = ace.getSid();
            boolean isInherit = ((ace.getFlag() & INHERIT_ONLY_ACE) != 0);

            //是用户权限本身，特定的用户权限
            if (aceSID.startsWith(FSIdentity.USER_SID_PREFIX)) {
                Inode.ACE nfsACE = new Inode.ACE();
                int inoUid = FSIdentity.getUidBySID(aceSID);
                int right = cifsAccessToUgo(ace.getMask(), isDir);
                nfsACE.setId(inoUid);
                if (!aceSID.equals(inodeOwner) && !isInherit) {
                    //判断nfs权限时候，不应该把cifs ace中u、g、o对应的权限翻译为nfs ace；cifs独有的权限会在cifs acl中判断
                    nfsACE.setNType(NFSACL_USER);
                    nfsACE.setRight(right);
                    nfsACEs.add(nfsACE);
                } else {
                    continue;
                }
            }

            //是用户权限本身，特定用户组权限
            if (aceSID.startsWith(FSIdentity.GROUP_SID_PREFIX)) {
                Inode.ACE nfsACE = new Inode.ACE();
                int inoGid = FSIdentity.getGidBySID(aceSID);
                int right = cifsAccessToUgo(ace.getMask(), isDir);
                nfsACE.setId(inoGid);
                if (!aceSID.equals(groupOwner) && isInherit){
                    nfsACE.setNType(NFSACL_GROUP);
                    nfsACE.setRight(right);
                    nfsACEs.add(nfsACE);
                } else {
                    continue;
                }
            }

            //其它用户权限
            if (SID.EVERYONE.getDisplayName().equals(aceSID)) {
                continue;
            }
        }

        if (aclDebug) {
            log.info("【transfer】 ino: {}, obj: {}, ori: {}, now: {}", inode.getNodeId(), inode.getObjName(), inode.getACEs(), nfsACEs);
        }

        return nfsACEs;
    }

    /**
     * 通过比较新的mode和旧的mode，获取发生变化的mode是在user、group、other的哪种类型
     *
     * @param oldMode 旧的权限位
     * @param newMode 新的权限位
     * @return [user权限是否改变，group权限是否改变，other权限是否改变]
     **/
    public static boolean[] checkModeUpdType(int oldMode, int newMode) {
        boolean[] res = {false, false, false};
        int[] type = {NFSACL_USER_OBJ, NFSACL_GROUP_OBJ, NFSACL_OTHER};
        for (int i = 0; i < res.length; i++) {
            int oldPermission = 0;
            int newPermission = 0;

            switch (type[i]) {
                case NFSACL_USER_OBJ:
                    oldPermission = oldMode & (USER_READ | USER_WRITE | USER_EXEC);
                    newPermission = newMode & (USER_READ | USER_WRITE | USER_EXEC);
                    break;
                case NFSACL_GROUP_OBJ:
                    oldPermission = oldMode & (GROUP_READ | GROUP_WRITE | GROUP_EXEC);
                    newPermission = newMode & (GROUP_READ | GROUP_WRITE | GROUP_EXEC);
                    break;
                case NFSACL_OTHER:
                    oldPermission = oldMode & (OTHER_READ | OTHER_WRITE | OTHER_EXEC);
                    newPermission = newMode & (OTHER_READ | OTHER_WRITE | OTHER_EXEC);
                    break;
                default:
                    break;
            }

            if (oldPermission != newPermission) {
                res[i] = true;
            }
        }

        return res;
    }

    public static List<Inode.ACE> parseAclToCifsACL(Inode inode) {
        List<Inode.ACE> resList = new LinkedList<>();
        try {
            //inode的属主与属组信息
            String inoOwner = FSIdentity.getUserSIDByUid(inode.getUid());
            String inoGroup = FSIdentity.getGroupSIDByGid(inode.getGid());

            List<Inode.ACE> aceList = inode.getACEs();

            if (null != aceList && !aceList.isEmpty()) {
                //判断是否存在cifs ace，如果有cifs ace，则读取cifs ace即可，如果没有，则需要将nfs ace转换
                if (CIFSACL.isExistCifsACE(aceList)) {

                    //0b100 --> user；0b010 --> group；0b001 --> everyone
                    byte existCifsUGO = 0;

                    for (Inode.ACE ace : aceList) {
                        if (ace.getNType() == 0 && CIFSACL.isCifsACE(ace)) {
                            if ((ace.getFlag() & INHERIT_ONLY_ACE) == 0) {
                                if (inoOwner.equals(ace.getSid())) {
                                    existCifsUGO |= 0b100;
                                }

                                if (inoGroup.equals(ace.getSid())) {
                                    existCifsUGO |= 0b010;
                                }

                                if (SID.EVERYONE.getDisplayName().equals(ace.getSid())) {
                                    existCifsUGO |= 0b001;
                                }
                            }

                            resList.add(ace);
                        }
                    }

                    //检查是否有漏判断ugo权限，如果有遗漏则补充判断
                    if (existCifsUGO < 7) {
                        int mode = inode.getMode();
                        if ((existCifsUGO & 0b100) == 0) {
                            int userRight = NFSACL.parseModeToInt(mode, NFSACL_USER_OBJ);
                            long userAccess = CIFSACL.ugoToCIFSAccess(userRight);
                            String aceSid = FSIdentity.getUserSIDByUid(inode.getUid());
                            Inode.ACE ace = new Inode.ACE((byte) 0, (short) 0, aceSid, userAccess);
                            resList.add(ace);
                        }

                        if ((existCifsUGO & 0b010) == 0) {
                            int groupRight = NFSACL.parseModeToInt(mode, NFSACL_GROUP_OBJ);
                            long groupAccess = CIFSACL.ugoToCIFSAccess(groupRight);
                            String aceSid = FSIdentity.getGroupSIDByGid(inode.getGid());
                            Inode.ACE ace = new Inode.ACE((byte) 0, (short) 0, aceSid, groupAccess);
                            resList.add(ace);
                        }

                        if ((existCifsUGO & 0b001) == 0) {
                            int otherRight = NFSACL.parseModeToInt(mode, NFSACL_OTHER);
                            long domainAccess = CIFSACL.ugoToCIFSAccess(otherRight);
                            Inode.ACE ace = new Inode.ACE((byte) 0, (short) 0, SID.EVERYONE.getDisplayName(), domainAccess);
                            resList.add(ace);
                        }
                    }
                } else {
                    //只有nfs ace就需要将nfs ace转换为 cifs ace
                    for (Inode.ACE ace : aceList) {
                        int nType = ace.getNType();
                        int id = ace.getId();
                        int right = ace.getRight();

                        SMB2ACE.ACEHeader aceHeader = new SMB2ACE.ACEHeader();
                        SMB2ACE.ACCESS_ALLOWED_ACE allowed_ace = new SMB2ACE.ACCESS_ALLOWED_ACE();
                        allowed_ace.setHeader(aceHeader);

                        Inode.ACE cifsAce = null;

                        switch (nType) {
                            case NFSACL_USER:
                                cifsAce = new Inode.ACE((byte) 0, (short) 0, FSIdentity.getUserSIDByUid(id), CIFSACL.ugoToCIFSAccess(right));
                                resList.add(cifsAce);
                                break;
                            case NFSACL_GROUP:
                                cifsAce = new Inode.ACE((byte) 0, (short) 0, FSIdentity.getGroupSIDByGid(id), CIFSACL.ugoToCIFSAccess(right));
                                resList.add(cifsAce);
                                break;
                            case DEFAULT_NFSACL_USER_OBJ:
                            case DEFAULT_NFSACL_USER:
                                cifsAce = new Inode.ACE((byte) 0, (short) (CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE | INHERIT_ONLY_ACE), FSIdentity.getUserSIDByUid(id), CIFSACL.ugoToCIFSAccess(right));
                                resList.add(cifsAce);
                                break;
                            case DEFAULT_NFSACL_GROUP_OBJ:
                            case DEFAULT_NFSACL_GROUP:
                                cifsAce = new Inode.ACE((byte) 0, (short) (CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE | INHERIT_ONLY_ACE), FSIdentity.getGroupSIDByGid(id), CIFSACL.ugoToCIFSAccess(right));
                                resList.add(cifsAce);
                                break;
                            case DEFAULT_NFSACL_CLASS:
                                break;
                            case DEFAULT_NFSACL_OTHER:
                                cifsAce = new Inode.ACE((byte) 0, (short) (CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE | INHERIT_ONLY_ACE), SID.EVERYONE.getDisplayName(), CIFSACL.ugoToCIFSAccess(right));
                                resList.add(cifsAce);
                                break;
                            default:
                                break;
                        }
                    }

                    //将mode权限转化为cifs acl返回，此处用于gui显示，因此无需计算effectMask
                    List<Inode.ACE> transferList = CIFSACL.parseModeToCIFSACL(inode, -1);
                    resList.addAll(transferList);
                }
            } else {
                //如果当前ace列表为空，则以mode权限翻译为cifs ace返回
                List<Inode.ACE> transferList = CIFSACL.parseModeToCIFSACL(inode, -1);
                resList.addAll(transferList);
            }
        } catch (Exception e) {
            log.error("parse acl to cifs acl error", e);
        }

        return resList;
    }
}
