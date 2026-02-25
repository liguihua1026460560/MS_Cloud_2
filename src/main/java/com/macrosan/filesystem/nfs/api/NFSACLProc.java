package com.macrosan.filesystem.nfs.api;

import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cifs.types.smb2.SID;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.RpcCallHeader;
import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.call.GetAclCall;
import com.macrosan.filesystem.nfs.call.SetAclCall;
import com.macrosan.filesystem.nfs.reply.GetAclReply;
import com.macrosan.filesystem.nfs.reply.SetAclReply;
import com.macrosan.filesystem.nfs.types.FAttr3;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.FSIPACLUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.filesystem.utils.acl.NFSACL;
import com.macrosan.message.jsonmsg.FSIdentity;
import com.macrosan.message.jsonmsg.FSIpACL;
import com.macrosan.message.jsonmsg.Inode;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.LinkedList;
import java.util.List;

import static com.macrosan.filesystem.FsConstants.NFSACLType.*;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.CONTAINER_INHERIT_ACE;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.OBJECT_INHERIT_ACE;
import static com.macrosan.filesystem.FsConstants.SMB2ACEType.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEType.SYSTEM_SCOPED_POLICY_ID_ACE_TYPE;
import static com.macrosan.filesystem.nfs.NFSBucketInfo.getBucketName;
import static com.macrosan.filesystem.utils.InodeUtils.isError;
import static com.macrosan.message.jsonmsg.Inode.NO_PERMISSION_INODE;

@Log4j2
public class NFSACLProc {
    public Mono<RpcReply> getAcl(RpcCallHeader callHeader, ReqInfo reqHeader, GetAclCall call) {
        GetAclReply getAclReply = new GetAclReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reqHeader.bucket = getBucketName(call.fh.fsid);
        if (ACLUtils.aclDebug) {
            log.info("【getAcl】: {}, msgId: {}, header: {}", call, callHeader.getHeader().id, callHeader);
        }
        return Node.getInstance().getInode(reqHeader.bucket, call.fh.ino)
                .map(inode -> {
                    getAclReply.mask = call.mask;
                    if (isError(inode)) {
                        getAclReply.status = NFS3ERR_INVAL;
                        getAclReply.attrFollow = 0;
                        getAclReply.aclCount = 0;
                        return (RpcReply) getAclReply;
                    } else {
                        getAclReply.status = NFS3_OK;
                        getAclReply.attr = FAttr3.mapToAttr(inode, call.fh.fsid);

                        //如果没有ACEs，就把UGO权限对应的ACE返回
                        for (int i = 0; i < UGO_ACE_ARR.length; i++) {
                            int id = 0;
                            int type = UGO_ACE_ARR[i];
                            if (type == NFSACL_USER_OBJ) {
                                id = inode.getUid();
                            } else if (type == NFSACL_GROUP_OBJ) {
                                id = inode.getGid();
                            }

                            GetAclReply.AclEntry entry = new GetAclReply.AclEntry(UGO_ACE_ARR[i], id, NFSACL.parseModeToInt(inode.getMode(), type));
                            getAclReply.entries.add(entry);
                        }

                        //获取inode对应的属主和属组
                        String inoOwnerSID = FSIdentity.getUserSIDByUid(inode.getUid());
                        String inoGroupSID = FSIdentity.getGroupSIDByGid(inode.getGid());

                        boolean isDir = inode.getNodeId() == 1 || (null != inode.getObjName() && inode.getObjName().endsWith("/"));

                        if (null != inode.getACEs() && !inode.getACEs().isEmpty()) {
                            List<Inode.ACE> curAcl = inode.getACEs();
                            boolean existCifsAce = false;
                            boolean existCifsInherit = false;

                            //用于记录存在cifs继承权限时，缺失的default user，group和other权限，按照 man setfacl 手册，nfs 若设置继承
                            //权限，缺失的继承权限会自动拷贝一份当前的ugo权限作为继承权限；仅在显示时加上
                            boolean[] defaultAbsent = {false, false, false};
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
                                                    defaultAbsent[0] = true;
                                                    GetAclReply.AclEntry entry = new GetAclReply.AclEntry(DEFAULT_NFSACL_USER_OBJ, FSIdentity.getUidBySID(sid), CIFSACL.cifsAccessToUgo(access, isDir));
                                                    getAclReply.defaultEntries.add(entry);
                                                } else if (inoGroupSID.equals(sid)) {
                                                    defaultAbsent[1] = true;
                                                    GetAclReply.AclEntry entry = new GetAclReply.AclEntry(DEFAULT_NFSACL_GROUP_OBJ, FSIdentity.getGidBySID(sid), CIFSACL.cifsAccessToUgo(access, isDir));
                                                    getAclReply.defaultEntries.add(entry);
                                                } else if (SID.EVERYONE.getDisplayName().equals(sid)) {
                                                    defaultAbsent[2] = true;
                                                    GetAclReply.AclEntry entry = new GetAclReply.AclEntry(DEFAULT_NFSACL_OTHER, 0, CIFSACL.cifsAccessToUgo(access, isDir));
                                                    getAclReply.defaultEntries.add(entry);
                                                } else if (sid.startsWith(FSIdentity.USER_SID_PREFIX)) {
                                                    GetAclReply.AclEntry entry = new GetAclReply.AclEntry(DEFAULT_NFSACL_USER, FSIdentity.getUidBySID(sid), CIFSACL.cifsAccessToUgo(access, isDir));
                                                    getAclReply.defaultEntries.add(entry);
                                                } else if (sid.startsWith(FSIdentity.GROUP_SID_PREFIX)) {
                                                    GetAclReply.AclEntry entry = new GetAclReply.AclEntry(DEFAULT_NFSACL_GROUP, FSIdentity.getGidBySID(sid), CIFSACL.cifsAccessToUgo(access, isDir));
                                                    getAclReply.defaultEntries.add(entry);
                                                }
                                            } else {
                                                //不存在继承权限
                                                if (inoOwnerSID.equals(sid) || inoGroupSID.equals(sid) || SID.EVERYONE.getDisplayName().equals(sid)) {
                                                    break;
                                                } else {
                                                    if (sid.startsWith(FSIdentity.USER_SID_PREFIX)) {
                                                        GetAclReply.AclEntry entry = new GetAclReply.AclEntry(NFSACL_USER, FSIdentity.getUidBySID(sid), CIFSACL.cifsAccessToUgo(access, isDir));
                                                        getAclReply.entries.add(entry);
                                                    } else if (sid.startsWith(FSIdentity.GROUP_SID_PREFIX)) {
                                                        GetAclReply.AclEntry entry = new GetAclReply.AclEntry(NFSACL_GROUP, FSIdentity.getGidBySID(sid), CIFSACL.cifsAccessToUgo(access, isDir));
                                                        getAclReply.entries.add(entry);
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
                                    GetAclReply.AclEntry entry = new GetAclReply.AclEntry(ntype, id, permission);
                                    if (ntype > 4096) {
                                        getAclReply.defaultEntries.add(entry);
                                    } else if (ntype != NFSACL_USER_OBJ && ntype != NFSACL_GROUP_OBJ && ntype != NFSACL_OTHER) {
                                        getAclReply.entries.add(entry);
                                    }
                                }
                            }

                            if (existCifsAce) {
                                if (existCifsInherit) {
                                    GetAclReply.AclEntry entry = new GetAclReply.AclEntry(DEFAULT_NFSACL_CLASS, 0, 7);
                                    getAclReply.defaultEntries.add(entry);

                                    for (int i = 0; i < DEFAULT_UGO_ACE_ARR.length; i++) {
                                        if (!defaultAbsent[i]) {
                                            int id = 0;
                                            int type = DEFAULT_UGO_ACE_ARR[i];
                                            if (type == DEFAULT_NFSACL_USER_OBJ) {
                                                id = inode.getUid();
                                            } else if (type == DEFAULT_NFSACL_GROUP_OBJ) {
                                                id = inode.getGid();
                                            }
                                            GetAclReply.AclEntry defaultEntry = new GetAclReply.AclEntry(DEFAULT_UGO_ACE_ARR[i], id, NFSACL.parseModeToInt(inode.getMode(), UGO_ACE_ARR[i]));
                                            getAclReply.defaultEntries.add(defaultEntry);
                                        }
                                    }
                                }

                                GetAclReply.AclEntry entry = new GetAclReply.AclEntry(NFSACL_CLASS, 0, 7);
                                getAclReply.entries.add(entry);
                            }
                        }

                        getAclReply.aclCount = getAclReply.entries.size();
                        getAclReply.totalAclEntries = getAclReply.entries.size();
                        getAclReply.defaultAclCount = getAclReply.defaultEntries.size();
                        getAclReply.totalDefaultEntries = getAclReply.defaultEntries.size();

                        return (RpcReply) getAclReply;
                    }
                })
                .doOnError(e -> log.info("get nfsACL fail, ", e));
    }

    /**
     * setfacl指令只可以赋予user和group权限，不可以赋予other权限
     * setfacl中的 mask 掩码需要与 特定user或group的权限相与，才是这些特定user与group的实际权限
     * 继承权限即 default的acl无需判断，只用于创建子目录和文件时继承权限使用，修改一个目录的继承权限后，原本已经目录下面的子目录不会修改继承权限
     * 注意：
     * 在原生NFS ACL中，设置NFS ACL后，原本的UGO权限即由NFS ACL中的 NFSACL_USER_OBJ、NFSACL_GROUP_OBJ和NFSACL_OTHER来决定，即相当于
     * 原本的mode参数；在moss中，设置了NFS ACL后，以上三个参数不会额外记录，而是记录在原本的mode参数中，因此如果要修改相关的参数，需要同步修改
     * mode的值
     **/
    public Mono<RpcReply> setAcl(RpcCallHeader callHeader, ReqInfo reqHeader, SetAclCall call) {
        SetAclReply setAclReply = new SetAclReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reqHeader.bucket = getBucketName(call.fh.fsid);
        if (ACLUtils.aclDebug) {
            log.info("【setAcl】: {}, msgId: {}, header: {}", call, callHeader.getHeader().id, callHeader);
        }

        return NFSBucketInfo.getBucketInfoReactive(reqHeader.bucket)
                .flatMap(bucketInfo -> {
                    if (!CheckUtils.writePermissionCheck(bucketInfo)) {
                        setAclReply.status = NFS3ERR_ROFS;
                        setAclReply.attrFollows = 0;
                        return Mono.just((RpcReply)setAclReply);
                    }

                    reqHeader.bucketInfo = bucketInfo;
                    FSIpACL ipACL = FSIPACLUtils.getIpACL(bucketInfo, FSIPACLUtils.getClientIp(reqHeader.nfsHandler));
                    if (ipACL == null ) {
                        setAclReply.status = NFS3ERR_ACCES;
                        setAclReply.attrFollows = 0;
                        return Mono.just((RpcReply)setAclReply);
                    }

                    reqHeader.ipACL = ipACL;
                    if (NFSACL.isNotNfsIpAclSquash(reqHeader, callHeader)) {
                        NFSACL.nfsSquash(reqHeader, callHeader);
                    }

                    int[] uidAndGid = ACLUtils.getUidAndGid(callHeader);

                    return Node.getInstance().getInode(reqHeader.bucket, call.fh.ino)
                            .flatMap(inode -> {
                                if (InodeUtils.isError(inode)) {
                                    setAclReply.status = NFS3ERR_INVAL;
                                    setAclReply.attrFollows = 0;
                                    return Mono.just((RpcReply)setAclReply);
                                }
                                if (inode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                                    setAclReply.status = NFS3ERR_ROFS;
                                    setAclReply.attrFollows = 0;
                                    return Mono.just((RpcReply)setAclReply);
                                }

                                if (inode.getUid() != uidAndGid[0] && uidAndGid[0] != 0) {
                                    setAclReply.status = NFS3ERR_ACCES;
                                    setAclReply.attrFollows = 0;
                                    return Mono.just((RpcReply)setAclReply);
                                }

                                // 除了文件的所有者和超级用户uid=0，其它任何用户或者用户组即使已经被授权rwx，也无法更改当前文件的acls
                                List<Inode.ACE> aclList = new LinkedList<>();
                                for (GetAclReply.AclEntry entry : call.entries) {
                                    Inode.ACE ace = new Inode.ACE(entry.type, entry.permissions, entry.uid);
                                    aclList.add(ace);
                                }

                                for (GetAclReply.AclEntry entry : call.defEntries) {
                                    Inode.ACE ace = new Inode.ACE(entry.type, entry.permissions, entry.uid);
                                    aclList.add(ace);
                                }

                                // 判断当前用户是否具有修改当前文件acl的权限
                                if (inode.getACEs() != null) {
                                    List<Inode.ACE> curAcl = inode.getACEs();
                                    if (curAcl.toString().equals(aclList.toString())) {
                                        setAclReply.status = NFS3_OK;
                                        setAclReply.attrFollows = 1;
                                        setAclReply.attr = FAttr3.mapToAttr(inode, call.fh.fsid);
                                        return Mono.just((RpcReply)setAclReply);
                                    }
                                }

                                return Node.getInstance().updateInodeACL(reqHeader.bucket, call.fh.ino, Json.encode(aclList))
                                        .map(i -> {
                                            if (!isError(i)) {
                                                setAclReply.status = NFS3_OK;
                                                setAclReply.attrFollows = 1;
                                                setAclReply.attr = FAttr3.mapToAttr(i, call.fh.fsid);
                                            } else {
                                                setAclReply.status = NFS3ERR_INVAL;
                                                setAclReply.attrFollows = 0;
                                            }
                                            return (RpcReply) setAclReply;
                                        });
                            });
                })
                .doOnError(e -> log.error("set nfs acl fail ", e));
    }
}
