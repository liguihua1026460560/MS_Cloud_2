package com.macrosan.filesystem.cifs.api.smb2;

import com.macrosan.action.managestream.FSPerformanceService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.*;
import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.SMB2.SMB2Reply;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.call.smb2.*;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.lease.LeaseCache;
import com.macrosan.filesystem.cifs.lease.LeaseClient;
import com.macrosan.filesystem.cifs.lease.LeaseLock;
import com.macrosan.filesystem.cifs.lock.CIFSLockClient;
import com.macrosan.filesystem.cifs.reply.smb2.*;
import com.macrosan.filesystem.cifs.rpc.pdu.call.RpcRequestCall;
import com.macrosan.filesystem.cifs.shareAccess.ShareAccessClient;
import com.macrosan.filesystem.cifs.shareAccess.ShareAccessLock;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.CreateContext;
import com.macrosan.filesystem.cifs.types.smb2.IOCTLSubCall;
import com.macrosan.filesystem.cifs.types.smb2.QueryDirInfo;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.cifs.types.smb2.pipe.BindPduInfo;
import com.macrosan.filesystem.cifs.types.smb2.pipe.RpcBindGenerator;
import com.macrosan.filesystem.cifs.types.smb2.pipe.RpcPipeType;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.utils.*;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.perf.AddressFSPerfLimiter;
import com.macrosan.utils.perf.BucketFSPerfLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuples;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.action.managestream.FSPerformanceService.Instance_Type.*;
import static com.macrosan.action.managestream.FSPerformanceService.getAddressPerfRedisKey;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NTStatus.*;
import static com.macrosan.filesystem.cache.ReadObjCache.readCacheDebug;
import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.*;
import static com.macrosan.filesystem.cifs.SMB2Header.SMB2_HDR_FLAG_ASYNC;
import static com.macrosan.filesystem.cifs.SMB2Header.SMB2_HDR_FLAG_REDIRECT;
import static com.macrosan.filesystem.cifs.api.smb2.SetInfo.setInfoReturnErrorReply;
import static com.macrosan.filesystem.cifs.call.smb2.CreateCall.*;
import static com.macrosan.filesystem.cifs.call.smb2.GetInfoCall.*;
import static com.macrosan.filesystem.cifs.call.smb2.QueryDirCall.*;
import static com.macrosan.filesystem.cifs.handler.SMBHandler.localIpDebug;
import static com.macrosan.filesystem.cifs.reply.smb1.TreeConnectXReply.FILE_ALL_ACCESS;
import static com.macrosan.filesystem.cifs.reply.smb1.TreeConnectXReply.STANDARD_RIGHTS_ALL_ACCESS;
import static com.macrosan.filesystem.cifs.reply.smb2.CreateReply.*;
import static com.macrosan.filesystem.cifs.rpc.RPCConstants.*;
import static com.macrosan.filesystem.cifs.shareAccess.ShareAccessLock.FILE_CHECK_SHAREACCESS;
import static com.macrosan.filesystem.cifs.types.smb2.FileAttrTagInfo.FILE_ATTRIBUTE_READONLY;
import static com.macrosan.filesystem.cifs.types.smb2.SMB2FileId.isFileNeedDelete;
import static com.macrosan.filesystem.cifs.types.smb2.SMB2FileId.needDeleteSet;
import static com.macrosan.filesystem.cifs.types.smb2.pipe.CreatePipe.createVirtualRpcPipeInode;
import static com.macrosan.filesystem.quota.FSQuotaConstants.SMB_CAP_QUOTA_FILE_NAME;
import static com.macrosan.filesystem.utils.FsTierUtils.*;
import static com.macrosan.filesystem.utils.InodeUtils.isError;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;

@Log4j2
public class SMB2Proc {
    private static final SMB2Proc instance = new SMB2Proc();
    private static final Node nodeInstance = Node.getInstance();
    private static final String localNode = ServerConfig.getInstance().getHostUuid();

    private SMB2Proc() {
    }

    public static SMB2Proc getInstance() {
        return instance;
    }

    //处理create的子请求
    private static Mono<SMB2Reply> execContext(CreateCall call, CreateReply body, Inode inode, SMB2Header header, SMB2Reply reply, Session session) {
        return Flux.fromArray(call.getContext())
                .concatMap(context -> {
                    String t = new String(context.tag);
                    switch (t) {
                        case "DHnQ":
//                    CreateContext.CreateDurableHandle createDurableHandle = new CreateContext.CreateDurableHandle(0, context.tag);
//                    body.getContexts().add(createDurableHandle);
                            break;
                        case "QFid":
                            CreateContext.QueryFsId queryFsId = new CreateContext.QueryFsId(0, context.tag);
                            queryFsId.nodeId = inode.getNodeId();
                            queryFsId.volumeId = 0;
                            body.getContexts().add(queryFsId);
                            break;
                        case "MxAc":
                            CreateContext.MaxAccess maxAccess = new CreateContext.MaxAccess(0, context.tag);
                            maxAccess.maximalAccess = FILE_ALL_ACCESS | STANDARD_RIGHTS_ALL_ACCESS;
                            body.getContexts().add(maxAccess);
                            break;
                        case "DH2Q":
                            CreateContext.CreateDurableV2Handle createDurableHandleV2 = new CreateContext.CreateDurableV2Handle(0, context.tag);
                            createDurableHandleV2.flags = ((CreateContext.CreateDurableV2Handle) context).flags;
                            createDurableHandleV2.timeout = 300_000;
                            body.getContexts().add(createDurableHandleV2);
                            break;
                        case "AlSi": // 设置新创建或覆盖文件的分配大小, 不需要返回
                            CreateContext.CreateAllocationSize createAllocationSize = (CreateContext.CreateAllocationSize) context;
                            if (createAllocationSize.allocationSize != 0) {
                                SMB2FileId.FileInfo fileInfo = body.getFileId().getFileInfo(body.getFileId());
                                if (fileInfo != null) {
                                    fileInfo.allocationSize = createAllocationSize.allocationSize;
                                }
                            }
                            break;
                        case "RqLs":
                            if ((inode.getBucket() + "/" + inode.getObjName()).endsWith("/")) {
                                // 文件夹不支持
                                body.setOplockLevel(SMB2_OPLOCK_LEVEL_NONE);
                                break;
                            }
                            return CheckUtils.cifsLeaseOpenCheck()
                                    .flatMap(b -> {
                                        if (b) {
                                            // Lease
                                            if (context.dataSize == 32) {
                                                CreateContext.CreateRequestLease createRequestLease = (CreateContext.CreateRequestLease) context;

                                                if (createRequestLease.getLeaseKey().equalsIgnoreCase("ffffffffffffffffffffffffffffffff")) {
                                                    return Mono.just(false);
                                                }
                                                LeaseLock leaseLock = new LeaseLock(createRequestLease.getLeaseKey(), inode.getBucket(), inode.getObjName(), inode.getNodeId(),
                                                        createRequestLease.leaseState, body.getFileId(), 1, localNode);

                                                return leaseExec(leaseLock.bucket, String.valueOf(leaseLock.ino), leaseLock, call, header, body, session.getHandler())
                                                        .onErrorResume(e -> {
                                                            log.info("CIFS LeaseExec error {}", leaseLock, e);
                                                            return Mono.just(new LeaseLock());
                                                        })
                                                        .flatMap(leaseLock0 -> {
                                                            if (leaseLock0.leaseKey == null) {
                                                                return Mono.just(false);
                                                            }
                                                            CreateContext.CreateRequestLease createResponseLease = new CreateContext.CreateRequestLease(0, context.tag);
                                                            createResponseLease.leaseKey = createRequestLease.leaseKey;
                                                            createResponseLease.leaseState = leaseLock0.leaseState;
                                                            createResponseLease.leaseFlags = 0;

                                                            body.getContexts().add(createResponseLease);

                                                            if (leaseLock0.block && LeaseCache.blockToBreakMap.containsKey(body.getFileId())) {
                                                                LeaseCache.pendingMap.put(body.getFileId(), Tuples.of(session.getHandler(), reply));
                                                                LeaseCache.pendingTimeoutMap.put(body.getFileId(), System.nanoTime());
                                                                return Mono.just(true);
                                                            }
                                                            return Mono.just(false);
                                                        });
                                            }
                                            // Lease_V2
                                            else if (context.dataSize == 52) {
                                                CreateContext.CreateRequestLeaseV2 createRequestLeaseV2 = (CreateContext.CreateRequestLeaseV2) context;

                                                if (createRequestLeaseV2.getLeaseKey().equalsIgnoreCase("ffffffffffffffffffffffffffffffff")) {
                                                    return Mono.just(false);
                                                }
                                                LeaseLock leaseLock = new LeaseLock(createRequestLeaseV2.getLeaseKey(), inode.getBucket(), inode.getObjName(), inode.getNodeId(),
                                                        createRequestLeaseV2.leaseState, body.getFileId(), 2, localNode);

                                                return leaseExec(leaseLock.bucket, String.valueOf(leaseLock.ino), leaseLock, call, header, body, session.getHandler())
                                                        .onErrorResume(e -> {
                                                            log.info("CIFS LeaseExec error {}", leaseLock, e);
                                                            return Mono.just(new LeaseLock());
                                                        })
                                                        .flatMap(leaseLock0 -> {
                                                            if (leaseLock0.leaseKey == null) {
                                                                return Mono.just(false);
                                                            }
                                                            CreateContext.CreateRequestLeaseV2 createResponseLeaseV2 = new CreateContext.CreateRequestLeaseV2(0, context.tag);
                                                            createResponseLeaseV2.leaseKey = createRequestLeaseV2.leaseKey;
                                                            createResponseLeaseV2.leaseState = leaseLock0.leaseState;
                                                            createResponseLeaseV2.leaseFlags = 0;
                                                            createResponseLeaseV2.epoch = leaseLock0.epoch;

                                                            body.getContexts().add(createResponseLeaseV2);

                                                            if (leaseLock0.block && LeaseCache.blockToBreakMap.containsKey(body.getFileId())) {
                                                                LeaseCache.pendingMap.put(body.getFileId(), Tuples.of(session.getHandler(), reply));
                                                                LeaseCache.pendingTimeoutMap.put(body.getFileId(), System.nanoTime());

                                                                return Mono.just(true);
                                                            }
                                                            return Mono.just(false);
                                                        });
                                            }
                                            return Mono.just(false);
                                        } else {
                                            return Mono.just(false);
                                        }
                                    });
                        case "DHnC": //不需要返回
                        default:
                            break;
                    }
                    return Mono.just(false);
                })
                .collectList()
                .flatMap(list -> {
                    if (list.stream().anyMatch(b -> b)) {
                        return Mono.just(createPendingReply(header));
                    }
                    return Mono.just(reply);
                });
    }

    public static SMB2Reply createPendingReply(SMB2Header header) {
        SMB2Reply reply = new SMB2Reply(header);
        reply.getHeader()
                .setStatus(STATUS_PENDING)
                .setFlags(reply.getHeader().getFlags() | SMB2_HDR_FLAG_REDIRECT | SMB2_HDR_FLAG_ASYNC)
                .setCreditCharge((short) 0)
                .setCreditRequested((short) 0);
        ErrorReply errorReply = new ErrorReply();
        errorReply.setBytes(new byte[]{33});
        reply.setBody(errorReply);
        return reply;
    }

    private static Mono<LeaseLock> leaseExec(String bucket, String key, LeaseLock leaseLock, CreateCall call, SMB2Header header, CreateReply body, SMBHandler smbHandler) {

        return LeaseClient.lease(bucket, key, leaseLock)
                .flatMap(state -> {
                    leaseLock.leaseState = state;
                    if (state >= 0) {
                        LeaseCache.addCache(leaseLock.leaseOpens.iterator().next(), leaseLock, header, smbHandler);
                    }
                    return Mono.just(breakState(call))
                            .flatMap(breakState -> LeaseClient.breakLease(bucket, key, leaseLock.leaseKey, breakState, body.getFileId(), localNode))
                            .flatMap(block -> {
                                leaseLock.block = block;
                                return Mono.just(leaseLock);
                            });
                });

    }

    private static Mono<Boolean> shareAccessExec(CreateCall call, Inode inode, SMB2Header header, Session session, SMB2FileId smb2FileId) {
        if ((call.getAccess() & FILE_CHECK_SHAREACCESS) != 0) {
            if (!SMBHandler.localIpMap.containsKey(session.localIp)) {
                if (localIpDebug) {
                    log.info("add smb ip : {}", session.localIp);
                }
                SMBHandler.localIpMap.put(session.localIp, 0);
                SMBHandler.newIpMap.put(session.localIp, System.nanoTime());
            }
            ShareAccessLock shareAccessLock = new ShareAccessLock(inode, smb2FileId, call, header, localNode);
            return ShareAccessClient.lock(inode.getBucket(), String.valueOf(inode.getNodeId()), shareAccessLock, header)
                    .filter(b -> b || !SMBHandler.newIpMap.containsKey(session.localIp))
                    .switchIfEmpty(Mono.error(new Exception("newIp shareAccess false")))
                    .retryBackoff(2, Duration.ofSeconds(10), Duration.ofSeconds(10))
                    .onErrorReturn(false);
        } else {
            return Mono.just(true);
        }
    }

    private static Mono<Boolean> shareAccessReconnectExec(CreateCall call, Inode inode, SMB2Header header, Session session, SMB2FileId smb2FileId, SMB2FileId reconnectFileId) {
        if ((call.getAccess() & FILE_CHECK_SHAREACCESS) != 0) {
            if (!SMBHandler.localIpMap.containsKey(session.localIp)) {
                if (localIpDebug) {
                    log.info("add smb ip : {}", session.localIp);
                }
                SMBHandler.localIpMap.put(session.localIp, 0);
                SMBHandler.newIpMap.put(session.localIp, System.nanoTime());
            }
            ShareAccessLock shareAccessLock = new ShareAccessLock(inode, smb2FileId, call, header, localNode);
            ShareAccessLock reconnectLock = new ShareAccessLock(inode, reconnectFileId, call, header, localNode);
            return ShareAccessClient.reconnectLock(inode.getBucket(), String.valueOf(inode.getNodeId()), shareAccessLock, reconnectLock, header);
        } else {
            return Mono.just(true);
        }
    }

    private static int breakState(CreateCall call) {
        int state = 0b100;
        if (call.getCreateDisposition() == FILE_OVERWRITE || call.getCreateDisposition() == FILE_OVERWRITE_IF) {
            // overwrite打破所有 lease
            state = 0b111;
        }
        return state;
    }

    public static SMB2Reply createSharingViolationReply(SMB2Header header) {
        SMB2Reply reply = new SMB2Reply(header);
        reply.getHeader()
                .setStatus(STATUS_SHARING_VIOLATION)
                .setFlags(reply.getHeader().getFlags() | SMB2_HDR_FLAG_REDIRECT)
                .setCreditCharge((short) 1)
                .setCreditRequested((short) 2);
        ErrorReply errorReply = new ErrorReply();
        reply.setBody(errorReply);
        return reply;
    }

    private static Mono<SMB2Reply> shareAccessConflict(Inode inode, SMB2Header header, Session session, SMB2FileId smb2FileId) {
        return CheckUtils.cifsLeaseOpenCheck().flatMap(b -> {
            SMB2Reply sharingViolationReply = createSharingViolationReply(header);
            if (b) {
                return LeaseClient.breakLease(inode.getBucket(), String.valueOf(inode.getNodeId()), "", 0b010, smb2FileId, localNode)
                        .flatMap(block -> {
                            if (block) {
                                sharingViolationReply.getHeader().setFlags(sharingViolationReply.getHeader().getFlags() | SMB2_HDR_FLAG_ASYNC);
                                LeaseCache.pendingMap.put(smb2FileId, Tuples.of(session.getHandler(), sharingViolationReply));
                                LeaseCache.pendingTimeoutMap.put(smb2FileId, System.nanoTime());
                                return Mono.just(createPendingReply(header));
                            } else {
                                return Mono.just(sharingViolationReply);
                            }
                        });
            } else {
                return Mono.just(sharingViolationReply);
            }
        });

    }

    private static Mono<Boolean> closeRelease(SMB2FileId smb2FileId, String node, SMB2Header header) {
        return CheckUtils.cifsLeaseOpenCheck()
                .flatMap(b -> {
                    if (b) {
                        return LeaseCache.closeCache(smb2FileId, header);
                    } else {
                        return Mono.just(true);
                    }
                })
                .flatMap(b -> ShareAccessClient.unlock(smb2FileId, header))
                .doOnNext(b -> CIFSLockClient.close(smb2FileId, node, header));
    }

    @SMB2.Smb2Opt(value = SMB2_CREATE, allowIPCSession = true)
    public Mono<SMB2Reply> create(SMB2Header header, Session session, CreateCall call) {
        String obj = new String(call.getFileName()).replace("\\", "/");
        SMB2Reply reply = new SMB2Reply(header);
        CreateReply body = new CreateReply();

        body.setOplockLevel(call.getOplockLevel());

        // 提前判断是否是命名管道，todo 其余管道仍然返 STATUS_OBJECT_NAME_NOT_FOUND
        // tid=-1，管道类型 obj名字，
        int pipeClass = RpcPipeType.judgePipe(obj, header.tid);
        if (pipeClass != -1) {
            if (pipeClass == 1) {
                reply.getHeader().setStatus(STATUS_OBJECT_NAME_NOT_FOUND);
                return Mono.just(reply);
            } else {
                Optional<RpcPipeType> rpcTypeOpt = RpcPipeType.fromFileName(obj);
                RpcPipeType pipeType = rpcTypeOpt.get();
                long id0 = SMB2FileId.getId();
                Inode rpcInode = createVirtualRpcPipeInode(pipeType);
                boolean[] isLink = {false};
                Inode[] linkInode = new Inode[1];
                mapToCreateReply(body, rpcInode, FILE_OPEND, 1L, header.getCompoundRequest(), call.getCreateOptions(), null, id0, isLink[0], linkInode[0]);
                reply.setBody(body);
                reply.getHeader().setStatus(STATUS_SUCCESS);
                return execContext(call, body, rpcInode, header, reply, session);
            }
        }

        String bucket = NFSBucketInfo.getBucketName(header.tid);
        FSPerformanceService.addIp(bucket, header.getHandler().getClientAddress());
        boolean[] caseSensitive = {SMB2.caseSensitive};

        // 检查桶是否开启文件共享，若已关闭则返错；创建inode需要用到父目录的nodeId，因此先lookup父目录
        return CheckUtils.cifsOpenCheck(header.tid, session, caseSensitive)
                .flatMap(res -> {
                    if (!res) {
                        reply.getHeader().setStatus(STATUS_NETWORK_NAME_DELETED);
                        return Mono.just(reply);
                    }
                    log.debug("caseSensitive public:bucket={}:{}", SMB2.caseSensitive, caseSensitive[0]);

                    ReqInfo reqHeader = new ReqInfo();
                    reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(bucket);
                    reqHeader.bucket = bucket;

                    String s3Id = session.getTreeAccount(header.getTid());

                    return Mono.just(CifsUtils.getParentDirName(obj))
                            .flatMap(dirObj -> {
                                if (StringUtils.isEmpty(dirObj)) {
                                    return nodeInstance.getInode(bucket, 1);
                                } else {
                                    return LookupCache.lookup(bucket, dirObj, reqHeader, caseSensitive[0], false, 1, null);
                                }
                            })
                            .flatMap(dirInode -> {
                                if (dirInode.getLinkN() == ERROR_INODE.getLinkN()) {
                                    log.info("get inode fail.bucket:{}, obj:{}: {}", reqHeader.bucket, obj, dirInode.getLinkN());
                                    reply.getHeader().setStatus(STATUS_IO_DEVICE_ERROR);
                                    return Mono.just(reply);
                                }

                                if (dirInode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                    log.info("get inode fail.bucket:{}, obj:{}: {}", reqHeader.bucket, obj, dirInode.getLinkN());
                                    reply.getHeader().setStatus(STATUS_OBJECT_NAME_NOT_FOUND);
                                    return Mono.just(reply);
                                }

                                //dirInode.objName和dirObj可能不一致
                                String realObj = dirInode.getObjName() + obj.substring(dirInode.getObjName().length());
                                Mono<Inode> inodeMono;
                                if (realObj.length() == 0) {
                                    inodeMono = Mono.just(dirInode);
                                } else {
                                    ReqInfo reqInfo = new ReqInfo();
                                    reqInfo.bucket = bucket;
                                    reqInfo.bucketInfo = reqHeader.bucketInfo;
                                    inodeMono = LookupCache.lookup(bucket, realObj, reqInfo, caseSensitive[0], false, dirInode.getNodeId(), dirInode.getACEs());
                                }

                                boolean[] isLink = {false};
                                Inode[] linkInode = new Inode[1];
                                return inodeMono
                                        .flatMap(inode -> {
                                            if ((inode.getMode() & S_IFMT) == S_IFLNK) {
                                                //如果是nfs软链接
                                                isLink[0] = true;
                                                return FsUtils.lookup(bucket, inode.getReference(), null, false, dirInode.getNodeId(), null)
                                                        .flatMap(inode0 -> {
                                                            linkInode[0] = inode0;
                                                            return Mono.just(inode);
                                                        });
                                            }
                                            return Mono.just(inode);
                                        })
                                        .flatMap(inode -> {
                                            int cifsMode = CifsUtils.getCIFSMode(call.getCreateOptions(), call.getMode());
                                            int mode = CifsUtils.getNFSMode(call.getCreateOptions());

                                            if (inode.getNodeId() > 0) {
                                                int inodeSmb2Mode = inode.getCifsMode();

                                                boolean isReadOnly = (inodeSmb2Mode & FILE_ATTRIBUTE_READONLY) != 0;
                                                if (isReadOnly) {
                                                    long accessMask = call.getAccess();

                                                    // 检查是否有写权限
                                                    if ((accessMask & FILE_WRITE_ACCESS) != 0) {
                                                        reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                                        return Mono.just(reply);
                                                    }
                                                }
                                            }

                                            boolean[] isReconnect = {false};
                                            SMB2FileId[] oldFileID = new SMB2FileId[1];
                                            for (CreateContext createContext : call.getContext()) {
                                                String t = new String(createContext.tag);
                                                if (t.equals("DH2C") || t.equals("DHnC")) {
                                                    isReconnect[0] = true;
                                                    oldFileID[0] = ((CreateContext.CreateDurableReConnectV2Handle) createContext).oldFileID;
                                                    break;
                                                }
                                            }

                                            return (isReconnect[0] ? WriteCacheClient.flush(inode, 0, 0) : Mono.just(true))
                                                    .flatMap(b -> {
                                                        switch (call.getCreateDisposition()) {
                                                            // File exists open. File not exist fail | mount
                                                            case FILE_OPEN:
                                                                // File exists open. File not exist create | dd 指令
                                                            case FILE_OPEN_IF:
                                                                if (!InodeUtils.isError(inode) && inode.getNodeId() > 0) {
                                                                    if ((call.getCreateOptions() & FILE_DIRECTORY_FILE) != 0 && (inode.getCifsMode() & FILE_ATTRIBUTE_DIRECTORY) == 0) {
                                                                        reply.getHeader().setStatus(STATUS_NOT_A_DIRECTORY);
                                                                        return Mono.just(reply);
                                                                    }

                                                                    return CIFSACL.judgeCIFSAccess(call.getAccess(), inode, dirInode, s3Id, bucket, call.getCreateDisposition(), call.getCreateOptions(), true, header, session)
                                                                            .flatMap(pass -> {
                                                                                if (!pass) {
                                                                                    reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                                                                                    return Mono.just(reply);
                                                                                }

                                                                                // FILE_OPEN 不需要加锁，避免文件删除、重命名时也进行加锁；设置删除needDelSet需要在权限判断之后，避免误删文件
                                                                                long id0 = SMB2FileId.getId();
                                                                                SMB2FileId smb2FileId = new SMB2FileId()
                                                                                        .setPersistent(id0)
                                                                                        .setVolatile_(0);
                                                                                return (isReconnect[0] && oldFileID[0] != null ? shareAccessReconnectExec(call, inode, header, session, smb2FileId, oldFileID[0]) : shareAccessExec(call, inode, header, session, smb2FileId))
                                                                                        .flatMap(ShareAccessCheck -> {
                                                                                            if (ShareAccessCheck) {
                                                                                                CreateReply.mapToCreateReply(body, inode, FILE_OPEND, dirInode.getNodeId(), header.getCompoundRequest(), call.getCreateOptions(), dirInode.getACEs(), id0, isLink[0], linkInode[0]);
                                                                                                reply.setBody(body);
                                                                                                reply.getHeader().setStatus(STATUS_SUCCESS);
                                                                                                return execContext(call, body, inode, header, reply, session);
                                                                                            } else {
                                                                                                return shareAccessConflict(inode, header, session, smb2FileId);
                                                                                            }
                                                                                        });
                                                                            });

                                                                } else {
                                                                    // inode获取出错
                                                                    if (inode.getLinkN() == Inode.ERROR_INODE.getLinkN()) {
                                                                        log.info("get inode fail.bucket:{}, obj:{}: {}", reqHeader.bucket, obj, dirInode.getLinkN());
                                                                        reply.getHeader().setStatus(STATUS_IO_DEVICE_ERROR);
                                                                        return Mono.just(reply);
                                                                    }

                                                                    // 不存在
                                                                    if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                                                        if (realObj.endsWith(SMB_CAP_QUOTA_FILE_NAME)) {
                                                                            Inode tmpInode = inode.clone();
                                                                            body.setCreateAction(FILE_OPEND);
                                                                            body.setMode(FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM | FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_ARCHIVE);
                                                                            tmpInode.setNodeId(0);
                                                                            tmpInode.setObjName(realObj);
                                                                            tmpInode.setLinkN(0);
                                                                            tmpInode.setBucket(bucket);
                                                                            body.setFileId(SMB2FileId.randomFileId(header.getCompoundRequest(), bucket, realObj, inode.getNodeId(), dirInode.getNodeId(), call.getCreateOptions(), tmpInode, dirInode.getACEs()));
                                                                            reply.setBody(body);
                                                                            reply.getHeader().setStatus(STATUS_SUCCESS);
                                                                            return Mono.just(reply);
                                                                        }
                                                                        // 返错
                                                                        if (call.getCreateDisposition() == FILE_OPEN) {
                                                                            reply.getHeader().setStatus(STATUS_OBJECT_NAME_NOT_FOUND);
                                                                            return Mono.just(reply);
                                                                        } else {
                                                                            // 创建；vi文件后wq会走这里；vdbench的write操作会首先创建文件
                                                                            body.setCreateAction(FILE_OPEN);
                                                                            return CIFSACL.judgeCIFSAccess(call.getAccess(), dirInode, dirInode, s3Id, bucket, call.getCreateDisposition(), call.getCreateOptions(), false, header, session)
                                                                                    .flatMap(pass -> {
                                                                                        if (!pass) {
                                                                                            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                                                                                            return Mono.just(reply);
                                                                                        }

                                                                                        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_create.name() + "-" + THROUGHPUT_QUOTA, 1L)
                                                                                                .flatMap(waitMillis -> {
                                                                                                    log.debug("craete 1, {}", waitMillis);
                                                                                                    String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), reqHeader.bucket);
                                                                                                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_create.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                                                                                                })
                                                                                                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis))
                                                                                                        .flatMap(l -> InodeUtils.smbCreate(reqHeader, dirInode, mode, cifsMode, realObj, body, reply,
                                                                                                                header.getCompoundRequest(), call.getCreateOptions(), s3Id)))
                                                                                                .flatMap(i -> {
                                                                                                    if (InodeUtils.isError(i)) {
                                                                                                        return Mono.just(reply);
                                                                                                    }
                                                                                                    return shareAccessExec(call, i, header, session, body.getFileId())
                                                                                                            .flatMap(ShareAccessCheck -> {
                                                                                                                if (ShareAccessCheck) {
                                                                                                                    return execContext(call, body, i, header, reply, session);
                                                                                                                } else {
                                                                                                                    return shareAccessConflict(i, header, session, body.getFileId());
                                                                                                                }
                                                                                                            });
                                                                                                });
                                                                                    });
                                                                        }
                                                                    }

                                                                    return Mono.just(reply);
                                                                }
                                                                // File exists fail. File not exist create
                                                            case FILE_CREATE:
                                                                // inode 存在则返错，返错时需要将已存在的inode信息返回
                                                                if (!InodeUtils.isError(inode) && inode.getNodeId() > 0) {
                                                                    if (isReconnect[0] && oldFileID[0] != null) {
                                                                        long id0 = SMB2FileId.getId();
                                                                        SMB2FileId smb2FileId = new SMB2FileId()
                                                                                .setPersistent(id0)
                                                                                .setVolatile_(0);
                                                                        return shareAccessReconnectExec(call, inode, header, session, smb2FileId, oldFileID[0])
                                                                                .flatMap(ShareAccessCheck -> {
                                                                                    if (ShareAccessCheck) {
                                                                                        CreateReply.mapToCreateReply(body, inode, FILE_CREATED, inode.getNodeId(), header.getCompoundRequest(), call.getCreateOptions(), dirInode.getACEs(), id0, isLink[0], linkInode[0]);
                                                                                        reply.setBody(body);
                                                                                        return execContext(call, body, inode, header, reply, session);
                                                                                    } else {
                                                                                        return shareAccessConflict(inode, header, session, smb2FileId);
                                                                                    }
                                                                                });
                                                                    } else {
                                                                        reply.getHeader().setStatus(STATUS_OBJECT_NAME_COLLISION);
                                                                        return Mono.just(reply);
                                                                    }
                                                                } else {
                                                                    // 不存在则创建
                                                                    if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                                                        body.setCreateAction(FILE_CREATE);

                                                                        return CIFSACL.judgeCIFSAccess(call.getAccess(), dirInode, dirInode, s3Id, bucket, call.getCreateDisposition(), call.getCreateOptions(), false, header, session)
                                                                                .flatMap(pass -> {
                                                                                    if (!pass) {
                                                                                        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                                                                                        return Mono.just(reply);
                                                                                    }

                                                                                    return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_mkdir.name() + "-" + THROUGHPUT_QUOTA, 1L)
                                                                                            .flatMap(waitMillis -> {
                                                                                                log.debug("craete 2, {}", waitMillis);
                                                                                                String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), reqHeader.bucket);
                                                                                                return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_mkdir.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                                                                                            })
                                                                                            .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis))
                                                                                                    .flatMap(l -> InodeUtils.smbCreate(reqHeader, dirInode, mode, cifsMode, realObj, body, reply, header.getCompoundRequest()
                                                                                                            , call.getCreateOptions(), s3Id)))
                                                                                            .flatMap(i -> {
                                                                                                if (InodeUtils.isError(i)) {
                                                                                                    return Mono.just(reply);
                                                                                                }
                                                                                                return shareAccessExec(call, i, header, session, body.getFileId())
                                                                                                        .flatMap(ShareAccessCheck -> {
                                                                                                            if (ShareAccessCheck) {
                                                                                                                return execContext(call, body, i, header, reply, session);
                                                                                                            } else {
                                                                                                                return shareAccessConflict(i, header, session, body.getFileId());
                                                                                                            }
                                                                                                        });
                                                                                            });
                                                                                });
                                                                    }

                                                                    // 如果是inode获取出错
                                                                    if (inode.getLinkN() == Inode.ERROR_INODE.getLinkN()) {
                                                                        log.info("get inode fail.bucket:{}, obj:{}: {}", reqHeader.bucket, obj, dirInode.getLinkN());
                                                                        reply.getHeader().setStatus(STATUS_IO_DEVICE_ERROR);
                                                                    }

                                                                    return Mono.just(reply);
                                                                }

                                                                // File exists overwrite. File not exist fail
                                                            case FILE_OVERWRITE:
                                                                // File exists overwrite. File not exist create
                                                            case FILE_OVERWRITE_IF:
                                                                // 存在则覆盖；此option会将原本文件的数据块删除，返回的size为0
                                                                if (!InodeUtils.isError(inode) && inode.getNodeId() > 0) {
                                                                    if (isReconnect[0] && oldFileID[0] != null) {
                                                                        long id0 = SMB2FileId.getId();
                                                                        SMB2FileId smb2FileId = new SMB2FileId()
                                                                                .setPersistent(id0)
                                                                                .setVolatile_(0);
                                                                        return shareAccessReconnectExec(call, inode, header, session, smb2FileId, oldFileID[0])
                                                                                .flatMap(ShareAccessCheck -> {
                                                                                    if (ShareAccessCheck) {
                                                                                        CreateReply.mapToCreateReply(body, inode, call.getCreateDisposition(), inode.getNodeId(), header.getCompoundRequest(),
                                                                                                call.getCreateOptions(), dirInode.getACEs(), id0, isLink[0], linkInode[0]);
                                                                                        reply.setBody(body);
                                                                                        return execContext(call, body, inode, header, reply, session);
                                                                                    } else {
                                                                                        return shareAccessConflict(inode, header, session, smb2FileId);
                                                                                    }
                                                                                });
                                                                    }
                                                                    ObjAttr attr = new ObjAttr();
                                                                    attr.hasSize = 1;
                                                                    attr.size = 0;
                                                                    if (cifsMode != 0) {
                                                                        attr.hasCifsMode = 1;
                                                                        attr.cifsMode = cifsMode;
                                                                    }

                                                                    return CIFSACL.judgeCIFSAccess(call.getAccess(), inode, dirInode, s3Id, bucket, call.getCreateDisposition(), call.getCreateOptions(), true, header, session)
                                                                            .flatMap(pass -> {
                                                                                if (!pass) {
                                                                                    reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                                                                                    return Mono.just(reply);
                                                                                }

                                                                                long id0 = SMB2FileId.getId();
                                                                                SMB2FileId smb2FileId = new SMB2FileId()
                                                                                        .setPersistent(id0)
                                                                                        .setVolatile_(0);
                                                                                return shareAccessExec(call, inode, header, session, smb2FileId)
                                                                                        .flatMap(ShareAccessCheck -> {
                                                                                            if (ShareAccessCheck) {
                                                                                                return nodeInstance.setAttr(inode.getNodeId(), bucket, attr, reqHeader.bucketInfo.get(BUCKET_USER_ID))
                                                                                                        .flatMap(i -> {
                                                                                                            CreateReply.mapToCreateReply(body, i, FILE_OVERWRITTEN, dirInode.getNodeId(), header.getCompoundRequest(), call.getCreateOptions(), dirInode.getACEs(), id0, isLink[0], linkInode[0]);
                                                                                                            reply.setBody(body);
                                                                                                            reply.getHeader().setStatus(STATUS_SUCCESS);
                                                                                                            return execContext(call, body, i, header, reply, session);
                                                                                                        });
                                                                                            } else {
                                                                                                return shareAccessConflict(inode, header, session, smb2FileId);
                                                                                            }
                                                                                        });
                                                                            });
                                                                } else {
                                                                    // inode获取出错
                                                                    if (inode.getLinkN() == Inode.ERROR_INODE.getLinkN()) {
                                                                        log.info("get inode fail.bucket:{}, obj:{}: {}", reqHeader.bucket, obj, dirInode.getLinkN());
                                                                        reply.getHeader().setStatus(STATUS_IO_DEVICE_ERROR);
                                                                        return Mono.just(reply);
                                                                    }

                                                                    // 不存在
                                                                    if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                                                        // 返错
                                                                        if (call.getCreateDisposition() == FILE_OVERWRITE) {
                                                                            reply.getHeader().setStatus(STATUS_NO_SUCH_FILE);
                                                                        } else {
                                                                            // 创建
                                                                            // 直接将整个文件上传，走这里
                                                                            body.setCreateAction(FILE_OPEND);

                                                                            return CIFSACL.judgeCIFSAccess(call.getAccess(), dirInode, dirInode, s3Id, bucket, call.getCreateDisposition(), call.getCreateOptions(), false, header, session)
                                                                                    .flatMap(pass -> {
                                                                                        if (!pass) {
                                                                                            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                                                                                            return Mono.just(reply);
                                                                                        }

                                                                                        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_create.name() + "-" + THROUGHPUT_QUOTA, 1L)
                                                                                                .flatMap(waitMillis -> {
                                                                                                    log.debug("craete 3, {}", waitMillis);
                                                                                                    String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), reqHeader.bucket);
                                                                                                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_create.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                                                                                                })
                                                                                                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis))
                                                                                                        .flatMap(l -> InodeUtils.smbCreate(reqHeader, dirInode, mode, cifsMode, realObj, body, reply,
                                                                                                                header.getCompoundRequest(), call.getCreateOptions(), s3Id)))
                                                                                                // share
                                                                                                .flatMap(i -> {
                                                                                                    if (InodeUtils.isError(i)) {
                                                                                                        return Mono.just(reply);
                                                                                                    }
                                                                                                    return shareAccessExec(call, i, header, session, body.getFileId())
                                                                                                            .flatMap(ShareAccessCheck -> {
                                                                                                                if (ShareAccessCheck) {
                                                                                                                    return execContext(call, body, i, header, reply, session);
                                                                                                                } else {
                                                                                                                    return shareAccessConflict(i, header, session, body.getFileId());
                                                                                                                }
                                                                                                            });
                                                                                                });
                                                                                    });
                                                                        }
                                                                    }

                                                                    return Mono.just(reply);
                                                                }
                                                                // File exists overwrite/supersede. File not exist create
                                                            case FILE_SUPERSEDE:
                                                            default:
                                                                reply.getHeader().setStatus(STATUS_NOT_IMPLEMENTED);
                                                                return Mono.just(reply);
                                                        }
                                                    });
                                        });
                            });
                });


    }

    @SMB2.Smb2Opt(value = SMB2_GETINFO, allowIPCSession = true)
    public Mono<SMB2Reply> getInfo(SMB2Header header, Session session, GetInfoCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        reply.getHeader().setCompoundRequest(header.getCompoundRequest());
        switch (call.getInfoType()) {
            case SMB2_0_INFO_FILE:
                return GetInfo.getFileInfo(reply, header, call, session);
            case SMB2_0_INFO_FILESYSTEM:
                return GetInfo.getFsInfo(reply, session, call);
            case SMB2_0_INFO_QUOTA:
                return GetInfo.getQuotaInfo(reply, header, call);
            case SMB2_0_INFO_SECURITY:
                return GetInfo.getSecurityInfo(reply, header, call);
            default:
                reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                return Mono.just(reply);
        }
    }

    @SMB2.Smb2Opt(value = SMB2_WRITE, allowIPCSession = true)
    public Mono<SMB2Reply> write(SMB2Header header, Session session, WriteCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        WriteReply body = new WriteReply();
        SMB2FileId.FileInfo fileInfo = call.fileId.getFileInfo(header.getCompoundRequest());
        if (fileInfo == null) {
            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_FILE_CLOSED);
            return Mono.just(reply);
        }
        SMB2FileId.updateFileInfo(header.getCompoundRequest(), call.fileId, fileInfo);

        Optional<RpcPipeType> rpcTypeOpt = RpcPipeType.fromFileName(fileInfo.obj);

        if (rpcTypeOpt.isPresent()) {
            BindPduInfo bindPdu = new BindPduInfo();
            byte[] data = call.data;

            if (data == null || data.length < 28) {
                log.debug("SMB2_WRITE 数据区太短，无法解析 Bind PDU: length={}, expected >=28",
                        data == null ? 0 : data.length);
                body.structSize = 0x0011;
                body.writeCount = 0;
                reply.setBody(body);
                return Mono.just(reply);
            }

            ByteBuf buf = Unpooled.wrappedBuffer(data);
            byte pduType = buf.getByte(2);
            if (pduType == BIND){
                bindPdu.readStruct(buf, 0);
                fileInfo.bindPduInfo = bindPdu;
            } else if (pduType == REQUEST) {
                IOCTLSubCall.pipeTrans subCall = new IOCTLSubCall.pipeTrans();
                subCall.readStruct(buf, 0);
                RpcRequestCall rpcRequestCall = subCall.rpcRequestCall;
                fileInfo.rpcRequestCall = rpcRequestCall;
            }
            body.structSize = 0x0011;
            body.writeCount = call.dataLength;
            reply.setBody(body);
            return Mono.just(reply);
        }
        String bucket = NFSBucketInfo.getBucketName(header.tid);
        FSPerformanceService.addIp(bucket, header.getHandler().getClientAddress());
        if (CheckUtils.checkIfOverFlow(call.writeOffset, call.dataLength)) {
            reply.getHeader().setStatus(STATUS_IO_DEVICE_ERROR);
            return Mono.just(reply);
        }

        SMB2FileId smb2FileId = (header.getCompoundRequest() != null && header.getCompoundRequest().getFileId() != null) ? header.getCompoundRequest().getFileId() : call.fileId;

        SMB2FileId.updateFileInfo(header.getCompoundRequest(), call.fileId, fileInfo);
        return BucketFSPerfLimiter.getInstance().limits(bucket, fs_write.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_write.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> Mono.just(fileInfo.openInode)))
                .flatMap(inode -> {
                    if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_OBJECT_NAME_NOT_FOUND);
                        return Mono.just(reply);
                    }
                    if (inode.getLinkN() == Inode.ERROR_INODE.getLinkN()) {
                        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_IO_DEVICE_ERROR);
                        log.info("fail inode failed.bucketName:{},objName:{}", bucket, fileInfo.obj);
                        return Mono.just(reply);
                    }

                    Mono<Long> qosMono = BucketFSPerfLimiter.getInstance().limits(bucket, fs_write.name() + "-" + BAND_WIDTH_QUOTA, call.dataLength)
                            .flatMap(waitMillis -> {
                                String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), bucket);
                                return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_write.name() + "-" + BAND_WIDTH_QUOTA, call.dataLength).map(waitMillis2 -> waitMillis + waitMillis2);
                            })
                            .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)));

                    Mono<Integer> writeRes;
                    AtomicReference<WriteCache> writeCacheReference = new AtomicReference<>();
                    if (!fileInfo.abandon.get()) {
                        if (header.getHandler().negprotInfo.getDialect() >= NegprotCall.SMB_3_0_0) {
//                            call.writeFlags |= SMB2_WRITEFLAG_WRITE_THROUGH;
                            writeRes = qosMono.flatMap(aLong -> WriteCacheClient.cifsWrite(smb2FileId, call.writeOffset, call.data, inode, call.writeFlags, fileInfo))
                                    .map(success -> success ? 0 : 1);
                        } else {
                            fileInfo.updateTimeOnClose = true;
                            writeRes = qosMono
                                    .flatMap(q -> FSQuotaUtils.checkFsQuota(inode))
                                    .flatMap(l -> WriteCache.getCache(bucket, inode.getNodeId(), call.writeFlags, inode.getStorage(), true))
                                    .flatMap(writeCache -> {
                                        writeCacheReference.set(writeCache);
                                        return writeCache.nfsWrite(call.writeOffset, call.data, inode, call.writeFlags)
                                                .map(success -> success ? 0 : 1);
                                    });
                        }
                    } else {
                        //上传过程中文件被删，后续数据块不再写入，但返回成功
                        writeRes = Mono.just(0);
                    }

                    return writeRes.flatMap(res -> {
                        if (res == 1) {
                            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_IO_DEVICE_ERROR);
                            return Mono.just(reply);
                        } else {
                            String leaseKey = LeaseCache.getLeaseKey(call.fileId);
                            CheckUtils.cifsLeaseOpenCheck().subscribe(b -> {
                                if (b) {
                                    LeaseClient.breakLease(bucket, String.valueOf(fileInfo.inodeId), leaseKey, 0b011, call.fileId, localNode).subscribe();
                                }
                            });

                            if (null != writeCacheReference.get()) {
                                long writeNum = writeCacheReference.get().getWriteNum();
                                if (writeNum > 160) {
                                    if (SMBHandler.runningDebug) {
                                        log.info("cifs:,write pending =============,writeNum:{}", writeNum);
                                    }
                                    reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_PENDING);
                                    return Mono.just(reply);
                                }
                            }

                            body.structSize = 0x0011;
                            body.writeCount = call.dataLength;
                            reply.setBody(body);
                            return Mono.just(reply);
                        }
                    });
                });
    }

    @SMB2.Smb2Opt(SMB2_SETINFO)
    public Mono<SMB2Reply> setInfo(SMB2Header header, Session session, SetInfoCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        switch (call.getInfoType()) {
            case SMB2_0_INFO_FILE:
                return SetInfo.setFileInfo(header, reply, session, call, header.getCompoundRequest());
            case SMB2_0_INFO_FILESYSTEM:
                return SetInfo.setFsInfo(reply, session, call, header);
            case SMB2_0_INFO_QUOTA:
                return SetInfo.setQuotaInfo(reply, session, call, header);
            case SMB2_0_INFO_SECURITY:
                return SetInfo.setSecurityInfo(reply, session, call, header);
            default:
                reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
                return Mono.just(reply);
        }
    }

    @SMB2.Smb2Opt(value = SMB2_READ, allowIPCSession = true)
    public Mono<SMB2Reply> read(SMB2Header header, Session session, ReadCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        SMB2Header replyHeader = reply.getHeader();
        ReadReply body = new ReadReply();

        SMB2FileId.FileInfo fileInfo = call.fileId.getFileInfo(header.getCompoundRequest());
        if (fileInfo == null) {
            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_FILE_CLOSED);
            return Mono.just(reply);
        }
        SMB2FileId.updateFileInfo(header.getCompoundRequest(), call.fileId, fileInfo);

        Optional<RpcPipeType> rpcTypeOpt = RpcPipeType.fromObjName(fileInfo.obj);
        if (rpcTypeOpt.isPresent()) {
            RpcPipeType pipeType = rpcTypeOpt.get();
            byte pduType;

            BindPduInfo bindPduInfo = fileInfo.bindPduInfo;
            RpcRequestCall rpcRequestCall = fileInfo.rpcRequestCall;
            if (bindPduInfo != null){
                pduType = bindPduInfo.getPduType();
            } else if (rpcRequestCall != null) {
                pduType = rpcRequestCall.header.packetType;
            } else {
                body.dataLen = 0;
                body.data = new byte[0];
                reply.setBody(body);
                reply.getHeader().setStatus(STATUS_SUCCESS);
                return Mono.just(reply);
            }

            byte[] response;

            if (pduType == BIND) {
                response = RpcBindGenerator.generateBindAck(bindPduInfo, pipeType);
                fileInfo.bindPduInfo = null;
            } else if (pduType == ALTER_CONTEXT) {
                response = RpcBindGenerator.generateAlterContextResp(bindPduInfo, pipeType);
                fileInfo.bindPduInfo = null;
            } else if (pduType == REQUEST) {
                response = RpcBindGenerator.generateRequestPdu(rpcRequestCall, fileInfo);
                fileInfo.rpcRequestCall = null;
            } else {
                log.debug("Unexpected pduType in cache: 0x{:02X} for pipe: {}", pduType, fileInfo.obj);
                response = new byte[0];
            }

            body.dataLen = response.length;
            body.data = response;
            reply.setBody(body);
            return Mono.just(reply);
        }

        String bucket = NFSBucketInfo.getBucketName(header.tid);
        FSPerformanceService.addIp(bucket, header.getHandler().getClientAddress());
        body.flags = call.flags;
        Inode[] accessInode = new Inode[1];
        SMB2FileId smb2FileId = (header.getCompoundRequest() != null && header.getCompoundRequest().getFileId() != null) ? header.getCompoundRequest().getFileId() : call.fileId;
        return BucketFSPerfLimiter.getInstance().limits(bucket, fs_read.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_read.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> nodeInstance.getInode(fileInfo.bucket, fileInfo.inodeId)))
                .flatMap(inode -> {
                    if ((inode.getMode() & S_IFMT) == S_IFLNK) {
                        //如果是nfs软链接
                        return FsUtils.lookup(bucket, inode.getReference(), null, false, fileInfo.getDirInode(), null);
                    }

                    return Mono.just(inode);
                })
                .flatMap(inode -> {
                    if (inode.getLinkN() > 0) {
                        boolean isSMB3 = header.getHandler().negprotInfo.getDialect() >= NegprotCall.SMB_3_0_0;

                        return (isSMB3
                                ? WriteCacheClient.flush(inode, 0, 0, smb2FileId)
                                : WriteCache.getCache(fileInfo.bucket, inode.getNodeId(), 0, inode.getStorage(), true).flatMap(cache -> cache.nfsCommit(inode, 0, 0)))
                                .flatMap(b -> {
                                    if (b) {
                                        return nodeInstance.getInode(fileInfo.bucket, inode.getNodeId());
                                    } else {
                                        log.error("commit fail before read.inode:{}", inode);
                                        return Mono.just(Inode.ERROR_INODE);
                                    }
                                });
                    }

                    return Mono.just(inode);
                })
                .flatMap(inode -> {
                    if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_OBJECT_NAME_NOT_FOUND);
                        body.data = new byte[0];
                        reply.setBody(body);
                        return Mono.just(reply);
                    }
                    if (inode.getLinkN() == Inode.ERROR_INODE.getLinkN()) {
                        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_IO_DEVICE_ERROR);
                        body.data = new byte[0];
                        reply.setBody(body);
                        log.info("fail inode failed.bucketName:{},objName:{}", bucket, fileInfo.obj);
                        return Mono.just(reply);
                    }
                    accessInode[0] = inode.clone();
                    MonoProcessor<SMB2Reply> res = MonoProcessor.create();
                    List<Inode.InodeData> inodeData = inode.getInodeData();
                    if (inodeData.isEmpty()) {
                        if (inode.getSize() <= 0) {//处理0kb大小的文件
                            replyHeader.status = STATUS_SUCCESS;
                        } else {
                            replyHeader.status = STATUS_IO_DEVICE_ERROR;
                        }
                        body.data = new byte[0];
                        reply.setBody(body);
                        return Mono.just(reply);
                    }

                    long cur = 0;
                    long readOffset = call.readOffset;
                    long readEnd = call.readOffset + call.length;
                    long inodeEnd = inode.getSize();
                    if (readEnd >= inodeEnd) {
                        readEnd = inodeEnd;
                        call.length = (int) (readEnd - call.readOffset);
                    }

                    if (call.length <= 0) {
                        replyHeader.status = STATUS_SUCCESS;
                        body.data = new byte[0];
                        reply.setBody(body);
                        return Mono.just(reply);
                    }

                    if (inode.getNodeId() < VersionUtil.V3_0_6_FS_NODE_ID) {
                        //旧数据走旧读流程
                        int readN = 0;
                        byte[] bytes = new byte[call.length];

                        Flux<Tuple2<Integer, byte[]>> flux = Flux.empty();

                        for (Inode.InodeData data : inodeData) {
                            long curEnd = cur + data.size;
                            if (readEnd < cur) {
                                break;
                            } else if (readOffset > curEnd) {
                                cur = curEnd;
                            } else {
                                long readFileOffset = data.offset + (readOffset - cur);
                                int readFileSize = (int) (Math.min(readEnd, curEnd) - readOffset);

                                flux = flux.mergeWith(FsUtils.readObj(readN, data.storage, bucket,
                                        data.fileName, readFileOffset, readFileSize, data.size));

                                readOffset += readFileSize;
                                readN += readFileSize;

                                if (readOffset >= readEnd) {
                                    break;
                                }

                                cur = curEnd;
                            }
                        }
                        flux.flatMap(t -> BucketFSPerfLimiter.getInstance().limits(bucket, fs_read.name() + "-" + BAND_WIDTH_QUOTA, t.var2.length)
                                        .flatMap(waitMillis -> {
                                            String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), bucket);
                                            return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_read.name() + "-" + BAND_WIDTH_QUOTA, t.var2.length).map(waitMillis2 -> waitMillis + waitMillis2);
                                        })
                                        .flatMap(waitMillis -> Mono.just(t).delayElement(Duration.ofMillis(waitMillis))))
                                .doOnNext(t -> {
                                    System.arraycopy(t.var2, 0, bytes, t.var1, t.var2.length);
                                })
                                .doOnError(e -> {
                                    log.error("", e);
                                    replyHeader.status = STATUS_IO_DEVICE_ERROR;
                                    body.data = new byte[0];
                                    reply.setBody(body);
                                    res.onNext(reply);
                                })
                                .doOnComplete(() -> {
                                    replyHeader.status = STATUS_SUCCESS;
                                    body.data = bytes;
                                    body.dataLen = bytes.length;
                                    reply.setBody(body);
                                    InodeUtils.updateInodeAtime(inode);
                                    res.onNext(reply);
                                }).subscribe();

                        return res;
                    } else {
                        //新数据走新读流程
                        return BucketFSPerfLimiter.getInstance().limits(bucket, fs_read.name() + "-" + BAND_WIDTH_QUOTA, call.length)
                                .flatMap(waitMillis -> {
                                    String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), bucket);
                                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_read.name() + "-" + BAND_WIDTH_QUOTA, call.length).map(waitMillis2 -> waitMillis + waitMillis2);
                                })
                                .flatMap(waitMillis -> waitMillis == 0 ? Mono.just(true) : Mono.delay(Duration.ofMillis(waitMillis)))
                                .flatMapMany(l -> ReadObjCache.readObj(inode, call.readOffset, call.length))
                                .collectList()
                                .map(byteList -> {
                                    byte[] bytes;
                                    int size = 0;
                                    if (byteList.size() == 1) {
                                        bytes = byteList.get(0).var2;
                                        size = bytes.length;
                                    } else {
                                        bytes = new byte[call.length];
                                        for (Tuple2<Integer, byte[]> tuple : byteList) {
                                            System.arraycopy(tuple.var2, 0, bytes, tuple.var1, tuple.var2.length);
                                            size += tuple.var2.length;
                                        }
                                    }

                                    if (size != call.length) {
                                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "read size not match");
                                    }

                                    replyHeader.status = STATUS_SUCCESS;
                                    body.data = bytes;
                                    body.dataLen = bytes.length;
                                    reply.setBody(body);
                                    InodeUtils.updateInodeAtime(inode);

                                    return reply;
                                })
                                .onErrorResume(e -> {
                                    if (null != e && null != e.getMessage() && e.getMessage().contains("pre-read data modified")) {
                                        if (readCacheDebug) {
                                            log.error("", e);
                                        }
                                    } else {
                                        log.error("", e);
                                    }

                                    replyHeader.status = STATUS_IO_DEVICE_ERROR;
                                    body.data = new byte[0];
                                    reply.setBody(body);
                                    return Mono.just(reply);
                                });
                    }
                })
                .doFinally(r -> {
                    fileFsAccessHandle(accessInode[0]);
                });
    }

    @SMB2.Smb2Opt(value = SMB2_CLOSE, allowIPCSession = true)
    public Mono<SMB2Reply> close(SMB2Header header, Session session, CloseCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(header.getCompoundRequest());

        SMB2FileId smb2FileId = (header.getCompoundRequest() != null && header.getCompoundRequest().getFileId() != null) ? header.getCompoundRequest().getFileId() : call.getFileId();
        boolean isSMB3 = header.getHandler().negprotInfo.getDialect() >= NegprotCall.SMB_3_0_0;

        return closeRelease(smb2FileId, localNode, header) // close后清除
                .flatMap(ignore -> {
                    if (fileInfo != null) {
                        FSPerformanceService.addIp(fileInfo.bucket, header.getHandler().getClientAddress());
                        return WriteCache.isExistCache(fileInfo.inodeId, isSMB3, smb2FileId)
                                .flatMap(res -> {
                                    if (isFileNeedDelete(fileInfo.inodeId)) {
                                        if (res && !isSMB3) {
                                            WriteCache.removeCache(fileInfo.inodeId);
                                        }
                                        return nodeInstance.getInode(fileInfo.bucket, fileInfo.inodeId)
                                                .flatMap(inode -> {
                                                    if (res && isSMB3) {
                                                        // 删除缓存
                                                        return WriteCacheClient.remove(inode, 0, 0, smb2FileId).map(b -> inode);
                                                    } else {
                                                        return Mono.just(inode);
                                                    }
                                                })
                                                .flatMap(inode -> {
                                                    if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                                        return closeRes(reply, session, STATUS_OBJECT_NAME_NOT_FOUND, call.getFlags(), NOT_FOUND_INODE);
                                                    }
                                                    if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                                        return closeRes(reply, session, STATUS_IO_DEVICE_ERROR, call.getFlags(), ERROR_INODE);
                                                    }
                                                    if (inode.getObjName().endsWith("/")) {
                                                        return BucketFSPerfLimiter.getInstance().limits(fileInfo.bucket, fs_rmdir.name() + "-" + THROUGHPUT_QUOTA, 1L)
                                                                .flatMap(waitMillis -> {
                                                                    String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), fileInfo.bucket);
                                                                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_rmdir.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                                                                })
                                                                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> ReadDirCache.listAndCache(fileInfo.bucket, inode.getObjName(), 0, 1024,
                                                                        new NFSHandler(), inode.getNodeId(), null, inode.getACEs())))
                                                                .flatMap(inodes -> {
                                                                    if (!inodes.isEmpty()) {
                                                                        return closeRes(reply, session, STATUS_DIRECTORY_NOT_EMPTY, call.getFlags(), inode);
                                                                    }
                                                                    return deleteInodeInClose(fileInfo, reply, session, call);
                                                                });
                                                    } else {
                                                        return BucketFSPerfLimiter.getInstance().limits(fileInfo.bucket, fs_remove.name() + "-" + THROUGHPUT_QUOTA, 1L)
                                                                .flatMap(waitMillis -> {
                                                                    String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), fileInfo.bucket);
                                                                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_remove.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                                                                })
                                                                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> deleteInodeInClose(fileInfo, reply, session, call)));
                                                    }
                                                });

                                    }
                                    if (res) {
                                        return nodeInstance.getInode(fileInfo.bucket, fileInfo.inodeId)
                                                .flatMap(inode -> {
                                                    if (InodeUtils.isError(inode)) {
                                                        reply.setBody(CloseReply.DEFAULT);
                                                        return Mono.just(reply);
                                                    }
                                                    return (isSMB3
                                                            ? WriteCacheClient.flush(inode, 0, 0, smb2FileId)
                                                            : WriteCache.getCache(fileInfo.bucket, inode.getNodeId(), 0, inode.getStorage(), true).flatMap(writeCache -> writeCache.nfsCommit(inode, 0, 0)))
                                                            .flatMap(res1 -> {
                                                                reply.getHeader().setStatus(STATUS_SUCCESS);
                                                                SMB2FileId.removeFileInfo(session);
                                                                if (call.getFlags() == 1) {
                                                                    return nodeInstance.getInode(fileInfo.bucket, fileInfo.inodeId)
                                                                            .flatMap(inode1 -> {
                                                                                reply.setBody(CloseReply.mapToCloseReply(inode1, call.getFlags()));
                                                                                return Mono.just(reply);
                                                                            });

                                                                } else {
                                                                    reply.setBody(CloseReply.DEFAULT);
                                                                }
                                                                return Mono.just(reply);
                                                            });
                                                });

                                    }
                                    if (fileInfo.updateTimeOnClose) {
                                        long mtime = System.currentTimeMillis() / 1000;
                                        int mtimeNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
                                        return nodeInstance.updateInodeTime(fileInfo.inodeId, fileInfo.bucket, mtime, mtimeNano, false, true, true)
                                                .flatMap(inode -> closeRes(reply, session, STATUS_SUCCESS, call.getFlags(), inode));
                                    }
                                    if (call.getFlags() == 1) {
                                        return Mono.just(fileInfo.openInode)
                                                .flatMap(inode -> {
                                                    if (InodeUtils.isError(inode)) {
                                                        reply.setBody(CloseReply.DEFAULT);
                                                        return Mono.just(reply);
                                                    }
                                                    reply.setBody(CloseReply.mapToCloseReply(inode, call.getFlags()));
                                                    return Mono.just(reply);
                                                });
                                    }
                                    return closeRes(reply, session, STATUS_SUCCESS, call.getFlags(), new Inode());
                                })
                                .doOnNext(r -> SMB2FileId.clearCache(header.getCompoundRequest(), call.getFileId()));
                    }
                    return closeRes(reply, session, STATUS_SUCCESS, call.getFlags(), new Inode());
                });
    }

    public Mono<SMB2Reply> deleteRes(SMB2Reply reply, Session session, Inode inode, short flags) {
        if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
            return closeRes(reply, session, STATUS_OBJECT_NAME_NOT_FOUND, flags, inode);
        }
        if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
            return closeRes(reply, session, STATUS_IO_DEVICE_ERROR, flags, inode);
        }
        return closeRes(reply, session, STATUS_SUCCESS, flags, inode);
    }

    public Mono<SMB2Reply> closeRes(SMB2Reply reply, Session session, int status, short flags, Inode inode) {
        reply.getHeader().setStatus(status);
        SMB2FileId.removeFileInfo(session);
        if (flags == 1) {
            reply.setBody(CloseReply.mapToCloseReply(inode, flags));
        } else {
            reply.setBody(CloseReply.DEFAULT);
        }
        return Mono.just(reply);
    }

    public Mono<SMB2Reply> deleteInodeInClose(SMB2FileId.FileInfo fileInfo, SMB2Reply reply, Session session, CloseCall call) {
        boolean esSwitch = ES_ON.equals(NFSBucketInfo.getBucketInfo(fileInfo.bucket).get(ES_SWITCH));
        return nodeInstance.deleteInode(fileInfo.inodeId, fileInfo.bucket, fileInfo.obj)
                .flatMap(r -> {
                    if (!InodeUtils.isError(r)) {
                        needDeleteSet.remove(fileInfo.inodeId);
                    }
                    return Mono.just(true).flatMap(b -> esSwitch && !isError(r) ? EsMetaTask.delEsMeta(r, fileInfo.bucket, fileInfo.obj, fileInfo.inodeId) : Mono.just(true))
                            .flatMap(b -> InodeUtils.findDirInode(fileInfo.obj, fileInfo.bucket))
                            .flatMap(dirInode -> {
                                long stamp = System.currentTimeMillis() / 1000;
                                int timeNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
                                return nodeInstance.updateInodeTime(dirInode.getNodeId(), fileInfo.bucket, stamp, timeNano, false, true, true);
                            })
                            .flatMap(res1 -> deleteRes(reply, session, r, call.getFlags()));
                });
    }

    public Mono<Inode> deleteDirAndFiles(Inode dirInode, String bucket, long count, NFSHandler nfsHandler) {
        String prefix = dirInode.getObjName();
        AtomicLong offset = new AtomicLong(count);
        return ReadDirCache.listAndCache(bucket, prefix, offset.get(), 1 << 20, nfsHandler, dirInode.getNodeId(), null, dirInode.getACEs())
                .flatMap(list -> {
                    if (!list.isEmpty()) {
                        offset.set(list.get(list.size() - 1).getCookie());
                    }
                    return Mono.just(list);
                })
                .flatMapMany(Flux::fromIterable)
                .flatMap(inode -> {
                    if (inode.getObjName().endsWith("/")) {
                        return deleteDirAndFiles(inode, bucket, 0, nfsHandler);
                    } else {
                        return nodeInstance.deleteInode(inode.getNodeId(), bucket, inode.getObjName());
                    }
                })
                .collectList()
                .flatMap(res -> {
                    return ReadDirCache.listAndCache(bucket, dirInode.getObjName(), offset.get(), 4096, nfsHandler, dirInode.getNodeId(), null, dirInode.getACEs())
                            .flatMap(list0 -> {
                                if (list0.isEmpty()) {
                                    return nodeInstance.deleteInode(dirInode.getNodeId(), bucket, dirInode.getObjName())
                                            .doOnNext(r -> needDeleteSet.remove(dirInode.getNodeId()));
                                } else {
                                    return deleteDirAndFiles(dirInode, bucket, offset.get(), nfsHandler);
                                }
                            });
                })
                .onErrorReturn(ERROR_INODE);
    }

    @SMB2.Smb2Opt(value = SMB2_QUERY_DIRECTORY, buf = 1 << 20)
    public Mono<SMB2Reply> queryDir(SMB2Header header, Session session, QueryDirCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        QueryDirReply body = new QueryDirReply();
        // [0]: "." 当前目录; [1]: ".." 上一级目录
        Inode[] dirInodes = {ERROR_INODE, ERROR_INODE};
        boolean[] isReturnHide = {false};
        AtomicInteger maxSize = new AtomicInteger(call.getResponseLen());

        SMB2FileId.FileInfo info = call.getFileId().getFileInfo(header.getCompoundRequest());
        if (info == null) {
            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_FILE_CLOSED);
            return Mono.just(reply);
        }
        int dirNameLen = info.obj.length();

        if ((call.getFlags() & SMB2_CONTINUE_FLAG_RESTART) != 0 || (call.getFlags() & SMB2_CONTINUE_FLAG_REOPEN) != 0) {
            info.queryDirCache = null;
        }

        if ((call.getFlags() & SMB2_CONTINUE_FLAG_SINGLE) != 0) {
            maxSize.set(1);
        }

        // 仅当前目录第一次reply时返回.和..;如果设置了单条目返回标志，.和..不需要再返回
        if ((call.getFlags() & SMB2_CONTINUE_FLAG_SINGLE) == 0 && (null == info.queryDirCache || StringUtils.isEmpty(info.queryDirCache.getMarker()))) {
            isReturnHide[0] = true;
        }
        FSPerformanceService.addIp(info.bucket, header.getHandler().getClientAddress());
        if (QueryDirInfo.isNotValidQueryClass(call.getFileInfoClass())) {
            reply.getHeader().setStatus(STATUS_INVALID_INFO_CLASS);
            return Mono.just(reply);
        }
        return BucketFSPerfLimiter.getInstance().limits(info.bucket, fs_readdir.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(header.getHandler().getClientAddress(), info.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_readdir.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMapMany(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMapMany(l -> Flux.just(new Tuple2<>(true, info.inodeId), new Tuple2<>(false, info.dirInode))))
                .flatMap(t -> {
                    if (isReturnHide[0]) {
                        return nodeInstance.getInode(info.bucket, t.var2)
                                .map(i -> {
                                    Inode dirI = i.clone();
                                    if (isError(dirI)) {
                                        dirI.setNodeId(t.var2);
                                        log.info("query: get hide file {} {} fail, linkN: {}", t.var1, t.var2, i.getLinkN());
                                    }

                                    if (t.var1) {
                                        dirI.setObjName(".");
                                        dirInodes[0] = dirI;
                                    } else {
                                        dirI.setObjName("..");
                                        dirInodes[1] = dirI;
                                    }

                                    maxSize.addAndGet(-dirI.countQueryDirInfo(call.getFileInfoClass(), ""));
                                    return dirI;
                                });
                    } else {
                        return Mono.just(ERROR_INODE);
                    }
                })
                .collectList()
                .flatMap(list -> CifsQueryDirCache.listAndCache(call.getFileInfoClass(), call.getPattern(), info, maxSize.get()))
                .map(tuple2 -> {
                    List<Inode> inodeList = tuple2.var1;
                    if (inodeList.isEmpty()) {
                        if (tuple2.var2) {
                            reply.getHeader().setStatus(STATUS_INFO_LENGTH_MISMATCH);
                        } else if (info.queryDirCache == null || info.queryDirCache.queryDirOffset == 0) {
                            //空目录也需返回.和..
                            if (info.queryDirCache != null
                                    && isReturnHide[0]
                            ) {
                                if (StringUtils.isNotBlank(info.obj)) {
                                    inodeList.add(0, dirInodes[1]);
                                    inodeList.add(0, dirInodes[0]);
                                    info.queryDirCache.queryDirOffset = dirInodes[0].getCookie();
                                } else {
                                    inodeList.add(0, dirInodes[0].clone().setObjName(".."));
                                    inodeList.add(0, dirInodes[0]);
                                    info.queryDirCache.queryDirOffset = 1;
                                }
                                return mapToQueryDirReply(call, inodeList, reply, body, dirNameLen);
                            }
                            reply.getHeader().setStatus(STATUS_NO_SUCH_FILE);
                        } else {
                            reply.getHeader().setStatus(STATUS_NO_MORE_FILES);
                        }
                        return reply;
                    } else {
                        //对于文件名长度大于255文件，不返回给客户端
                        inodeList = inodeList.stream().filter(inode -> !CheckUtils.isFileNameTooLong(inode.getObjName())).collect(Collectors.toList());
                        if (isReturnHide[0]) {
                            inodeList.add(0, dirInodes[1]);
                            inodeList.add(0, dirInodes[0]);
                        }
                        return mapToQueryDirReply(call, inodeList, reply, body, dirNameLen);
                    }
                })
                .doOnNext(r -> {
                    // 仅在同一次列举的第一个queryDir完成后根据atime情况更新时间
                    if (isReturnHide[0] && dirInodes[0].getNodeId() > 0 && dirInodes[0].getLinkN() > 0 && Inode.isRelaUpdate(dirInodes[0])) {
                        nodeInstance.updateInodeTime(dirInodes[0].getNodeId(), info.bucket, System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), true, false, false);
                    }
                });
    }

    private SMB2Reply mapToQueryDirReply(QueryDirCall call, List<Inode> inodeList, SMB2Reply reply, QueryDirReply body, int dirNameLen) {
        switch (call.getFileInfoClass()) {
            case SMB2_FIND_DIRECTORY_INFO:
                for (Inode inode : inodeList) {
                    QueryDirInfo.DirInfo dirInfo = QueryDirInfo.DirInfo.mapToDirInfo(dirNameLen, inode);
                    body.getInfoList().add(dirInfo);
                }
                reply.setBody(body);
                return reply;
            case SMB2_FIND_FULL_DIRECTORY_INFO:
                for (Inode inode : inodeList) {
                    QueryDirInfo.FullDirInfo dirInfo = QueryDirInfo.FullDirInfo.mapToFullDirInfo(dirNameLen, inode);
                    body.getInfoList().add(dirInfo);
                }
                reply.setBody(body);
                return reply;
            case SMB2_FIND_ID_BOTH_DIRECTORY_INFO:
                for (Inode inode : inodeList) {
                    QueryDirInfo.IdBothFullDirInfo queryDirInfo = QueryDirInfo.IdBothFullDirInfo.mapToIdBothFullDirInfo(dirNameLen, inode);
                    body.getInfoList().add(queryDirInfo);
                }
                reply.setBody(body);
                return reply;
            case SMB2_FIND_ID_FULL_DIRECTORY_INFO:
                for (Inode inode : inodeList) {
                    QueryDirInfo.IdFullDirInfo queryDirInfo = QueryDirInfo.IdFullDirInfo.mapToIdFullDirInfo(dirNameLen, inode);
                    body.getInfoList().add(queryDirInfo);
                }
                reply.setBody(body);
                return reply;
            case SMB2_FIND_NAME_INFO:
                for (Inode inode : inodeList) {
                    QueryDirInfo.FileNamesInfo queryDirInfo = new QueryDirInfo.FileNamesInfo();
                    queryDirInfo.setFileName(inode.realFileName(dirNameLen).getBytes(StandardCharsets.UTF_16LE));
                    body.getInfoList().add(queryDirInfo);
                }
                reply.setBody(body);
                return reply;
            case SMB2_FIND_BOTH_DIRECTORY_INFO:
                for (Inode inode : inodeList) {
                    QueryDirInfo.BothFullDirInfo queryDirInfo = QueryDirInfo.BothFullDirInfo.mapToFullDirInfo(dirNameLen, inode);
                    body.getInfoList().add(queryDirInfo);
                }
                reply.setBody(body);
                return reply;
        }

        reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_ACCESS_DENIED);
        return reply;
    }

    @SMB2.Smb2Opt(SMB2_FLUSH)
    public Mono<SMB2Reply> flush(SMB2Header header, Session session, FlushCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        SMB2Header replyHeader = reply.getHeader();
        FlushReply body = new FlushReply();
        SMB2FileId.FileInfo fileInfo = call.fileId.getFileInfo(header.getCompoundRequest());
        if (fileInfo == null) {
            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_FILE_CLOSED);
            return Mono.just(reply);
        }

        SMB2FileId smb2FileId = (header.getCompoundRequest() != null && header.getCompoundRequest().getFileId() != null) ? header.getCompoundRequest().getFileId() : call.fileId;
        boolean isSMB3 = header.getHandler().negprotInfo.getDialect() >= NegprotCall.SMB_3_0_0;

        return WriteCache.isExistCache(fileInfo.inodeId, isSMB3, smb2FileId)
                .flatMap(res -> {
                    if (res) {
                        return nodeInstance.getInode(fileInfo.bucket, fileInfo.inodeId)
                                .flatMap(inode -> {
                                    if (InodeUtils.isError(inode)) {
                                        return setInfoReturnErrorReply(reply, inode);
                                    }
                                    return (isSMB3
                                            ? WriteCacheClient.flush(inode, 0, 0, smb2FileId)
                                            : WriteCache.getCache(fileInfo.bucket, inode.getNodeId(), 0, inode.getStorage(), true).flatMap(writeCache -> writeCache.nfsCommit(inode, 0, 0)))
                                            .flatMap(res1 -> {
                                                if (res1) {
                                                    replyHeader.status = STATUS_SUCCESS;
                                                    reply.setBody(body);
                                                    return Mono.just(reply);
                                                } else {
                                                    replyHeader.status = STATUS_IO_DEVICE_ERROR;
                                                    return Mono.just(reply);
                                                }
                                            });
                                });
                    } else {
                        reply.setBody(body);
                        return Mono.just(reply);
                    }
                });
    }


    @SMB2.Smb2Opt(value = SMB2_LOCK)
    public Mono<SMB2Reply> lock(SMB2Header header, Session session, LockCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        LockReply body = new LockReply();
        reply.setBody(body);
        return Mono.just(reply);
        // 当前版本暂不开放 lock 功能
//        String bucket = NFSBucketInfo.getBucketName(header.tid);
//        SMB2FileId.FileInfo fileInfo = call.fileId.getFileInfo(header.getCompoundRequest());
//        if (fileInfo == null) {
//            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_FILE_CLOSED);
//            return Mono.just(reply);
//        }
//
//        boolean allMatchFlags = call.locks.stream()
//                .allMatch(lock -> {
//                    if (call.locks.size() == 1) {
//                        return lock.flags == SMB2_LOCKFLAG_SHARED_LOCK ||
//                                lock.flags == SMB2_LOCKFLAG_EXCLUSIVE_LOCK ||
//                                lock.flags == (SMB2_LOCKFLAG_SHARED_LOCK | SMB2_LOCKFLAG_FAIL_IMMEDIATELY) ||
//                                lock.flags == (SMB2_LOCKFLAG_EXCLUSIVE_LOCK | SMB2_LOCKFLAG_FAIL_IMMEDIATELY) ||
//                                lock.flags == SMB2_LOCKFLAG_UNLOCK;
//                    } else {
//                        return lock.flags == (SMB2_LOCKFLAG_SHARED_LOCK | SMB2_LOCKFLAG_FAIL_IMMEDIATELY) ||
//                                lock.flags == (SMB2_LOCKFLAG_EXCLUSIVE_LOCK | SMB2_LOCKFLAG_FAIL_IMMEDIATELY) ||
//                                lock.flags == SMB2_LOCKFLAG_UNLOCK;
//                    }
//                });
//        if (!allMatchFlags) {
//            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_INVALID_PARAMETER);
//            return Mono.just(reply);
//        }
//        if (call.locks.size() > 1) {
//            boolean hasLockFailImmediately = call.locks.stream()
//                    .anyMatch(lock -> lock.flags == (SMB2_LOCKFLAG_SHARED_LOCK | SMB2_LOCKFLAG_FAIL_IMMEDIATELY) ||
//                            lock.flags == (SMB2_LOCKFLAG_EXCLUSIVE_LOCK | SMB2_LOCKFLAG_FAIL_IMMEDIATELY)
//                    );
//            boolean hasUnlock = call.locks.stream()
//                    .anyMatch(lock -> lock.flags == SMB2_LOCKFLAG_UNLOCK);
//            if (hasLockFailImmediately && hasUnlock) {
//                reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_INVALID_PARAMETER);
//                return Mono.just(reply);
//            }
//        }
//
//        Set<CIFSLock> successLocks = new HashSet<>();
//        return Flux.fromIterable(call.locks)
//                .concatMap(lock -> {
//                    CIFSLock cifsLock = null;
//                    switch (lock.flags) {
//                        case SMB2_LOCKFLAG_SHARED_LOCK:
//                            cifsLock = new CIFSLock(bucket, fileInfo.obj, fileInfo.inodeId, call.fileId, header, localNode, lock, 1, false, true, false);
//                            break;
//                        case SMB2_LOCKFLAG_SHARED_LOCK | SMB2_LOCKFLAG_FAIL_IMMEDIATELY:
//                            cifsLock = new CIFSLock(bucket, fileInfo.obj, fileInfo.inodeId, call.fileId, header, localNode, lock, 1, true, true, false);
//                            break;
//                        case SMB2_LOCKFLAG_EXCLUSIVE_LOCK:
//                            cifsLock = new CIFSLock(bucket, fileInfo.obj, fileInfo.inodeId, call.fileId, header, localNode, lock, 2, false, true, false);
//                            break;
//                        case SMB2_LOCKFLAG_EXCLUSIVE_LOCK | SMB2_LOCKFLAG_FAIL_IMMEDIATELY:
//                            cifsLock = new CIFSLock(bucket, fileInfo.obj, fileInfo.inodeId, call.fileId, header, localNode, lock, 2, true, true, false);
//                            break;
//                        case SMB2_LOCKFLAG_UNLOCK:
//                            cifsLock = new CIFSLock(bucket, fileInfo.obj, fileInfo.inodeId, call.fileId, header, localNode, lock, 0, false, false, true);
//                            if (!CIFSLockClient.isLocked(cifsLock, header)) {
//                                reply.getHeader().setStatus(STATUS_RANGE_NOT_LOCKED); // 批量unlock时，错误lock参数前的lock正常解除，之后不解除。
//                                return Mono.just(false);
//                            } else {
//                                return CIFSLockClient.unlock(bucket, String.valueOf(fileInfo.inodeId), cifsLock, true);
//                            }
//                        default:
//                            reply.getHeader().setStatus(STATUS_INVALID_PARAMETER);
//                            return Mono.just(false);
//                    }
//                    CIFSLock successLock = new CIFSLock(cifsLock);
//                    return CIFSLockClient.lock(bucket, String.valueOf(fileInfo.inodeId), cifsLock)
//                            .map(b -> {
//                                if (!b) {
//                                    reply.getHeader().setStatus(STATUS_LOCK_NOT_GRANTED);
//                                } else {
//                                    successLocks.add(successLock);
//                                }
//                                return b;
//                            });
//                })
//                .takeWhile(result -> result)
//                .collectList()
//                .flatMap(resultList -> {
//                    if (resultList.size() < call.locks.size()) { // 一次加锁请求中有加锁失败时全部回退
//                        if (call.locks.size() == 1) {
//                            LockCall.LockElement lock = call.locks.get(0);
//                            if (call.locks.get(0).flags == SMB2_LOCKFLAG_SHARED_LOCK || call.locks.get(0).flags == SMB2_LOCKFLAG_EXCLUSIVE_LOCK) {
//                                CIFSLock cifsLock;
//                                if (call.locks.get(0).flags == SMB2_LOCKFLAG_SHARED_LOCK) {
//                                    cifsLock = new CIFSLock(bucket, fileInfo.obj, fileInfo.inodeId, call.fileId, header, localNode, lock, 1, false, false, false);
//                                } else {
//                                    cifsLock = new CIFSLock(bucket, fileInfo.obj, fileInfo.inodeId, call.fileId, header, localNode, lock, 2, false, false, false);
//                                }
//                                reply.getHeader().setStatus(STATUS_SUCCESS);
//                                LockReply body = new LockReply();
//                                reply.setBody(body);
//                                MonoProcessor<Boolean> res = MonoProcessor.create();
//                                CIFSLockServer.pendingMap.compute(cifsLock, (cifsLock0, tuple2) -> {
//                                    if (!CIFSLockServer.waitSuccessSet.contains(cifsLock)) {
//                                        reply.getHeader().setFlags(reply.getHeader().getFlags() | SMB2_HDR_FLAG_ASYNC);
//                                        tuple2 = Tuples.of(session.getHandler(), reply);
//                                        res.onNext(false);
//                                    } else {
//                                        res.onNext(true);
//                                    }
//                                    return tuple2;
//                                });
//                                return res.flatMap(flag -> {
//                                    if (flag) {
//                                        String leaseKey = LeaseCache.getLeaseKey(call.fileId);
//                                        CheckUtils.cifsLeaseOpenCheck().subscribe(b -> {
//                                            if (b) {
//                                                LeaseClient.breakLease(bucket, String.valueOf(fileInfo.inodeId), leaseKey, 0b011, call.fileId, localNode).subscribe();
//                                            }
//                                        });
//                                        return Mono.just(reply);
//                                    } else {
//                                        return Mono.just(createPendingReply(header));
//                                    }
//                                });
//                            }
//                        }
//                        return Flux.fromIterable(successLocks)
//                                .flatMap(successLock -> CIFSLockClient.unlock(bucket, String.valueOf(fileInfo.inodeId), successLock, true))
//                                .collectList()
//                                .map(list -> reply);
//                    } else {
//                        if (call.locks.get(0).flags != SMB2_LOCKFLAG_UNLOCK) {
//                            // 加锁成功打破 handle 和 read lease
//                            CheckUtils.cifsLeaseOpenCheck().subscribe(b -> {
//                                if (b) {
//                                    String leaseKey = LeaseCache.getLeaseKey(call.fileId);
//                                    LeaseClient.breakLease(bucket, String.valueOf(fileInfo.inodeId), leaseKey, 0b011, call.fileId, localNode).subscribe();
//                                }
//                            });
//                        }
//                        reply.getHeader().setStatus(STATUS_SUCCESS);
//                        LockReply body = new LockReply();
//                        reply.setBody(body);
//                        return Mono.just(reply);
//                    }
//                });
    }

}
