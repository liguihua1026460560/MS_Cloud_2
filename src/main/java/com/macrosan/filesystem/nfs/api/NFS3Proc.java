package com.macrosan.filesystem.nfs.api;

import com.macrosan.action.managestream.FSPerformanceService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cache.ReadObjCache;
import com.macrosan.filesystem.cache.WriteCache;
import com.macrosan.filesystem.lock.redlock.LockType;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.nfs.call.*;
import com.macrosan.filesystem.nfs.direct.DirectWrite;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.reply.*;
import com.macrosan.filesystem.nfs.types.*;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.filesystem.utils.*;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.NFSACL;
import com.macrosan.message.jsonmsg.BucketInfo;
import com.macrosan.message.jsonmsg.DirInfo;
import com.macrosan.message.jsonmsg.FSQuotaConfig;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.perf.AddressFSPerfLimiter;
import com.macrosan.utils.perf.BucketFSPerfLimiter;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.action.managestream.FSPerformanceService.Instance_Type.*;
import static com.macrosan.action.managestream.FSPerformanceService.getAddressPerfRedisKey;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.ACLConstants.NFS_DIR_SUB_DEL_ACL;
import static com.macrosan.filesystem.FsConstants.ACLConstants.NOT_CONTAIN_ACL;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NFSACLType.NFS_ACE;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.*;
import static com.macrosan.filesystem.cache.ReadObjCache.readCacheDebug;
import static com.macrosan.filesystem.nfs.NFSBucketInfo.getBucketName;
import static com.macrosan.filesystem.quota.FSQuotaConstants.FS_DIR_QUOTA;
import static com.macrosan.filesystem.utils.FsTierUtils.fileFsAccessHandle;
import static com.macrosan.filesystem.utils.InodeUtils.*;
import static com.macrosan.filesystem.utils.acl.ACLUtils.DEFAULT_ANONY_UID;
import static com.macrosan.message.jsonmsg.Inode.*;

@Log4j2
public class NFS3Proc {
    private static final NFS3Proc instance = new NFS3Proc();
    private static final Node nodeInstance = Node.getInstance();
    public static boolean debug = false;

    private NFS3Proc() {
    }

    public static NFS3Proc getInstance() {
        return instance;
    }

    @NFSV3.Opt(NFSV3.Opcode.NFS3PROC_FSINFO)
    public Mono<RpcReply> fsInfo(RpcCallHeader callHeader, ReqInfo reqHeader, FH2 fh) {
        reqHeader.bucket = getBucketName(fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        FsInfoReply reply = new FsInfoReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            reply.status = NFS3ERR_STALE;
            return Mono.just(reply);
        }
        return CheckUtils.nfsOpenCheck(fh.fsid, reqHeader)
                .flatMap(res -> {
                    if (res) {
                        reply.status = 0;
                    } else {
                        log.info("bucket:{}, does not open NFS,or the client ip is not allow to access.", reqHeader.bucket);
                        reply.status = NFS3ERR_STALE;
                    }
                    return Mono.just(reply);
                });
    }

    @NFSV3.Opt(NFSV3.Opcode.NFS3PROC_FSSTAT)
    public Mono<RpcReply> fsStat(RpcCallHeader callHeader, ReqInfo reqHeader, FH2 fh) {
        reqHeader.bucket = getBucketName(fh.fsid);
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        FsStatReply reply = new FsStatReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            reply.status = NFS3ERR_STALE;
            return Mono.just(reply);
        }
        return CheckUtils.nfsOpenCheck(fh.fsid, reqHeader)
                .flatMap(res -> {
                    if (res) {
                        reply.status = 0;

                        return FSQuotaUtils.findFinalMinFsQuotaConfig(reqHeader.bucket, fh.ino)
                                .flatMap(tuple -> {
                                    FSQuotaConfig config = tuple.getT1();
                                    long inodeId = tuple.getT2();

                                    if (inodeId == 1L) {
                                        return ErasureClient.reduceBucketInfo(reqHeader.bucket)
                                                .filter(BucketInfo::isAvailable)
                                                .map(BucketInfo::getBucketStorage)
                                                .doOnError(e -> log.error("get bucket used capacity error", e))
                                                .defaultIfEmpty("0")
                                                .flatMap(usedCapacityStr -> {
                                                    long usedCapacity = Long.parseLong(usedCapacityStr);
                                                    long hard = config.getCapacityHardQuota() != 0 ? config.getCapacityHardQuota() : -1;
                                                    long totalSize = (hard > 0) ? Math.max(hard, usedCapacity) : 0;
                                                    long availableSize = Math.max(0, totalSize - usedCapacity);

                                                    reply.totalBytes = totalSize;
                                                    reply.freeBytes = availableSize;
                                                    reply.availFreeBytes = reply.freeBytes;

                                                    return Mono.just(reply);
                                                });
                                    }

                                    return FSQuotaRealService.getFsQuotaInfo(reqHeader.bucket, inodeId, FS_DIR_QUOTA, 0)
                                            .flatMap(dirInfo -> {
                                                if (DirInfo.isErrorInfo(dirInfo)) {
                                                    return Mono.<RpcReply>empty();
                                                } else {
                                                    long usedCapacity = Long.parseLong(dirInfo.getUsedCap());
                                                    long hard = config.getCapacityHardQuota() != 0 ? config.getCapacityHardQuota() : -1;
                                                    long totalSize = (hard > 0) ? Math.max(hard, usedCapacity) : 0;
                                                    long availableSize = Math.max(0, totalSize - usedCapacity);

                                                    reply.totalBytes = totalSize;
                                                    reply.freeBytes = availableSize;
                                                    reply.availFreeBytes = reply.freeBytes;
                                                }

                                                return Mono.just(reply);
                                            });

                                })
                                // 只有当目录以及其任意祖先目录都不存在配额时才会进这里
                                .switchIfEmpty(Mono.defer(() -> {
                                    List<StoragePool> storagePoolList = StoragePoolFactory.getAvailableStoragesWithCachePool(reqHeader.bucket);
                                    for (StoragePool pool : storagePoolList) {
                                        int km = pool.getM() + pool.getK();
                                        int k = pool.getK();

                                        reply.totalBytes += (pool.getCache().totalSize + pool.getCache().firstPartCapacity) / km * k;
                                        reply.freeBytes += (pool.getCache().totalSize + pool.getCache().firstPartCapacity - pool.getCache().size - pool.getCache().firstPartUsedSize) / km * k;
                                    }
                                    reply.availFreeBytes = reply.freeBytes;

                                    return Mono.just(reply);
                                }));
                    } else {
                        NFSBucketInfo.logFsidInfo(fh.fsid, reqHeader.bucket);
                        reply.status = NFS3ERR_STALE;
                    }
                    return Mono.just(reply);
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_READDIRPLUS, buf = 1 << 20)
    public Mono<RpcReply> readdirPlus(RpcCallHeader callHeader, ReqInfo reqHeader, ReadDirPlusCall call) {
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        String bucket = reqHeader.bucket;
        AtomicInteger dirLength = new AtomicInteger();
        AtomicInteger dirBytesLength = new AtomicInteger();
        long nodeId = call.fh.ino;
        long offset = call.cookie;
        int count = call.count;

        ReadDirPlusReply reply = new ReadDirPlusReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            reply.status = NFS3ERR_STALE;
            return Mono.just(reply);
        }
        reply.verf = call.verf;
        Inode[] dirInodes = new Inode[1];
        boolean[] isUpAtime = new boolean[1];
        return nodeInstance.getInode(bucket, nodeId)
                .flatMap(dirInode -> {
                    dirInodes[0] = dirInode;
                    if (isError(dirInode)) {
                        throw new NFSException(NfsErrorNo.NFS3ERR_I0, "input/out error.nodeId:" + nodeId + " bucket" + reqHeader.bucket + ":" + dirInode.getLinkN());
                    }
                    dirLength.set(dirInode.getObjName().length());
                    dirBytesLength.set(dirInode.getObjName().getBytes(StandardCharsets.UTF_8).length);
                    reply.dir = Attr.mapToAttr(dirInode, call.fh.fsid);
                    String prefix = dirInode.getObjName();

                    return NFSACL.judgeNFSOptAccess(dirInode, reqHeader, callHeader, false, null, 0)
                            .flatMap(pass -> {
                                if (!pass) {
                                    reply.dir = Attr.mapToAttr(dirInodes[0], call.fh.fsid);
                                    reply.status = NFS3ERR_ACCES;
                                    return Mono.just((RpcReply) reply);
                                }

                                return ReadDirCache.listAndCache(bucket, prefix, offset, count, reqHeader.nfsHandler, dirInode.getNodeId(), reply, dirInode.getACEs())
                                        .map(inodeList -> {
                                            int replySize = reply.size();

                                            for (Inode inode : inodeList) {
                                                int limitFileNameLength = 255;
                                                if (inode.getObjName().endsWith("/")) {
                                                    limitFileNameLength = 256;
                                                }
                                                if (inode.getObjName().getBytes(StandardCharsets.UTF_8).length - dirBytesLength.get() <= limitFileNameLength) {
                                                    Entry entry = Entry.mapToEntry(dirLength.get(), inode, call.fh.fsid);
                                                    if (replySize + entry.size() >= count - SunRpcHeader.SIZE) {
                                                        break;
                                                    }

                                                    reply.entryList.add(entry);
                                                    replySize += entry.size();
                                                }
                                            }

                                            if (!isError(dirInodes[0]) && isRelaUpdate(dirInodes[0])) {
                                                dirInodes[0].setAtime(System.currentTimeMillis() / 1000);
                                                dirInodes[0].setAtimensec((int) (System.nanoTime() % ONE_SECOND_NANO));
                                                reply.dir = Attr.mapToAttr(dirInodes[0], call.fh.fsid);
                                                isUpAtime[0] = true;
                                            }

                                            return (RpcReply) reply;
                                        })
                                        .doOnNext(reply0 -> {
                                            if (isUpAtime[0]) {
                                                nodeInstance.updateInodeTime(dirInodes[0].getNodeId(), bucket, dirInodes[0].getAtime(), dirInodes[0].getAtimensec(), true, false, false);
                                            }
                                        });
                            })
                            .doOnError(e -> log.error("", e));
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_READDIR, buf = 1 << 20)
    public Mono<RpcReply> readdir(RpcCallHeader callHeader, ReqInfo reqHeader, ReadDirCall call) {
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        String bucket = reqHeader.bucket;
        AtomicInteger dirLength = new AtomicInteger();
        AtomicInteger dirBytesLength = new AtomicInteger();
        long nodeId = call.fh.ino;
        long offset = call.cookie;
        int count = call.count;

        ReadDirReply reply = new ReadDirReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            reply.status = NFS3ERR_STALE;
            reply.attrBefore = 0;
            return Mono.just(reply);
        }
        reply.follows = 0;
        Inode[] dirInodes = new Inode[1];
        boolean[] isUpAtime = new boolean[1];
        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_readdir.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_readdir.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> nodeInstance.getInode(bucket, nodeId)))
                .flatMap(dirInode -> {
                    dirInodes[0] = dirInode;
                    if (isError(dirInode)) {
                        throw new NFSException(NfsErrorNo.NFS3ERR_I0, "input/out error.nodeId:" + nodeId + " bucket:" + reqHeader.bucket + ":" + dirInode.getLinkN());
                    }
                    dirLength.set(dirInode.getObjName().length());
                    dirBytesLength.set(dirInode.getObjName().getBytes(StandardCharsets.UTF_8).length);
                    reply.attr = FAttr3.mapToAttr(dirInode, call.fh.fsid);
                    String prefix = dirInode.getObjName();

                    return NFSACL.judgeNFSOptAccess(dirInode, reqHeader, callHeader, false, null, 0)
                            .flatMap(pass -> {
                                if (!pass) {
                                    reply.attr = FAttr3.mapToAttr(dirInodes[0], call.fh.fsid);
                                    reply.status = NFS3ERR_ACCES;
                                    return Mono.just((RpcReply) reply);
                                }

                                return ReadDirCache.listAndCache(bucket, prefix, offset, count, reqHeader.nfsHandler, dirInode.getNodeId(), reply, dirInode.getACEs())
                                        .map(inodeList -> {
                                            int replySize = reply.size();
                                            if (inodeList.size() > 0) {
                                                reply.follows = 1;
                                            }
                                            int i = 0;
                                            for (Inode inode : inodeList) {
                                                int limitFileNameLength = 255;
                                                if (inode.getObjName().endsWith("/")) {
                                                    limitFileNameLength = 256;
                                                }
                                                if (inode.getObjName().getBytes(StandardCharsets.UTF_8).length - dirBytesLength.get() <= limitFileNameLength) {
                                                    int follows = i == (inodeList.size() - 1) ? 0 : 1;
                                                    DirEnt dirEnt = DirEnt.mapToDirEnt(inode.getNodeId(), inode.getObjName(), follows, inode.getCookie(), dirLength.get());
                                                    if (replySize + dirEnt.entSize() >= count - SunRpcHeader.SIZE) {
                                                        break;
                                                    }
                                                    reply.entryList.add(dirEnt);
                                                    replySize += dirEnt.entSize();
                                                }
                                                i++;
                                            }

                                            if (reply.entryList.isEmpty()) {
                                                reply.eof = 1;
                                            }

                                            if (!isError(dirInodes[0]) && Inode.isRelaUpdate(dirInodes[0])) {
                                                dirInodes[0].setAtime(System.currentTimeMillis() / 1000);
                                                dirInodes[0].setAtimensec((int) (System.nanoTime() % ONE_SECOND_NANO));
                                                reply.attr = FAttr3.mapToAttr(dirInodes[0], call.fh.fsid);
                                                isUpAtime[0] = true;
                                            }

                                            return (RpcReply) reply;
                                        })
                                        .doOnNext(reply0 -> {
                                            if (isUpAtime[0]) {
                                                nodeInstance.updateInodeTime(dirInodes[0].getNodeId(), bucket, dirInodes[0].getAtime(), dirInodes[0].getAtimensec(), true, false, false);
                                            }
                                        });
                            })
                            .doOnError(e -> log.error("", e));
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_WRITE)
    public Mono<RpcReply> directWrite(RpcCallHeader callHeader, ReqInfo reqHeader, DirectWriteCall call) {
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        String bucket = reqHeader.bucket;
        long nodeId = call.fh.ino;
        WriteReply reply = new WriteReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            reply.status = NFS3ERR_STALE;
            return Mono.just(reply);
        }
        Inode[] inodes = new Inode[1];
        if (CheckUtils.checkIfOverFlow(call.writeOffset, call.dataLen)) {
            reply.status = NfsErrorNo.NFS3ERR_FBIG;
            reply.attr = Attr.mapToAttr(ERROR_INODE, call.fh.fsid);
            return Mono.just(reply);
        }

        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_write.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_write.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> waitMillis == 0 ? Mono.just(0L) : Mono.delay(Duration.ofMillis(waitMillis)))
                .flatMap(l -> nodeInstance.getInode(reqHeader.bucket, call.fh.ino))
                .flatMap(inode -> {
                    if (isError(inode)) {
                        reply.status = NfsErrorNo.NFS3ERR_I0;
                        reply.attr = Attr.mapToAttr(ERROR_INODE, call.fh.fsid);
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, nodeId, inode.getLinkN());
                        return Mono.just(reply);
                    }
                    inodes[0] = inode;

                    return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_write.name() + "-" + BAND_WIDTH_QUOTA, call.dataLen)
                            .flatMap(waitMillis -> {
                                String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                                return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_write.name() + "-" + BAND_WIDTH_QUOTA, call.dataLen).map(waitMillis2 -> waitMillis + waitMillis2);
                            })
                            .flatMap(waitMillis -> waitMillis == 0 ? Mono.just(true) : Mono.delay(Duration.ofMillis(waitMillis)))
                            .flatMap(l -> FSQuotaUtils.checkFsQuota(inode))
                            .flatMap(l -> DirectWrite.directWrite(call, inode))
                            .map(b -> {
                                reply.count = call.count;
                                reply.sync = call.sync;

                                if (!b) {
                                    reply.status = NfsErrorNo.NFS3ERR_I0;
                                } else {
                                    reply.status = 0;
                                    inode.setSize(call.writeOffset + call.count);
                                }
                                reply.attr = Attr.mapToAttr(inode, call.fh.fsid);
                                return (RpcReply) reply;
                            });
                }).onErrorResume(e -> {
                    if (inodes[0] != null) {
                        if (e instanceof NFSException) {
                            int stat = ((NFSException) e).getErrCode();
                            if (stat == NfsErrorNo.NFS3ERR_DQUOT) {
                                FSQuotaUtils.logQuotaExceeded(inodes[0], e);
                            } else {
                                log.error("obj:{},nodeId:{}", inodes[0].getObjName(), inodes[0].getNodeId(), e);
                            }
                        } else {
                            log.error("obj:{},nodeId:{}", inodes[0].getObjName(), inodes[0].getNodeId(), e);
                        }
                    } else {
                        log.error("", e);
                    }
                    reply.status = NfsErrorNo.NFS3ERR_I0;
                    if (e instanceof NFSException) {
                        int stat = ((NFSException) e).getErrCode();
                        if (stat == NfsErrorNo.NFS3ERR_DQUOT) {
                            reply.status = NfsErrorNo.NFS3ERR_DQUOT;
                        }
                    }
                    if (inodes[0] != null) {
                        reply.attr = Attr.mapToAttr(inodes[0], call.fh.fsid);
                    } else {
                        reply.attr = Attr.mapToAttr(ERROR_INODE, call.fh.fsid);
                    }
                    return Mono.just(reply);
                });
    }

    //    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_WRITE)
    public Mono<RpcReply> write(RpcCallHeader callHeader, ReqInfo reqHeader, WriteCall call) {
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        String bucket = reqHeader.bucket;
        long nodeId = call.fh.ino;
        WriteReply reply = new WriteReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            reply.status = NFS3ERR_STALE;
            return Mono.just(reply);
        }
        Inode[] inodes = new Inode[1];
        if (CheckUtils.checkIfOverFlow(call.writeOffset, call.dataLen)) {
            reply.status = NfsErrorNo.NFS3ERR_FBIG;
            reply.attr = Attr.mapToAttr(ERROR_INODE, call.fh.fsid);
            return Mono.just(reply);
        }

        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_write.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_write.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> waitMillis == 0 ? Mono.just(0L) : Mono.delay(Duration.ofMillis(waitMillis)))
                .flatMap(l -> nodeInstance.getInode(reqHeader.bucket, call.fh.ino))
                .flatMap(inode -> {
                    if (isError(inode)) {
                        reply.status = NfsErrorNo.NFS3ERR_I0;
                        reply.attr = Attr.mapToAttr(ERROR_INODE, call.fh.fsid);
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, nodeId, inode.getLinkN());
                        return Mono.just(reply);
                    }
                    inodes[0] = inode;

                    return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_write.name() + "-" + BAND_WIDTH_QUOTA, call.bytes.length)
                            .flatMap(waitMillis -> {
                                String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                                return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_write.name() + "-" + BAND_WIDTH_QUOTA, call.bytes.length).map(waitMillis2 -> waitMillis + waitMillis2);
                            })
                            .flatMap(waitMillis -> waitMillis == 0 ? Mono.just(true) : Mono.delay(Duration.ofMillis(waitMillis)))
                            .flatMap(l -> FSQuotaUtils.checkFsQuota(inode))
                            .flatMap(l -> WriteCache.getCache(bucket, nodeId, call.sync, inode.getStorage()))
                            .flatMap(fileCache -> fileCache.nfsWrite(call.writeOffset, call.bytes, inode, call.sync))
                            .map(b -> {
                                reply.count = call.count;
                                reply.sync = call.sync;

                                if (!b) {
                                    reply.status = NfsErrorNo.NFS3ERR_I0;
                                } else {
                                    reply.status = 0;
                                    inode.setSize(call.writeOffset + call.count);
                                }
                                reply.attr = Attr.mapToAttr(inode, call.fh.fsid);
                                return (RpcReply) reply;
                            });
                }).onErrorResume(e -> {
                    if (inodes[0] != null) {
                        if (e instanceof NFSException) {
                            int stat = ((NFSException) e).getErrCode();
                            if (stat == NfsErrorNo.NFS3ERR_DQUOT) {
                                FSQuotaUtils.logQuotaExceeded(inodes[0], e);
                            } else {
                                log.error("obj:{},nodeId:{}", inodes[0].getObjName(), inodes[0].getNodeId(), e);
                            }
                        } else {
                            log.error("obj:{},nodeId:{}", inodes[0].getObjName(), inodes[0].getNodeId(), e);
                        }
                    } else {
                        log.error("", e);
                    }
                    reply.status = NfsErrorNo.NFS3ERR_I0;
                    if (e instanceof NFSException) {
                        int stat = ((NFSException) e).getErrCode();
                        if (stat == NfsErrorNo.NFS3ERR_DQUOT) {
                            reply.status = NfsErrorNo.NFS3ERR_DQUOT;
                        }
                    }
                    if (inodes[0] != null) {
                        reply.attr = Attr.mapToAttr(inodes[0], call.fh.fsid);
                    } else {
                        reply.attr = Attr.mapToAttr(ERROR_INODE, call.fh.fsid);
                    }
                    return Mono.just(reply);
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_COMMIT)
    public Mono<RpcReply> commit(RpcCallHeader callHeader, ReqInfo reqHeader, CommitCall call) {
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        String bucket = reqHeader.bucket;
        long nodeId = call.fh.ino;
        CommitReply reply = new CommitReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            reply.status = NFS3ERR_STALE;
            return Mono.just(reply);
        }
        Inode[] inodes = new Inode[1];
        return nodeInstance.getInode(reqHeader.bucket, call.fh.ino)
                .flatMap(inode -> {
                    if (isError(inode)) {
                        reply.status = NfsErrorNo.NFS3ERR_I0;
                        reply.attr = Attr.mapToAttr(inode, call.fh.fsid);
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, nodeId, inode.getLinkN());
                        return Mono.just(reply);
                    }

                    inodes[0] = inode;
                    return NFSACL.judgeNFSOptAccess(inode, reqHeader, callHeader, true, null, 0)
                            .flatMap(pass -> {
                                if (!pass) {
                                    reply.status = NFS3ERR_ACCES;
                                    reply.attr = Attr.mapToAttr(inode, call.fh.fsid);
                                    return Mono.just((RpcReply) reply);
                                }

                                return WriteCache.getCache(bucket, nodeId, 0, inode.getStorage())
                                        .flatMap(nfsCache -> nfsCache.nfsCommit(inode, call.offset, call.count))
                                        .map(b -> {
                                            if (b) {
                                                reply.status = 0;
                                            } else {
                                                reply.status = NfsErrorNo.NFS3ERR_I0;
                                            }
                                            reply.attr = Attr.mapToAttr(inode, call.fh.fsid);
                                            return (RpcReply) reply;
                                        });
                            });
                }).onErrorResume(e -> {
                    log.error("", e);
                    reply.status = NfsErrorNo.NFS3ERR_I0;
                    if (e instanceof NFSException) {
                        int stat = ((NFSException) e).getErrCode();
                        if (stat == NfsErrorNo.NFS3ERR_DQUOT) {
                            reply.status = NfsErrorNo.NFS3ERR_DQUOT;
                        }
                    }
                    if (inodes[0] != null) {
                        reply.attr = Attr.mapToAttr(inodes[0], call.fh.fsid);
                    } else {
                        reply.attr = Attr.mapToAttr(ERROR_INODE, call.fh.fsid);
                    }
                    return Mono.just(reply);
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_GETATTR)
    public Mono<RpcReply> getattr(RpcCallHeader callHeader, ReqInfo reqHeader, GetAttrCall call) {
        GetAttrReply reply = new GetAttrReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            reply.status = NFS3ERR_STALE;
            return Mono.just(reply);
        }
        return CheckUtils.nfsOpenCheck(call.fh.fsid, reqHeader)
                .flatMap(res -> {
                    if (!res) {
                        return Mono.just(STALE_HANDLE_INODE);
                    }
                    return nodeInstance.getInode(reqHeader.bucket, call.fh.ino);
                })
                .map(inode -> {
                    if (isError(inode)) {
                        reply.status = NfsErrorNo.NFS3ERR_NOENT;
                        reply.stat = FAttr3.mapToAttr(inode, call.fh.fsid);
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, call.fh.ino, inode.getLinkN());
                        return reply;
                    }
                    if (inode.getLinkN() == STALE_HANDLE_INODE.getLinkN()) {
                        reply.status = NFS3ERR_STALE;
                        reply.stat = FAttr3.mapToAttr(inode, call.fh.fsid);
                        return reply;
                    }
                    reply.status = 0;
                    reply.stat = FAttr3.mapToAttr(inode, call.fh.fsid);
                    return reply;
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_SETATTR)
    public Mono<RpcReply> setattr(RpcCallHeader callHeader, ReqInfo reqHeader, SetAttrCall call) {
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        SetAttrReply reply = new SetAttrReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            reply.status = NFS3ERR_STALE;
            reply.followsOld = 0;
            reply.followsNew = 0;
            return Mono.just(reply);
        }
        reply.call = call;

        return nodeInstance.getInode(reqHeader.bucket, call.fh.ino)
                .flatMap(inode -> {
                    if (isError(inode)) {
                        reply.status = NfsErrorNo.NFS3ERR_I0;
                        reply.followsOld = 0;
                        reply.followsNew = 0;
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, call.fh.ino, inode.getLinkN());
                        return Mono.just(reply);
                    }

                    Map<String, String> extraParam = new HashMap<>();
                    extraParam.put("objAttr", Json.encode(call.attr));
                    return NFSACL.judgeNFSOptAccess(inode, reqHeader, callHeader, true, extraParam, 0)
                            .flatMap(pass -> {
                                int uid = ACLUtils.getUidAndGid(callHeader)[0];
                                int gid = ACLUtils.getUidAndGid(callHeader)[1];
                                int[] gids = callHeader.auth.flavor == 1 ? ((AuthUnix) (callHeader.auth)).getGids() : new int[]{0};
                                List<Integer> gidList = Arrays.stream(gids)
                                        .boxed()
                                        .collect(Collectors.toList());

                                if (!pass) {
                                    reply.status = NFS3ERR_ACCES;
                                    reply.followsNew = 0;
                                    log.error("permission denied. ino: {}, obj: {}:{}:{}, req: {}:{}", inode.getNodeId(), inode.getUid(), inode.getGid(), inode.getObjName(), uid, gid);
                                    return Mono.just(reply);
                                }

                                if (ACLUtils.NFS_ACL_START) {
                                    boolean processOldInode = false;
                                    //兼容旧版本数据，判断是否进行身份的权限提升
                                    if (!ACLUtils.checkNewInode(inode) && ACLUtils.isOldInodeOwner(inode, uid)) {
                                        processOldInode = true;
                                    }

                                    boolean changeOwn = false;
                                    boolean changeGrp = false;
                                    boolean changeModeRoot = false;
                                    boolean changeMode = false;

                                    if (!processOldInode || uid == 0 || uid == DEFAULT_ANONY_UID) {
                                        // 非root用户不允许更改文件或者目录的属主
                                        changeOwn = call.attr.hasUid != 0 && uid != 0;
                                        // 非root和owner用户不可以更改文件或者目录的属组，owner只可以更改自身所在的组
                                        changeGrp = call.attr.hasGid != 0 && (uid != 0 && (uid != inode.getUid() || (gid != call.attr.gid && !gidList.contains(call.attr.gid))));
                                        // 非root用户不允许更改根目录的ugo权限
                                        changeModeRoot = (call.fh.ino == 1 && uid != 0 && call.attr.hasMode != 0);
                                        // 非root和owner用户不可以更改ugo权限
                                        changeMode = ((uid != 0 && uid != inode.getUid()) && call.attr.hasMode != 0);
                                    } else {
                                        // 非root用户不允许更改文件或者目录的属主
                                        changeOwn = call.attr.hasUid != 0 && uid != 0;
                                        // 非root和owner用户不可以更改文件或者目录的属组，owner只可以更改自身所在的组
                                        changeGrp = call.attr.hasGid != 0 && uid != 0 && (gid != call.attr.gid && !gidList.contains(call.attr.gid));
                                        // 非root用户不允许更改根目录的ugo权限
                                        changeModeRoot = (call.fh.ino == 1 && uid != 0 && call.attr.hasMode != 0);
                                    }

                                    boolean existSpecMode = ACLUtils.checkSpecMode(call.attr);
                                    if (changeOwn || changeGrp || changeModeRoot || changeMode || existSpecMode) {
                                        boolean printLog = changeOwn && (!changeGrp && !changeModeRoot && !changeMode);
                                        if (existSpecMode) {
                                            log.error("Currently prohibited from setting special permission bits: {}", call.attr.mode);
                                        } else if (!printLog) {
                                            //只有changeOwn触发时，消除打印；原生nfs执行vi时，中间过渡文件4913可能无权限，但不影响vi，因此
                                            //消除该情况下的报错
                                            log.error("user {} is attempting to modify the owner or group of {}:{}, chgOwn: {}, chgGrp: {}, chgRoot: {}, chgMode: {}", uid, reqHeader.bucket, inode.getObjName(), changeOwn, changeGrp, changeModeRoot, changeMode);
                                        }

                                        reply.status = NFS3ERR_PERM;
                                        reply.followsOld = 0;
                                        reply.followsNew = 0;
                                        return Mono.just(reply);
                                    }
                                }

                                return nodeInstance.setAttr(call.fh.ino, reqHeader.bucket, call.attr, reqHeader.bucketInfo.get(BUCKET_USER_ID))
                                        .map(i -> {
                                            if (!isError(i)) {
                                                reply.attr = FAttr3.mapToAttr(i, call.fh.fsid);
                                                reply.status = 0;
                                            } else {
                                                reply.status = EIO;
                                                reply.followsNew = 0;
                                                log.error("update inode fail.....bucket:{},nodeId:{}: {}", reqHeader.bucket, call.fh.ino, i.getLinkN());
                                            }
                                            return reply;
                                        });
                            });
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_NULL)
    public Mono<RpcReply> proNull(RpcCallHeader callHeader, ReqInfo reqHeader, NullCall call) {
        NullReply rpcReply = new NullReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        return Mono.just(rpcReply);
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_PATHCONF)
    public Mono<RpcReply> pathConf(RpcCallHeader callHeader, ReqInfo reqHeader, PathConfCall call) {
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        NFSACL.nfsSquash(reqHeader, callHeader);
        PathConfReply rpcReply = new PathConfReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            rpcReply.status = NFS3ERR_STALE;
            return Mono.just(rpcReply);
        }
        rpcReply.status = 0;
        rpcReply.linkMax = 255;
        rpcReply.nameMax = 255;

        rpcReply.noTrunc = 0;

        rpcReply.chownRestricted = 1;
        rpcReply.caseInsensitive = 0;
        rpcReply.casePreserving = 1;

        return Mono.just(rpcReply);
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_LOOKUP)
    public Mono<RpcReply> lookup(RpcCallHeader callHeader, ReqInfo reqHeader, EntryInCall call) {
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        LookupReply lookupReply = new LookupReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            lookupReply.status = NFS3ERR_STALE;
            return Mono.just(lookupReply);
        }
        lookupReply.fh = new FH2();

        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_lookup.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_lookup.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> nodeInstance.getInode(reqHeader.bucket, call.fh.ino)))
                .flatMap(dirInode -> {
                    if (isError(dirInode)) {
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, call.fh.ino, dirInode.getLinkN());
                        lookupReply.dirStat = FAttr3.mapToAttr(dirInode, call.fh.fsid);
                        return Mono.just(dirInode);
                    }
                    byte[] name = call.name;
                    String objName;
                    lookupReply.dirStat = FAttr3.mapToAttr(dirInode, call.fh.fsid);
                    if (name.length == 2 && name[0] == '.') {
                        return Mono.just(dirInode);
                    } else if (name.length == 3 && name[0] == '.' && name[1] == '.') {
                        //root 没有parent
                        if (dirInode.getNodeId() == 1) {
                            return Mono.just(dirInode);
                        } else {
                            String dirName = dirInode.getObjName();
                            dirName = dirName.substring(0, dirName.length() - 1);
                            dirName = dirName.substring(0, dirName.lastIndexOf("/") + 1);
                            objName = dirName;
                        }
                    } else {
                        objName = dirInode.getObjName() + new String(call.name, 0, call.name.length);
                    }

                    return NFSACL.judgeNFSOptAccess(dirInode, reqHeader, callHeader, false, null, 0)
                            .flatMap(pass -> {
                                if (!pass) {
                                    return Mono.just(NO_PERMISSION_INODE);
                                }

                                return RedLockClient.lock(reqHeader, objName, LockType.READ, true, false)
                                        .flatMap(lockRes -> FsUtils.lookup(reqHeader.bucket, objName, reqHeader, false, dirInode.getNodeId(), dirInode.getACEs()));
                            });
                })
                .map(inode -> {
                    if (NOT_FOUND_INODE.equals(inode)) {
                        lookupReply.status = ENOENT;
                    } else if (ERROR_INODE.equals(inode)) {
                        lookupReply.status = EIO;
                    } else if (NO_PERMISSION_INODE.equals(inode)) {
                        lookupReply.status = NFS3ERR_ACCES;
                    } else {
                        lookupReply.status = OK;
                        lookupReply.fh = LookupReply.mapToFH2(inode, call.fh.fsid);
                        lookupReply.stat = FAttr3.mapToAttr(inode, call.fh.fsid);
                    }

                    return lookupReply;
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_ACCESS)
    public Mono<RpcReply> access(RpcCallHeader callHeader, ReqInfo reqHeader, AccessCall call) {
        AccessReply accessReply = new AccessReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        accessReply.stat = new FAttr3();
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);

        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            accessReply.status = NFS3ERR_STALE;
            accessReply.attrBefore = 0;
            return Mono.just(accessReply);
        }
        return NFSBucketInfo.isFsidExist(call.fh.fsid)
                .flatMap(b -> {
                    if (!b) {
                        return Mono.just(STALE_HANDLE_INODE);
                    }
                    return nodeInstance.getInode(reqHeader.bucket, call.fh.ino);
                })
                .flatMap(inode -> {
                    if (NOT_FOUND_INODE.equals(inode)) {
                        accessReply.status = ENOENT;
                        accessReply.attrBefore = 0;
                        return Mono.just(accessReply);
                    } else if (ERROR_INODE.equals(inode)) {
                        accessReply.status = EIO;
                        accessReply.attrBefore = 0;
                        return Mono.just(accessReply);
                    } else if (inode.getLinkN() == STALE_HANDLE_INODE.getLinkN()) {
                        accessReply.attrBefore = 0;
                        accessReply.status = NFS3ERR_STALE;
                        return Mono.just(accessReply);
                    } else {
                        accessReply.status = OK;
                        accessReply.stat = FAttr3.mapToAttr(inode, call.fh.fsid);
                        return NFSACL.getNFSAccess(inode, reqHeader, callHeader)
                                .map(right -> {
                                    accessReply.access = call.access & right;
                                    return accessReply;
                                });
                    }
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_CREATE)
    public Mono<RpcReply> create(RpcCallHeader callHeader, ReqInfo reqHeader, CreateCall call) {
        reqHeader.bucket = getBucketName(call.dirFh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        EntryOutReply entryOutReply = new EntryOutReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        call.attr.mode |= S_IFREG;
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            entryOutReply.status = NFS3ERR_STALE;
            entryOutReply.attrFollow = 0;
            entryOutReply.objFhFollows = 0;
            return Mono.just(entryOutReply);
        }

        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_create.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_create.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> nodeInstance.getInode(reqHeader.bucket, call.dirFh.ino)))
                .flatMap(dirInode -> {
                    if (InodeUtils.isError(dirInode)) {
                        entryOutReply.status = NfsErrorNo.NFS3ERR_I0;
                        entryOutReply.attrFollow = 0;
                        entryOutReply.objFhFollows = 0;
                        log.info("get inode fail.bucket:{}, nodeId:{}", reqHeader.bucket, dirInode.getNodeId());
                        return Mono.just(dirInode);
                    }

                    return NFSACL.judgeNFSOptAccess(dirInode, reqHeader, callHeader, true, null, ACLUtils.setDirOrFile(false))
                            .flatMap(pass -> {
                                if (!pass) {
                                    entryOutReply.status = NFS3ERR_ACCES;
                                    entryOutReply.objFhFollows = 0;
                                    entryOutReply.attrFollow = 0;
                                    return Mono.just(dirInode);
                                }

                                return InodeUtils.nfsCreate(reqHeader, dirInode, call.attr.mode | S_IFREG, call.name, entryOutReply, call.dirFh.fsid, callHeader, null);
                            });
                })
                .map(inode -> entryOutReply);
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_MKDIR)
    public Mono<RpcReply> mkdir(RpcCallHeader callHeader, ReqInfo reqHeader, MkdirCall call) {
        reqHeader.bucket = getBucketName(call.dirFh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        EntryOutReply entryOutReply = new EntryOutReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            entryOutReply.status = NFS3ERR_STALE;
            entryOutReply.attrFollow = 0;
            entryOutReply.objFhFollows = 0;
            return Mono.just(entryOutReply);
        }
        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_mkdir.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_mkdir.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> nodeInstance.getInode(reqHeader.bucket, call.dirFh.ino)))
                .flatMap(dirInode -> {
                    if (InodeUtils.isError(dirInode)) {
                        entryOutReply.status = NfsErrorNo.NFS3ERR_I0;
                        entryOutReply.attrFollow = 0;
                        entryOutReply.objFhFollows = 0;
                        log.info("get inode fail.bucket:{}, nodeId:{}", reqHeader.bucket, dirInode.getNodeId());
                        return Mono.just(dirInode);
                    }

                    return NFSACL.judgeNFSOptAccess(dirInode, reqHeader, callHeader, true, null, ACLUtils.setDirOrFile(true))
                            .flatMap(pass -> {
                                if (!pass) {
                                    entryOutReply.status = NFS3ERR_ACCES;
                                    entryOutReply.objFhFollows = 0;
                                    entryOutReply.attrFollow = 0;
                                    return Mono.just(dirInode);
                                }

                                return InodeUtils.nfsCreate(reqHeader, dirInode, call.attr.mode | S_IFDIR, call.name, entryOutReply, call.dirFh.fsid, callHeader, null);
                            });
                })
                .map(inode -> entryOutReply);


    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_MKNOD)
    public Mono<RpcReply> mknod(RpcCallHeader callHeader, ReqInfo reqHeader, MkNodCall call) {
        reqHeader.bucket = getBucketName(call.dirFh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        EntryOutReply entryOutReply = new EntryOutReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            entryOutReply.status = NFS3ERR_STALE;
            entryOutReply.attrFollow = 0;
            entryOutReply.objFhFollows = 0;
            return Mono.just(entryOutReply);
        }
        boolean[] onlyRootExec = {true};
        switch (call.type) {
            case FileType.NF_CHR:
                call.attr.mode |= S_IFCHR;
                break;
            case FileType.NF_BLK:
                call.attr.mode |= S_IFBLK;
                break;
            case FileType.NF_FIFO:
                call.attr.mode |= S_IFFIFO;
                onlyRootExec[0] = false;
                break;
            case FileType.NF_SOCK:
                call.attr.mode |= S_IFSOCK;
                onlyRootExec[0] = false;
                break;
            default:
                onlyRootExec[0] = false;
                break;
        }
        return nodeInstance.getInode(reqHeader.bucket, call.dirFh.ino)
                .flatMap(dirInode -> {
                    if (InodeUtils.isError(dirInode)) {
                        entryOutReply.status = NfsErrorNo.NFS3ERR_I0;
                        entryOutReply.attrFollow = 0;
                        entryOutReply.objFhFollows = 0;
                        log.info("get inode fail.bucket:{}, nodeId:{}", reqHeader.bucket, dirInode.getNodeId());
                        return Mono.just(dirInode);
                    }

                    return NFSACL.judgeNFSOptAccess(dirInode, reqHeader, callHeader, true, null, ACLUtils.setDirOrFile(false))
                            .flatMap(pass -> {
                                int uid = ACLUtils.getUidAndGid(callHeader)[0];
                                if (!pass || (onlyRootExec[0] && uid != 0)) {
                                    entryOutReply.status = NFS3ERR_ACCES;
                                    entryOutReply.objFhFollows = 0;
                                    entryOutReply.attrFollow = 0;
                                    log.info("mknod fail, no such permission, uid: {}, type: {}", uid, call.type);
                                    return Mono.just(dirInode);
                                }

                                return InodeUtils.nfsCreate(reqHeader, dirInode, call.attr.mode, call.name, entryOutReply, call.dirFh.fsid, callHeader, call);
                            });
                })
                .map(inode -> entryOutReply);
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_REMOVE)
    public Mono<RpcReply> remove(RpcCallHeader callHeader, ReqInfo reqHeader, EntryInCall call) {
        return remove0(callHeader, reqHeader, call, false);
    }

    public Mono<RpcReply> remove0(RpcCallHeader callHeader, ReqInfo reqHeader, EntryInCall call, boolean isRmDir) {
        long start = System.currentTimeMillis();
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        RemoveReply removeReply = new RemoveReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            removeReply.status = NFS3ERR_STALE;
            return Mono.just(removeReply);
        }
        String bucketName = reqHeader.bucket;
        String[] objName = new String[1];
        boolean[] isRmDirErr = new boolean[1];
        Inode[] dirInodes = new Inode[1];
        long optTime = System.currentTimeMillis();
        boolean[] isRepeat = new boolean[1];
        boolean esSwitch = ES_ON.equals(reqHeader.bucketInfo.get(ES_SWITCH));
        String name = (isRmDir ? fs_rmdir : fs_remove).name();
        Map<String, String> removeAclParam = new HashMap<>(1);
        removeAclParam.put(NFS_DIR_SUB_DEL_ACL, NOT_CONTAIN_ACL);
        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, name + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, name + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> Mono.delay(Duration.ofMillis(waitMillis)).flatMap(l -> nodeInstance.getInode(reqHeader.bucket, call.fh.ino)))
                .flatMap(dirInode -> {
                    if (isError(dirInode)) {
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, call.fh.ino, dirInode.getLinkN());
                        return Mono.just(dirInode);
                    }

                    dirInodes[0] = dirInode;
                    objName[0] = dirInode.getObjName() + new String(call.name, 0, call.name.length);
                    isRepeat[0] = isRequestRepeat(objName[0], optTime);

                    return NFSACL.judgeNFSOptAccess(dirInode, reqHeader, callHeader, true, removeAclParam, 0)
                            .flatMap(pass -> {
                                if (!pass) {
                                    return Mono.just(NO_PERMISSION_INODE);
                                }

                                return RedLockClient.lock(reqHeader, objName[0], LockType.WRITE, true, false)
                                        .flatMap(lock -> FsUtils.lookup(bucketName, objName[0], reqHeader, false, -1, null));
                            });
                })
                .flatMap(newInode -> {
                    if (isError(newInode) || newInode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        return Mono.just(newInode);
                    }
                    //cifs判断待删除的Inode本身是否具有删除权限，若父目录已具备删除子文件和子文件夹的权限，则无需再判断该权限
                    if (NOT_CONTAIN_ACL.equals(removeAclParam.get(NFS_DIR_SUB_DEL_ACL)) && !NFSACL.judgeCifsRemove(reqHeader.bucketInfo, newInode, callHeader)) {
                        return Mono.just(NO_PERMISSION_INODE);
                    }

                    if (newInode.getObjName().endsWith("/")) {
                        String prefix = newInode.getObjName();
                        return ReadDirCache.listAndCache(reqHeader.bucket, prefix, 0, 1024, reqHeader.nfsHandler, dirInodes[0].getNodeId(), null, dirInodes[0].getACEs())
                                .flatMap(list1 -> {
                                    if (!list1.isEmpty()) {
                                        isRmDirErr[0] = true;
                                        return Mono.just(ERROR_INODE);
                                    }
                                    return Mono.just(newInode);
                                });
                    }
                    return Mono.just(newInode);
                })
                .flatMap(inode -> {
                    if (NOT_FOUND_INODE.equals(inode)) {
                        removeReply.status = ENOENT;
                        if (isRepeat[0]) {
                            removeReply.status = NfsErrorNo.NFS3_OK;
                        }
                        return Mono.just(removeReply);
                    } else if (ERROR_INODE.equals(inode)) {
                        removeReply.status = EIO;
                        if (isRmDirErr[0]) {
                            removeReply.status = NfsErrorNo.NFS3ERR_NOTEMPTY;
                        }
                        return Mono.just(removeReply);
                    } else if (inode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        removeReply.status = NFS3ERR_ACCES;
                        return Mono.just(removeReply);
                    } else {
                        return nodeInstance.deleteInode(inode.getNodeId(), bucketName, objName[0])
                                .flatMap(inode1 -> {
                                    return nodeInstance.updateInodeTime(dirInodes[0].getNodeId(), bucketName, inode1.getCtime(), inode1.getCtimensec(), false, true, true)
                                            .map(i0 -> {
                                                if (isError(inode1)) {
                                                    removeReply.status = EIO;
                                                    if (inode1.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                                        removeReply.status = ENOENT;
                                                    }
                                                } else {
                                                    long end = System.currentTimeMillis();
                                                    if (end - start > 55_000L) {
                                                        NFSHandler.delTimeout.put(call.fh.ino, end);
                                                    }
                                                    removeReply.status = OK;
                                                }
                                                return removeReply;
                                            }).flatMap(i -> {
                                                if (esSwitch && !isError(inode1)) {
                                                    return EsMetaTask.delEsMeta(inode1.clone().setObjName(objName[0]), inode.clone().setObjName(objName[0]), bucketName, objName[0], inode.getNodeId(), true)
                                                            .map(b0 -> removeReply);
                                                }
                                                return Mono.just(removeReply);
                                            });
                                });
                    }
                })
                .map(res -> (RpcReply) (res))
                .doFinally(s -> {
                    deleteRequestInodeTimeMap(objName[0], optTime);
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_RMDIR)
    public Mono<RpcReply> rmDir(RpcCallHeader callHeader, ReqInfo reqHeader, EntryInCall call) {
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        byte[] dirName = new byte[call.len + 1];
        System.arraycopy(call.name, 0, dirName, 0, call.len);
        dirName[call.len] = '/';
        call.name = new byte[call.len + 1];
        call.name = dirName.clone();
        return remove0(callHeader, reqHeader, call, true);
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_READ, buf = 11 << 2)
    public Mono<RpcReply> read(RpcCallHeader callHeader, ReqInfo reqHeader, ReadCall call) {
        reqHeader.bucket = getBucketName(call.fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        ReadReply readReply = new ReadReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            readReply.status = NFS3ERR_STALE;
            readReply.attrFollows = 0;
            return Mono.just(readReply);
        }
        Inode[] accessInode = new Inode[1];
        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_read.name() + "-" + THROUGHPUT_QUOTA, 1L)
                .flatMap(waitMillis -> {
                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_read.name() + "-" + THROUGHPUT_QUOTA, 1L).map(waitMillis2 -> waitMillis + waitMillis2);
                })
                .flatMap(waitMillis -> waitMillis == 0 ? Mono.just(true) : Mono.delay(Duration.ofMillis(waitMillis)))
                .flatMap(l -> RedLockClient.lockDir(reqHeader, call.fh.ino, LockType.READ, true))
                .flatMap(lock -> nodeInstance.getInode(reqHeader.bucket, call.fh.ino))
                .flatMap(inode -> {
                    if (isError(inode)) {
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, call.fh.ino, inode.getLinkN());
                        readReply.status = NfsErrorNo.NFS3ERR_I0;
                        readReply.attrFollows = 0;
                        readReply.eof = 1;
                        readReply.data = new byte[0];
                        return Mono.just(readReply);
                    }
                    accessInode[0] = inode.clone();

                    MonoProcessor<ReadReply> res = MonoProcessor.create();
                    List<Inode.InodeData> inodeData = inode.getInodeData();
                    if (inodeData.isEmpty()) {
                        if (inode.getSize() <= 0) {//处理0kb大小的文件
                            readReply.status = 0;
                        } else {
                            readReply.status = EIO;
                        }

                        readReply.eof = 1;
                        readReply.attrFollows = 0;
                        readReply.data = new byte[0];
                        return Mono.just(readReply);
                    }

                    long cur = 0;
                    long readOffset = call.offset;
                    long readEnd = call.offset + call.size;
                    long inodeEnd = inode.getSize();
                    if (readEnd >= inodeEnd) {
                        readEnd = inodeEnd;
                        readReply.eof = 1;
                        call.size = (int) (readEnd - call.offset);
                    }

                    if (call.size <= 0) {
                        readReply.status = 0;
                        readReply.eof = 1;
                        readReply.attrFollows = 0;
                        readReply.data = new byte[0];
                        return Mono.just(readReply);
                    }

                    if (inode.getNodeId() < VersionUtil.V3_0_6_FS_NODE_ID) {
                        //旧数据走旧读流程
                        int readN = 0;
                        byte[] bytes = new byte[call.size];

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

                                flux = flux.mergeWith(FsUtils.readObj(readN, data.storage, reqHeader.bucket,
                                        data.fileName, readFileOffset, readFileSize, data.size));

                                readOffset += readFileSize;
                                readN += readFileSize;

                                if (readOffset >= readEnd) {
                                    break;
                                }

                                cur = curEnd;
                            }
                        }

                        readReply.attr = FAttr3.mapToAttr(inode, call.fh.fsid);
                        flux.flatMap(t -> BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_read.name() + "-" + BAND_WIDTH_QUOTA, call.size)
                                        .flatMap(waitMillis -> {
                                            String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                                            return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_read.name() + "-" + BAND_WIDTH_QUOTA, call.size).map(waitMillis2 -> waitMillis + waitMillis2);
                                        })
                                        .flatMap(waitMillis -> Mono.just(t).delayElement(Duration.ofMillis(waitMillis)))
                                ).doOnNext(t -> {
                                    System.arraycopy(t.var2, 0, bytes, t.var1, t.var2.length);
                                })
                                .doOnError(e -> {
                                    log.error("", e);
                                    readReply.status = EIO;
                                    readReply.attrFollows = 0;
                                    readReply.data = new byte[0];
                                    readReply.count = 0;
                                    res.onNext(readReply);
                                })
                                .doOnComplete(() -> {
                                    readReply.status = 0;
                                    readReply.data = bytes;
                                    InodeUtils.updateInodeAtime(inode);
                                    readReply.count = readReply.data.length;
                                    res.onNext(readReply);
                                }).subscribe();

                        return res;
                    } else {
                        //新数据走新读流程
                        readReply.attr = FAttr3.mapToAttr(inode, call.fh.fsid);
                        return BucketFSPerfLimiter.getInstance().limits(reqHeader.bucket, fs_read.name() + "-" + BAND_WIDTH_QUOTA, call.size)
                                .flatMap(waitMillis -> {
                                    String redisKey = getAddressPerfRedisKey(reqHeader.nfsHandler.getClientAddress(), reqHeader.bucket);
                                    return AddressFSPerfLimiter.getInstance().limits(redisKey, fs_read.name() + "-" + BAND_WIDTH_QUOTA, call.size).map(waitMillis2 -> waitMillis + waitMillis2);
                                })
                                .flatMap(waitMillis -> waitMillis == 0 ? Mono.just(true) : Mono.delay(Duration.ofMillis(waitMillis)))
                                .flatMapMany(l -> ReadObjCache.readObj(inode, call.offset, call.size))
                                .collectList()
                                .map(byteList -> {
                                    byte[] bytes;
                                    int size = 0;
                                    if (byteList.size() == 1) {
                                        bytes = byteList.get(0).var2;
                                        size = bytes.length;
                                    } else {
                                        bytes = new byte[call.size];
                                        for (Tuple2<Integer, byte[]> tuple : byteList) {
                                            System.arraycopy(tuple.var2, 0, bytes, tuple.var1, tuple.var2.length);
                                            size += tuple.var2.length;
                                        }
                                    }

                                    if (size != call.size) {
                                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "read size not match");
                                    }

                                    readReply.status = 0;
                                    readReply.data = bytes;
                                    InodeUtils.updateInodeAtime(inode);
                                    readReply.count = readReply.data.length;

                                    return readReply;
                                })
                                .onErrorResume(e -> {
                                    if (null != e && null != e.getMessage() && e.getMessage().contains("pre-read data modified")) {
                                        if (readCacheDebug) {
                                            log.error("", e);
                                        }
                                    } else {
                                        log.error("", e);
                                    }
                                    readReply.status = EIO;
                                    readReply.attrFollows = 0;
                                    readReply.data = new byte[0];
                                    readReply.count = 0;
                                    return Mono.just(readReply);
                                });
                    }
                })
                .map(res -> (RpcReply) (res))
                .doFinally(r -> {
                    fileFsAccessHandle(accessInode[0]);
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_LINK)
    public Mono<RpcReply> link(RpcCallHeader callHeader, ReqInfo reqHeader, LinkCall call) {
        reqHeader.bucket = getBucketName(call.srcFh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        LinkReply linkReply = new LinkReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            linkReply.status = NFS3ERR_STALE;
            linkReply.attrFollows = 0;
            return Mono.just(linkReply);
        }
        String bucket = reqHeader.bucket;
        Inode[] inodes = new Inode[]{ERROR_INODE, ERROR_INODE};
        boolean[] isDirInode = new boolean[]{false};
        return RedLockClient.lockDir(reqHeader, call.srcParentDirFh.ino, LockType.WRITE, true)
                .flatMap(l -> nodeInstance.getInode(bucket, call.srcParentDirFh.ino))
                .flatMap(dirInode -> {
                    if (isError(dirInode)) {
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, call.srcParentDirFh.ino, dirInode.getLinkN());
                        return Mono.just(dirInode);
                    }

                    return NFSACL.judgeNFSOptAccess(dirInode, reqHeader, callHeader, true, null, 0)
                            .flatMap(pass -> {
                                if (!pass) {
                                    return Mono.just(NO_PERMISSION_INODE);
                                }

                                return nodeInstance.getInode(bucket, call.srcFh.ino)
                                        .flatMap(oldInode -> {
                                            if (isError(oldInode)) {
                                                return Mono.just(oldInode);
                                            }
                                            inodes[0] = oldInode;
                                            byte[] buf = call.name;
                                            String name = dirInode.getObjName() + new String(buf, 0, buf.length);

                                            if (name.getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH) {
                                                return Mono.just(NAME_TOO_LONG_INODE);
                                            }
                                            if (inodes[0].getObjName().endsWith("/") || (inodes[0].getMode() & S_IFMT) == S_IFDIR) {
                                                isDirInode[0] = true;
                                                return Mono.just(oldInode);
                                            }

                                            return NFSACL.judgeUGOAndACL(oldInode, callHeader, reqHeader.bucketInfo, null)
                                                    .flatMap(pass0 -> {
                                                        if (!pass0) {
                                                            return Mono.just(NO_PERMISSION_INODE);
                                                        }

                                                        return nodeInstance.createHardLink(bucket, oldInode.getNodeId(), name);
                                                    });
                                        });
                            });
                })
                .flatMap(inode -> {
                    inodes[1] = inode;
                    if (isError(inode)) {
                        log.error("create hard link inode fail... {}", inode.getLinkN());
                        linkReply.status = EIO;
                        linkReply.attrFollows = 0;
                    } else if (inode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        linkReply.status = NFS3ERR_ACCES;
                        linkReply.attrFollows = 0;
                    } else if (NAME_TOO_LONG_INODE.equals(inode)) {
                        linkReply.status = NfsErrorNo.NFS3ERR_NAMETOOLONG;
                        linkReply.attrFollows = 0;
                    } else if (FILES_QUOTA_EXCCED_INODE.getLinkN() == inode.getLinkN()) {
                        linkReply.attrFollows = 0;
                        linkReply.status = NfsErrorNo.NFS3ERR_DQUOT;
                    } else if (isDirInode[0]) {
                        linkReply.status = NfsErrorNo.NFS3ERR_ISDIR;
                        linkReply.attrFollows = 0;
                    } else {
                        linkReply.status = OK;
                        linkReply.attr = FAttr3.mapToAttr(inode, call.srcParentDirFh.fsid);
                    }
                    if (linkReply.status == NfsErrorNo.NFS3_OK) {
                        return nodeInstance.updateInodeTime(call.srcParentDirFh.ino, bucket, System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), false, true, true);
                    }
                    return Mono.just(inode);
                })
                .flatMap(i -> {
                    if (ES_ON.equals(reqHeader.bucketInfo.get(ES_SWITCH)) && !isError(inodes[1]) && !NAME_TOO_LONG_INODE.equals(inodes[1])) {
                        return EsMetaTask.putLinkEsMeta(inodes[1], inodes[0]).map(b0 -> linkReply);
                    }
                    return Mono.just(linkReply);
                })
                .map(i -> linkReply);

    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_SYMLINK)
    public Mono<RpcReply> symLink(RpcCallHeader callHeader, ReqInfo reqHeader, SymLinkCall call) {
        reqHeader.bucket = getBucketName(call.dirFh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        EntryOutReply symLinkReply = new EntryOutReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            symLinkReply.status = NFS3ERR_STALE;
            symLinkReply.attrFollow = 0;
            symLinkReply.objFhFollows = 0;
            return Mono.just(symLinkReply);
        }
        String bucket = reqHeader.bucket;
        return nodeInstance.getInode(bucket, call.dirFh.ino)
                .flatMap(dirInode -> {
                    if (isError(dirInode)) {
                        log.info("get inode fail.bucket:{}, nodeId:{}: {}", reqHeader.bucket, call.dirFh.ino, dirInode.getLinkN());
                        return Mono.just(dirInode);
                    }

                    String name = dirInode.getObjName() + new String(call.linkName);
                    String reference = new String(call.referenceName);
                    int cifsMode = FILE_ATTRIBUTE_ARCHIVE;
                    if (name.endsWith("/")) {
                        cifsMode = FILE_ATTRIBUTE_DIRECTORY;
                    }

                    if (name.getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH) {
                        return Mono.just(NAME_TOO_LONG_INODE);
                    }

                    int finalCifsMode = cifsMode;
                    return NFSACL.judgeNFSOptAccess(dirInode, reqHeader, callHeader, true, null, 0)
                            .flatMap(pass -> {
                                if (!pass) {
                                    return Mono.just(NO_PERMISSION_INODE);
                                }

                                Map<String, String> parameter = new HashMap<>();
                                if (null != dirInode.getACEs() && !dirInode.getACEs().isEmpty()) {
                                    parameter.put(NFS_ACE, Json.encode(dirInode.getACEs()));
                                }

                                return InodeUtils.create(reqHeader, call.linkAttr.mode | S_IFLNK, finalCifsMode, name, reference, -1, "", null, parameter, callHeader)
                                        .flatMap(res -> {
                                            if (isError(res)) {
                                                return Mono.just(res);
                                            }
                                            return Node.getInstance().updateInodeTime(dirInode.getNodeId(), reqHeader.bucket, res.getMtime(), res.getMtimensec(), false, true, true)
                                                    .map(i -> res);
                                        });
                            });
                })
                .map(inode -> {
                    if (isError(inode)) {
                        log.error("create symlink inode fail...bucket:{},dirInodeId:{}: {}", bucket, call.dirFh.ino, inode.getLinkN());
                        symLinkReply.status = NfsErrorNo.NFS3ERR_I0;
                        symLinkReply.objFhFollows = 0;
                        symLinkReply.attrFollow = 0;
                    } else if (inode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        symLinkReply.status = NFS3ERR_ACCES;
                        symLinkReply.objFhFollows = 0;
                        symLinkReply.attrFollow = 0;
                    } else if (NAME_TOO_LONG_INODE.equals(inode)) {
                        symLinkReply.status = NfsErrorNo.NFS3ERR_NAMETOOLONG;
                        symLinkReply.objFhFollows = 0;
                        symLinkReply.attrFollow = 0;
                    } else {
                        symLinkReply.status = NfsErrorNo.NFS3_OK;
                        symLinkReply.fh = FH2.mapToFH2(inode, call.dirFh.fsid);
                        symLinkReply.attr = FAttr3.mapToAttr(inode, call.dirFh.fsid);
                    }
                    return symLinkReply;
                });
    }

    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_READLINK)
    public Mono<RpcReply> readLink(RpcCallHeader callHeader, ReqInfo reqHeader, FH2 fh) {
        reqHeader.bucket = getBucketName(fh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        ReadLinkReply readLinkReply = new ReadLinkReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            readLinkReply.status = NFS3ERR_STALE;
            readLinkReply.attrFollows = 0;
            return Mono.just(readLinkReply);
        }
        String bucket = reqHeader.bucket;
        return nodeInstance.getInode(bucket, fh.ino)
                .flatMap(inode -> {
                    if ((inode.getMode() & S_IFMT) == S_IFLNK) {
                        return NFSACL.judgeNFSOptAccess(inode, reqHeader, callHeader, false, null, 0)
                                .map(pass -> {
                                    if (!pass) {
                                        readLinkReply.status = NFS3ERR_ACCES;
                                        readLinkReply.attrFollows = 0;
                                        readLinkReply.data = new byte[0];
                                        readLinkReply.dataLen = 0;
                                        return readLinkReply;
                                    } else {
                                        String link = inode.getReference();
                                        readLinkReply.status = NfsErrorNo.NFS3_OK;
                                        readLinkReply.data = link.getBytes();
                                        readLinkReply.attr = FAttr3.mapToAttr(inode, fh.fsid);
                                        readLinkReply.dataLen = readLinkReply.data.length;
                                        return readLinkReply;
                                    }
                                });
                    } else {
                        log.error("create symlink inode fail...bucket:{},dirInodeId:{}", bucket, fh.ino);
                        readLinkReply.status = NfsErrorNo.NFS3ERR_I0;
                        readLinkReply.attrFollows = 0;
                        readLinkReply.data = new byte[0];
                        readLinkReply.dataLen = 0;
                        return Mono.just(readLinkReply);
                    }
                });
    }

    /**
     * 重命名文件：
     * 1) 新的文件名不存在，则直接在磁盘中修改，并更新缓存中的内容
     * 2) 新的文件名已经存在，此时会提示是否选择覆盖源文件：若否则不会触发重命名功能；
     * 若是会让被改名的文件覆盖已存在的文件；先删除被覆盖的文件，再rename旧文件成新文件
     * <p>
     * 重命名目录：
     * 1) 遍历该目录下的所有文件与子目录，将所有Inode单独重命名
     * 2) 新的目录名已经存在，且存在的目录名下面无与就目录名相同的子目录名，则将旧目录以旧名字的形式搬到已存在的目录下
     * 3) 新的目录名已经存在，且存在的目录名下面有与旧目录名相同的子目录名，若该子目录不为空，则rename失败；
     * 若该子目录为空，则将旧目录以旧名字的形式搬到已存在的目录下
     * <p>
     * 关于重命名时访问时间的修改：被rename inode本身ctime需改，还需更改父目录的mtime和ctime
     * 如果是把目录搬移至另一个目录下，则新旧父级目录均需要更新时间 /nfs  /nfs/dir1  /nfs/dir2/dir1
     * 1) 若newInode不存在父级目录，则仅修改newInode本身的ctime
     * 2) 若newInode存在父级目录，则不仅修改newInode本身的ctime，还需修改父级目录的mtime和ctime
     **/
    @NFSV3.Opt(value = NFSV3.Opcode.NFS3PROC_RENAME)
    public Mono<RpcReply> rename(RpcCallHeader callHeader, ReqInfo reqHeader, ReNameCall call) {
        reqHeader.bucket = getBucketName(call.fromDirFh.fsid);
        FSPerformanceService.addIp(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress());
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(reqHeader.bucket);
        String bucket = reqHeader.bucket;
        ReNameReply reNameReply = new ReNameReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        if (!IpWhitelistUtils.checkNFSWhitelist(reqHeader.bucket, reqHeader.nfsHandler.getClientAddress())) {
            reNameReply.status = NFS3ERR_STALE;
            reNameReply.fromBeforeFollows = 0;
            reNameReply.fromAfterFollows = 0;
            reNameReply.toBeforeFollows = 0;
            reNameReply.toAfterFollows = 0;
            return Mono.just(reNameReply);
        }

        AtomicReference<String> oldObjectName = new AtomicReference<>();
        AtomicReference<String> newObjName = new AtomicReference<>();

        AtomicBoolean dir = new AtomicBoolean(false);
        AtomicBoolean overWrite = new AtomicBoolean(false);
        Inode[] dirInodes = new Inode[2];
        long optTime = System.currentTimeMillis();
        return Flux.just(new Tuple2<>(true, call.fromDirFh.ino), new Tuple2<>(false, call.toDirFh.ino))
                .flatMap(t -> nodeInstance.getInode(bucket, t.var2)
                        .flatMap(dirInode -> {
                            if (isError(dirInode)) {
                                log.info("get inode fail.bucket:{}, from dir nodeId:{},to dir nodeId{}:{}", reqHeader.bucket, call.fromDirFh.ino, call.toDirFh.ino, dirInode.getLinkN());
                                return Mono.just(new Tuple2<>(t.var1, dirInode));
                            }

                            byte[] name = t.var1 ? call.fromName : call.toName;
                            String objName = dirInode.getObjName() + new String(name, 0, name.length);

                            if (t.var1) {
                                dirInodes[0] = dirInode;
                                oldObjectName.set(objName);
                                reNameReply.fromBeforeAttr = SimpleAttr.mapToSimpleAttr(dirInode);
                                reqHeader.repeat = isRequestRepeat(objName, optTime);
                            } else {
                                dirInodes[1] = dirInode;
                                newObjName.set(objName);
                                reNameReply.toBeforeAttr = SimpleAttr.mapToSimpleAttr(dirInode);
                            }

                            return NFSACL.judgeNFSOptAccess(dirInode, reqHeader, callHeader, true, null, 0)
                                    .flatMap(pass -> {
                                        if (!pass) {
                                            if (t.var1) {
                                                log.error("obj: {} rename fail, fromDir: {} has no permission", oldObjectName.get(), dirInode.getObjName());
                                            } else {
                                                log.error("obj: {} rename fail, toDir: {} has no permission", newObjName.get(), dirInode.getObjName());
                                            }
                                            return Mono.just(new Tuple2<>(t.var1, NO_PERMISSION_INODE));
                                        }

                                        return RedLockClient.lock(reqHeader, objName, LockType.WRITE, true, t.var1)
                                                .flatMap(lock -> FsUtils.lookup(bucket, objName, reqHeader, true, dirInode.getNodeId(), dirInode.getACEs()))
                                                .map(inode -> {
                                                    //mv dir
                                                    if (t.var1 && StringUtils.isNotEmpty(inode.getObjName()) && inode.getObjName().endsWith("/")) {
                                                        dir.set(true);
                                                    }
                                                    return new Tuple2<>(t.var1, inode);
                                                });
                                    });
                        })).collectList()
                .flatMap(list -> {
                    Inode[] oldInode = new Inode[]{ERROR_INODE};
                    Inode[] newInode = new Inode[]{ERROR_INODE};

                    for (Tuple2<Boolean, Inode> tuple2 : list) {
                        if (tuple2.var1) {
                            oldInode[0] = tuple2.var2;
                        } else {
                            newInode[0] = tuple2.var2;
                        }
                    }
                    if (debug) {
                        log.info("【rename】 old: {} {}, new: {} {}", oldInode[0].getObjName(), oldInode[0].getLinkN(), newInode[0].getObjName(), newInode[0].getLinkN());
                    }
                    if (ERROR_INODE.equals(oldInode[0]) || NOT_FOUND_INODE.equals(oldInode[0])) {
                        reNameReply.status = NfsErrorNo.NFS3ERR_I0;
                        if (NOT_FOUND_INODE.equals(oldInode[0])) {
                            reNameReply.status = NfsErrorNo.NFS3ERR_NOENT;
                            if (reqHeader.repeat) {
                                log.info("rename end callback, {}", oldObjectName.get());
                                deleteRequestInodeTimeMap(oldObjectName.get(), optTime);
                                reNameReply.status = NfsErrorNo.NFS3_OK;
                            }
                        }
                        reNameReply.fromBeforeFollows = 0;
                        reNameReply.fromAfterFollows = 0;
                        reNameReply.toBeforeFollows = 0;
                        reNameReply.toAfterFollows = 0;
                        return Mono.just(reNameReply)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }
                    if (oldInode[0].getLinkN() == NO_PERMISSION_INODE.getLinkN() || newInode[0].getLinkN() == NO_PERMISSION_INODE.getLinkN() || !NFSACL.judgeCifsRename(reqHeader.bucketInfo, oldInode[0], dirInodes[0], dirInodes[1], callHeader, dir.get())) {
                        reNameReply.status = NFS3ERR_ACCES;
                        reNameReply.fromBeforeFollows = 0;
                        reNameReply.fromAfterFollows = 0;
                        reNameReply.toBeforeFollows = 0;
                        reNameReply.toAfterFollows = 0;
                        return Mono.just(reNameReply)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }

                    // 如果新名称的Inode已存在，则应当覆盖已存在的文件；如果inode为retryInode，表示返回的是当前目录下子目录的inode，为客户端长时间未接收到响应重发的rename请求，应当直接返回
                    if (!isError(newInode[0])) {
                        if (RETRY_INODE.getLinkN() == newInode[0].getLinkN() && newInode[0].getNodeId() == -3) {
                            //正在重命名，返回结果为空
                            reNameReply.fromBeforeFollows = 0;
                            reNameReply.fromAfterFollows = 0;
                            reNameReply.toBeforeFollows = 0;
                            reNameReply.toAfterFollows = 0;
                            return Mono.just(reNameReply)
                                    .doOnNext(r2 -> releaseLock(reqHeader, r2));
                        }
                        overWrite.set(true);
                    }

                    String[] oldObjName0 = new String[1];
                    String[] newObjName0 = new String[1];

                    oldObjName0[0] = dir.get() ? oldObjectName.get() + '/' : oldObjectName.get();
                    newObjName0[0] = dir.get() ? newObjName.get() + '/' : newObjName.get();
                    if (newObjName0[0].getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH) {
                        ReNameReply.changeToErrorReply(reNameReply, NfsErrorNo.NFS3ERR_NAMETOOLONG);
                        return Mono.just(reNameReply)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }
                    // 如果更改的为目录
                    if (dir.get()) {
                        // 如果更新后的目录名已经存在，则检查这个新的目录名之下是否还有与旧目录名称相同的dirInode
                        return Mono.just(overWrite.get())
                                .flatMap(isOverWrite -> {
                                    return FSQuotaUtils.canRename(bucket, oldInode[0].getNodeId(), newInode[0])
                                            .flatMap(can -> {
                                                if (!can) {
                                                    reNameReply.status = NfsErrorNo.NFS3ERR_I0;
                                                    reNameReply.fromBeforeFollows = 0;
                                                    reNameReply.fromAfterFollows = 0;
                                                    reNameReply.toBeforeFollows = 0;
                                                    reNameReply.toAfterFollows = 0;
                                                    return Mono.just(reNameReply)
                                                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                }
                                                if (isOverWrite) {
                                                    String prefix = newInode[0].getObjName();
                                                    return ReadDirCache.listAndCache(bucket, prefix, 0, 4096, reqHeader.nfsHandler, newInode[0].getNodeId(), null, newInode[0].getACEs())
                                                            .flatMap(list1 -> {
                                                                if (!list1.isEmpty()) {
                                                                    log.error("directory already exists: {}", newObjName0[0]);
                                                                    ReNameReply.changeToErrorReply(reNameReply, NfsErrorNo.NFS3ERR_EXIST);
                                                                    return Mono.just(reNameReply)
                                                                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                                }
                                                                if (newObjName0[0].startsWith(oldInode[0].getObjName())) {
                                                                    ReNameReply.changeToErrorReply(reNameReply, NfsErrorNo.NFS3ERR_INVAL);
                                                                    return Mono.just(reNameReply)
                                                                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                                }
                                                                MonoProcessor<ReNameReply> reNameReplyRes = MonoProcessor.create();
                                                                Mono.defer(() -> scanAndReName0(oldInode[0], bucket, newObjName0[0], 0, reqHeader, reNameReply, dirInodes, call.fromDirFh.fsid))
                                                                        .doOnSubscribe(subscription -> {
                                                                            reqHeader.optCompleted = false;
                                                                        })
                                                                        .doOnNext(reply -> {
                                                                            reqHeader.optCompleted = true;
                                                                            if (!reqHeader.timeout) {
                                                                                deleteRequestInodeTimeMap(oldObjectName.get(), optTime);
                                                                            }
                                                                            reNameReplyRes.onNext(reply);
                                                                        })
                                                                        .subscribe();
                                                                return reNameReplyRes;
                                                            });
                                                } else {
                                                    // 将oldInode视为dirInode，遍历该目录下的所有子目录与文件
                                                    // 重命名时不需要再get新的inode，直接rename即可
                                                    if (newObjName0[0].startsWith(oldInode[0].getObjName())) {
                                                        ReNameReply.changeToErrorReply(reNameReply, NfsErrorNo.NFS3ERR_INVAL);
                                                        return Mono.just(reNameReply)
                                                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                    }
                                                    MonoProcessor<ReNameReply> reNameReplyRes = MonoProcessor.create();
                                                    Mono.defer(() -> scanAndReName0(oldInode[0], bucket, newObjName0[0], 0, reqHeader, reNameReply, dirInodes, call.fromDirFh.fsid))
                                                            .doOnSubscribe(subscription -> {
                                                                reqHeader.optCompleted = false;
                                                            })
                                                            .doOnNext(reply -> {
                                                                reqHeader.optCompleted = true;
                                                                if (!reqHeader.timeout) {
                                                                    deleteRequestInodeTimeMap(oldObjectName.get(), optTime);
                                                                }
                                                                reNameReplyRes.onNext(reply);
                                                            })
                                                            .subscribe();
                                                    return reNameReplyRes;
                                                }
                                            });
                                });
                    } else {
                        // 如果更改的为文件
                        return renameFile(oldInode[0].getNodeId(), newInode[0], oldObjName0[0], newObjName0[0], bucket, overWrite.get(), new AtomicInteger(0))
                                .flatMap(inode -> {
                                    if (isError(inode) || inode.isDeleteMark()) {
                                        if (NOT_FOUND_INODE.getLinkN() == inode.getLinkN() || inode.isDeleteMark()) {
                                            reNameReply.status = NfsErrorNo.NFS3ERR_NOENT;
                                        }
                                        if (ERROR_INODE.getLinkN() == inode.getLinkN()) {
                                            reNameReply.status = NfsErrorNo.NFS3ERR_I0;
                                        }
                                        reNameReply.fromBeforeFollows = 0;
                                        reNameReply.fromAfterFollows = 0;
                                        reNameReply.toBeforeFollows = 0;
                                        reNameReply.toAfterFollows = 0;
                                        return Mono.just(reNameReply)
                                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                    }
                                    return Mono.just(inode)
                                            .flatMap(i -> updateTime(dirInodes, reNameReply, bucket))
                                            .flatMap(r -> updateFAttr3(dirInodes, r, call.fromDirFh.fsid))
                                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                });

                    }
                })
                .map(res -> (RpcReply) res)
                .doFinally(s -> {
                    if (reqHeader.optCompleted && !reqHeader.repeat) {
                        deleteRequestInodeTimeMap(oldObjectName.get(), optTime);
                    } else {
                        log.info("rename running {}", oldObjectName.get());
                    }
                    reqHeader.timeout = true;
                });
    }

    public Mono<ReNameReply> releaseLock(ReqInfo reqHeader, ReNameReply reNameReply) {
        if (!reqHeader.lock.isEmpty()) {
            for (String key : reqHeader.lock.keySet()) {
                String value = reqHeader.lock.get(key);
                RedLockClient.unlock(reqHeader.bucket, key, value, true).subscribe();
            }
        }
        return Mono.just(reNameReply);
    }

    public Mono<ReNameReply> updateFAttr3(Inode[] dirInodes, ReNameReply reNameReply, long fsid) {
        return Mono.just(1L)
                .flatMap(l -> {
                    reNameReply.fromAttr = FAttr3.mapToAttr(dirInodes[0], fsid);
                    reNameReply.toAttr = FAttr3.mapToAttr(dirInodes[1], fsid);
                    return Mono.just(reNameReply);
                });
    }

    /**
     * rename完成后更新fromDirFh与toDirFh代表的父级目录的mtime和ctime
     **/
    public Mono<ReNameReply> updateTime(Inode[] dirInodes, ReNameReply reNameReply, String bucket) {
        List<Inode> inodeList = Arrays.stream(dirInodes).filter(Objects::nonNull).collect(Collectors.toList());

        // 如果两个inode的nodeId相等，只发送一次更改时间的请求
        if (inodeList.size() == 2 && !isError(dirInodes[0]) &&
                !isError(dirInodes[1]) && dirInodes[0].getNodeId() == dirInodes[1].getNodeId()) {
            return nodeInstance.updateInodeTime(dirInodes[0].getNodeId(), dirInodes[0].getBucket(), System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), false, true, true)
                    .map(inode -> {
                        dirInodes[0] = inode;
                        dirInodes[1] = inode;
                        return reNameReply;
                    })
                    .doOnError(e -> log.info("update time error 1", e))
                    .onErrorReturn(reNameReply);
        } else {
            long stamp = System.currentTimeMillis() / 1000;
            int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
            return Flux.fromIterable(inodeList)
                    .flatMap(dirInode -> nodeInstance.updateInodeTime(dirInode.getNodeId(), dirInode.getBucket(), stamp, stampNano, false, true, true))
                    .collectList()
                    .map(list -> {
                        if (list.get(0).getNodeId() == dirInodes[0].getNodeId()) {
                            dirInodes[0] = list.get(0);
                            dirInodes[1] = list.get(1);
                        } else {
                            dirInodes[1] = list.get(0);
                            dirInodes[0] = list.get(1);
                        }
                        return reNameReply;
                    })
                    .doOnError(e -> log.info("update time error 2", e))
                    .onErrorReturn(reNameReply);
        }
    }


    public Mono<Inode> renameFile(long oldNodeId, Inode newInode, String oldObjName, String newObjName, String bucket, boolean isOverWrite, AtomicInteger renameNum) {
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
                                            .map(i0 -> {
                                                if (isError(inode1)) {
                                                    log.error("rename: delete {} failed: {}", newObjName, inode1.getLinkN());
                                                    return false;
                                                } else {
                                                    return true;
                                                }
                                            });

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
                                if (isError(inode)) {
                                    log.info("rename {}->{} fail: {}", oldObjName, newObjName, inode.getLinkN());
                                    return Mono.just(inode);
                                }
                                return Mono.just(inode).flatMap(i -> {
                                    if (esSwitch) {
                                        Inode cInode = inode.clone();
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
    public Mono<Inode> scanAndReName(Inode dirInode, String bucket, String newObjName, long count, NFSHandler nfsHandler, AtomicInteger renameNum) {
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
                .onErrorResume(throwable -> Mono.just(ERROR_INODE));
    }

    public Mono<List<Inode>> scanAndReNamePart(Inode dirInode, String bucket, String newObjName, AtomicLong offset, NFSHandler nfsHandler, AtomicInteger renameNum) {
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
//                    offset.accumulateAndGet(inode.getCookie(), (oldVal, newVal) -> oldVal > newVal ? oldVal : newVal);
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

    public Mono<ReNameReply> scanAndReName0(Inode oldInode, String bucket, String newObjName, long offset, ReqInfo reqHeader, ReNameReply reNameReply, Inode[] dirInodes, long fsid) {
        AtomicInteger renameNum = new AtomicInteger();
        return scanAndReName(oldInode, bucket, newObjName, offset, reqHeader.nfsHandler, renameNum)
                .flatMap(inode -> {
                    if (isError(inode)) {
                        log.info("rename {}->{} fail: {}", oldInode.getObjName(), newObjName, inode.getLinkN());
                    }
                    if (renameNum.get() >= 100000) {
                        log.info("renameNum {}", renameNum.get());
                    }
                    return updateTime(dirInodes, reNameReply, bucket)
                            .flatMap(r -> updateFAttr3(dirInodes, r, fsid))
                            .onErrorReturn(reNameReply)
                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                });
    }

}
