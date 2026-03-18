package com.macrosan.filesystem.cache;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.ec.server.WriteCacheServer;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.timeout.FastMonoTimeOut;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.ChunkFile;
import com.macrosan.message.jsonmsg.ChunkFile.UpdateChunkOpt;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.filesystem.FsConstants.NFSACLType.NFS_ACE;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_DQUOT;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.message.jsonmsg.ChunkFile.ERROR_CHUNK;
import static com.macrosan.message.jsonmsg.ChunkFile.NOT_FOUND_CHUNK;
import static com.macrosan.message.jsonmsg.Inode.*;

@Log4j2
public class Node {
    public static final int TOTAL_V_NUM = 4096;
    private static final int MAX_RETRY_NUM = 10;
    private static final int RETRY_TIME_OUT = 1000;

    private static Node instance;

    private static final Logger delObjLogger = LogManager.getLogger("DeleteObjLog.Node");

    public static final StoragePool STORAGE_POOL = StoragePoolFactory.getStoragePool(StorageOperate.META);

    public static final Map<String, Long> INODE_TIME_MAP = new HashMap<>();

    public static Node getInstance() {
        return instance;
    }

    public static void init() {
        instance = new Node(TOTAL_V_NUM);
    }

    String node = ServerConfig.getInstance().getHostUuid();
    int vNum;
    Vnode[] vnodes;
    int nodeNum;

    MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("fs-cache"));

    Node(int vNum) {
        this.vNum = vNum;
        this.nodeNum = Integer.parseInt(node) - 1;
        vnodes = new Vnode[vNum];

        for (int i = 0; i < vNum; i++) {
            vnodes[i] = new Vnode(this, i);
        }
    }

    public Vnode getVnode(int v) {
        return vnodes[v];
    }

    public Vnode getInodeV(long nodeId) {
        int index = Math.abs((int) (nodeId % vNum));
        return vnodes[index];
    }

    public Mono<Inode> getInode(String bucket, long nodeId) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "0")
                .put("bucket", bucket)
                .put("nodeId", String.valueOf(nodeId));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res
                .flatMap(i -> {
                    if (i.getNodeId() > 0 && i.isDeleteMark()) {
                        return Mono.just(NOT_FOUND_INODE);
                    }
                    return Mono.just(i);
                });
    }

    public Mono<Inode> getInodeNotRepair(String bucket, long nodeId) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "0")
                .put("bucket", bucket)
                .put("nodeId", String.valueOf(nodeId))
                .put("notRepair", "1");
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res
                .flatMap(i -> {
                    if (i.getNodeId() > 0 && i.isDeleteMark()) {
                        return Mono.just(NOT_FOUND_INODE);
                    }
                    return Mono.just(i);
                });
    }

    public Mono<Inode> updateInodeData(String bucket, long nodeId, long fileOffset, InodeData inodeData, String versionNum) {
        return updateInodeData(bucket, nodeId, fileOffset, inodeData, versionNum, "", 0, "");
    }

    public Mono<Inode> updateInodeData(String bucket, long nodeId, long fileOffset, InodeData inodeData, String versionNum,
                                       String oldInodeData) {
        return updateInodeData(bucket, nodeId, fileOffset, inodeData, versionNum, oldInodeData, 0, "");
    }


    /**
     * 原子更新updateInodeData，向rsocket server发送1操作，server处理对应方法INODE_CACHE_OPT
     */
    public Mono<Inode> updateInodeData(String bucket, long nodeId, long fileOffset, InodeData inodeData, String versionNum,
                                       String oldInodeData, int updateTime, String quotaDir) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "1")
                .put("bucket", bucket)
                .put("fileOffset", String.valueOf(fileOffset))
                .put("inodeData", Json.encode(inodeData))
                .put("nodeId", String.valueOf(nodeId))
                .put("versionNum", versionNum)
                .put("oldInodeData", oldInodeData)
                .put(QUOTA_KEY, quotaDir)
                .put("updateTime", String.valueOf(updateTime));
        MonoProcessor<Inode> res = MonoProcessor.create();

        exec(nodeId, msg, 0, res);
        return res.doFinally(s -> {
            res.onComplete();
        });
    }

    /***
     * @param ACEs(access control entry) 访问控制列表的字符串
     **/
    public Mono<Inode> updateInodeACL(String bucket, long nodeId, String ACEs) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "2")
                .put("bucket", bucket)
                .put("ACEs", ACEs)
                .put("nodeId", String.valueOf(nodeId));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res;
    }

    public Mono<Inode> deleteInode(long nodeId, String bucket, String objName) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "3")
                .put("bucket", bucket)
                .put("objName", objName)
//                .put("versionNum", VersionUtil.getVersionNumMaybeUpdate(isSyncSwitchOff, nodeId))
                .put("nodeId", String.valueOf(nodeId));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res.doOnNext(inode -> {
            if (!InodeUtils.isError(inode)) {
                delObjLogger.info(" deleteObject success,bucketName:{},objName:{},nodeId:{}", bucket, objName, nodeId);
            }
        });
    }

    public Mono<Inode> createHardLink(String bucket, long nodeId, String linkName) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "4")
                .put("bucket", bucket)
                .put("objName", linkName)
                .put("nodeId", String.valueOf(nodeId));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res;
    }

    public Mono<Inode> renameFile(long nodeId, String oldObjName, String newObjName, String bucket) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "5")
                .put("bucket", bucket)
                .put("oldObj", oldObjName)
                .put("newObj", newObjName)
                .put("nodeId", String.valueOf(nodeId));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res;
    }

    /**
     * @param attr 待修改的参数
     * @param bucketOwner 桶拥有者，仅在修改uid为root时需要，重置s3 拥有者为桶拥有者
     **/
    public Mono<Inode> setAttr(long nodeId, String bucket, ObjAttr attr, String bucketOwner, String... isAllocate) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "6")
                .put("bucket", bucket)
                .put("nodeId", String.valueOf(nodeId))
                .put("attr", Json.encode(attr))
                .put(BUCKET_USER_ID, bucketOwner);
        if (isAllocate.length > 0) {
            msg.put("isAllocate", String.valueOf(isAllocate[0]));
        }
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res.flatMap(i -> {
            if (i.getLinkN() == CAP_QUOTA_EXCCED_INODE.getLinkN()) {
                throw new NFSException(NFS3ERR_DQUOT, "can not set attr ,because of exceed quota.bucket:" + bucket + ",nodeId:" + i.getNodeId());
            }

            if (!InodeUtils.isError(i) && i.isDeleteMark()) {
                return Mono.just(i);
            }

            boolean esSwitch = ES_ON.equals(NFSBucketInfo.getBucketInfo(i.getBucket()).get(ES_SWITCH));
            if (esSwitch && !InodeUtils.isError(i) &&
                    (attr.hasMtime != 0 || (isAllocate.length > 0 && attr.hasSize != 0 && attr.size == 0)
                            || (isAllocate.length == 0 && attr.hasSize != 0))) {
                return EsMetaTask.putEsMeta(i).map(b -> i);
            }
            return Mono.just(i);
        });
    }

    public Mono<Inode> updateInodeTime(long nodeId, String bucket, long stamp, int stampNano, boolean isUpdAtime, boolean isUpdMtime, boolean isUpdCtime) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "7")
                .put("bucket", bucket)
                .put("nodeId", String.valueOf(nodeId))
                .put("stamp", String.valueOf(stamp))
                .put("stampNano", String.valueOf(stampNano))
                .put("isUpdAtime", String.valueOf(isUpdAtime))
                .put("isUpdMtime", String.valueOf(isUpdMtime))
                .put("isUpdCtime", String.valueOf(isUpdCtime));

        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res.flatMap(i -> {
            if (isUpdMtime && !InodeUtils.isError(i) && nodeId != 1) {
                boolean esSwitch = ES_ON.equals(NFSBucketInfo.getBucketInfo(i.getBucket()).get(ES_SWITCH));
                if (esSwitch) {
                    return EsMetaTask.putEsMeta(i).map(v -> i);
                }
            }
            return Mono.just(i);
        });
    }

    //用于修复Inode元数据
    public Mono<Inode> emptyUpdate(long nodeId, String bucket, Inode... newInode) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "8")
                .put("bucket", bucket)
                .put("nodeId", String.valueOf(nodeId));
        if (newInode.length > 0) {
            msg.put("inode", Json.encode(newInode[0]));
        }
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res;
    }

    public Mono<ChunkFile> getChunk(String chunkFileName) {
        Tuple3<Long, String, String> chunk = ChunkFile.getChunkFromFileName(chunkFileName);
        return getChunk(chunk.var1, chunk.var2, chunk.var3);
    }

    public Mono<ChunkFile> getChunk(long nodeId, String bucket, String chunkKey) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "9")
                .put("bucket", bucket)
                .put("chunkKey", chunkKey)
                .put("nodeId", String.valueOf(nodeId));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);

        return res.map(inode -> {
            if (inode.getLinkN() > 0) {
                UpdateChunkOpt opt = inode.getUpdateChunk().get(chunkKey).get(0);
                return ((ChunkFile.CreateChunkOpt) opt).chunkFile;
            } else if (ERROR_INODE.getLinkN() == inode.getLinkN()) {
                return ERROR_CHUNK;
            } else if (NOT_FOUND_INODE.getLinkN() == inode.getLinkN() || inode.isDeleteMark()) {
                //调S3 copy接口后，处理inode元数据已经被删除了，但是chunk元数据还未被删除的情况
                if (inode.getUpdateChunk() != null) {
                    UpdateChunkOpt opt = inode.getUpdateChunk().get(chunkKey).get(0);
                    return ((ChunkFile.CreateChunkOpt) opt).chunkFile;
                }
                return NOT_FOUND_CHUNK;
            }

            log.error("map inode {} to chunk fail. return error chunk", inode);
            return ERROR_CHUNK;
        });
    }

    public Mono<Boolean> updateInodeLinkN(long nodeId, String bucket, int isAdd) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "10")
                .put("bucket", bucket)
                .put("isAdd", isAdd + "")
                .put("nodeId", String.valueOf(nodeId));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res
                .flatMap(inode -> {
                    boolean updateRes = true;
                    if (InodeUtils.isError(inode)) {
                        updateRes = false;
                        log.error("update inode linkN fail..inodeId:{},bucket:{}", nodeId, bucket);
                    }
                    return Mono.just(updateRes);
                });
    }

    /**
     * s3类型的Metadata创建inode
     * metaHash 用来标识元数据是否时同名对象
     **/
    public Mono<Inode> createS3Inode(long dirNodeId, String bucket, String objName, String versionId, String metaHash, List<Inode.ACE> ACEs) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "11")
                .put("bucket", bucket)
                .put("nodeId", String.valueOf(dirNodeId))
                .put("versionId", versionId)
                .put("metaHash", metaHash)
                .put("objName", objName);

        if (null != ACEs) {
            List<Inode.ACE> defACEs = ACLUtils.pickAllDefACL(ACEs, objName);
            if (null != defACEs && !defACEs.isEmpty()) {
                msg.put(NFS_ACE, Json.encode(defACEs));
            }
        }

        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(dirNodeId, msg, 0, res);
        return res;
    }

    /**
     * 创建inode时从nodeId对应的节点获得versionNum
     *
     * @param isCache 原用于判断是否让getVersion走一致性缓存，以解决 deleteInode 在vnode主中触发updateMeta后形成的死锁问题
     *                现更改为用于判断是否为updateMeta中的getVersion，若是updateMeta，则使用getLastVersionNum更新版本号
     *                若是create时使用，则使用getVersionNum更新版本号
     **/
    public Mono<Inode> getVersion(long nodeId, String bucket, boolean isCache, String versionNum) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "12")
                .put("bucket", bucket)
                .put("nodeId", String.valueOf(nodeId))
                .put("isCache", isCache ? "1" : "0");
        if (!isCache) {
            msg.put("oldVersion", versionNum);
        }

        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res;
    }

    public Mono<Inode> getInodeAndUpdateCache(String bucket, long inodeId) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "13")
                .put("bucket", bucket)
                .put("nodeId", String.valueOf(inodeId));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(inodeId, msg, 0, res);
        return res;
    }

    public Mono<Inode> updateLinkNAndInodeDataFileName(String bucket, long inodeId, Inode inode, boolean isAdd) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "14")
                .put("bucket", bucket)
                .put("inode", Json.encode(inode))
                .put("isAdd", String.valueOf(isAdd))
                .put("nodeId", String.valueOf(inodeId));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(inodeId, msg, 0, res);
        return res;
    }

    public Mono<Inode> repairCookieAndInode(long nodeId, String bucket, long createTime) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "15")
                .put("bucket", bucket)
                .put("nodeId", String.valueOf(nodeId))
                .put("createTime", String.valueOf(createTime));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res;
    }

    /***
     * @param objAcl 对象ACL 字符串
     **/
    public Mono<Inode> updateObjACL(String bucket, long nodeId, String objAcl) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "16")
                .put("bucket", bucket)
                .put("objAcl", objAcl)
                .put("nodeId", String.valueOf(nodeId));
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res;
    }

    public Mono<Inode> updateCIFSACL(String bucket, long nodeId, String ACEs, String user, String group, String bucketOwner) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "17")
                .put("bucket", bucket)
                .put("ACEs", ACEs)
                .put("user", user)
                .put("group", group)
                .put("nodeId", String.valueOf(nodeId))
                .put("shareNFS", NFSBucketInfo.isNFSShare(bucket) ? "1" : "0")
                .put(BUCKET_USER_ID, bucketOwner);
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res;
    }

    public Mono<Inode> createInode(String bucket, Inode inode, long nodeId, long stamp, String version, String s3Account, String displayName) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "50")
                .put("bucket", bucket)
                .put("inode", Json.encode(inode))
                .put("stamp", String.valueOf(stamp))
                .put("nodeId", String.valueOf(nodeId))
                .put("version", version)
                .put("s3Account", s3Account)
                .put("displayName", displayName);
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res;
    }

    /**
     *
     * @param inode
     * @param offset
     * @param count
     * @param type 1为下刷inode缓存. 2为write触发,下刷写满的缓存块. 3为下刷所有缓存. 4为文件删除清除缓存，cifs的close删除文件触发
     * @return
     */
    public Mono<Boolean> flushWriteCache(Inode inode, long offset, int count, int type) {
        if (WriteCacheServer.writeCacheDebug) {
            log.info("flushWriteCache ino: {}, offset: {}, count: {}", inode.getNodeId(), offset, count);
        }

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "18")
                .put("bucket", inode.getBucket())
                .put("nodeId", String.valueOf(inode.getNodeId()))
                .put("inode", Json.encode(inode))
                .put("offset", String.valueOf(offset))
                .put("count", String.valueOf(count))
                .put("type", String.valueOf(type));

        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(inode.getNodeId(), msg, 0, res);
        return res
                .flatMap(inode0 -> {
                    boolean flushRes = true;
                    if (WriteCacheServer.writeCacheDebug) {
                        log.info("flushWriteCache end ino: {}, offset: {}, count: {}, {}", inode.getNodeId(), offset, count, InodeUtils.isError(inode0));
                    }
                    if (InodeUtils.isError(inode0)) {
                        flushRes = false;
                    }
                    return Mono.just(flushRes);
                });
    }

    // END_OF_FILE使用
    public Mono<Boolean> flushWriteCache(Inode inode, long offset, int count, long end, int type) {
        if (WriteCacheServer.writeCacheDebug) {
            log.info("flushWriteCache ino: {}, offset: {}, count: {}", inode.getNodeId(), offset, count);
        }

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "18")
                .put("bucket", inode.getBucket())
                .put("nodeId", String.valueOf(inode.getNodeId()))
                .put("inode", Json.encode(inode))
                .put("offset", String.valueOf(offset))
                .put("count", String.valueOf(count))
                .put("end", String.valueOf(end))
                .put("type", String.valueOf(type));

        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(inode.getNodeId(), msg, 0, res);
        return res
                .flatMap(inode0 -> {
                    boolean flushRes = true;
                    if (WriteCacheServer.writeCacheDebug) {
                        log.info("flushWriteCache end ino: {}, offset: {}, count: {}, {}", inode.getNodeId(), offset, count, InodeUtils.isError(inode0));
                    }
                    if (InodeUtils.isError(inode0)) {
                        flushRes = false;
                    }
                    return Mono.just(flushRes);
                });
    }

    public Mono<Inode> markDeleteInode(String bucket, long nodeId, Inode... deleteInode) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("opt", "19")
                .put("bucket", bucket)
                .put("nodeId", String.valueOf(nodeId));
        if (deleteInode.length > 0) {
            msg.put("inode", Json.encode(deleteInode[0]));
        }
        MonoProcessor<Inode> res = MonoProcessor.create();
        exec(nodeId, msg, 0, res);
        return res;
    }

    private void execRes(long nodeId, SocketReqMsg msg, int retry, MonoProcessor<Inode> res, PayloadMetaType type,
                         Inode resInode, Vnode vnode) {
        if (type == SUCCESS) {
            if (resInode.getLinkN() == Inode.RETRY_INODE.getLinkN()) {
                if (retry < MAX_RETRY_NUM) {
                    ErasureServer.DISK_SCHEDULER.schedule(() -> exec(nodeId, msg, retry + 1, res), (long) (retry + 1) * RETRY_TIME_OUT, TimeUnit.MILLISECONDS);
                } else {
                    res.onNext(ERROR_INODE);
                }
            } else {
                log.debug("nodeId:{}, inode info:{}", nodeId, resInode);
                //删除重试之后，返回NOT_FOUND，则视为删除成功
                if ("3".equals(msg.get("opt"))
                        && (resInode != null && resInode.equals(NOT_FOUND_INODE))
                        && retry > 0
                ) {
                    res.onNext(DEL_SUCCESS_INODE);
                } else {
                    res.onNext(resInode);
                }

            }
        } else if (type == ERROR) {
            //inode 心跳丢失，执行心跳失败
            long delay = (long) (retry + 1) * RETRY_TIME_OUT;
            if (resInode != null && resInode.getLinkN() == HEART_DOWN_INODE.getLinkN()) {
                List<Tuple3<String, String, String>> nodeList0 = Node.STORAGE_POOL.mapToNodeInfo(String.valueOf(vnode.storageVnode)).block();
                //优化重试间隔时间，避免每次重试都间隔1s，可能重试10次之后Vnode还未切主成功或ERROR_SOCKET还未被清除
                if (retry < 5) {
                    delay = RETRY_TIME_OUT;
                }
                hearFail(nodeList0, nodeId, 0);
            }
            if (retry < MAX_RETRY_NUM) {
                ErasureServer.DISK_SCHEDULER.schedule(() -> exec(nodeId, msg, retry + 1, res), delay, TimeUnit.MILLISECONDS);
            } else {
                res.onNext(ERROR_INODE);
            }
        }
    }

    public void exec(long nodeId, SocketReqMsg msg, int retry, MonoProcessor<Inode> res) {
        if (res.isDisposed()) {
            return;
        }

        Vnode vnode = getInodeV(nodeId);
        String masterNode = vnode.state.masterNode;

        if (masterNode == null) {
            if (retry < MAX_RETRY_NUM) {
                ErasureServer.DISK_SCHEDULER.schedule(() -> exec(nodeId, msg, retry + 1, res), (long) (retry + 1) * RETRY_TIME_OUT, TimeUnit.MILLISECONDS);
            } else {
                res.onNext(ERROR_INODE);
            }
        } else {
            String ip = NodeCache.getIP(masterNode);

            if (RabbitMqUtils.CURRENT_IP.equals(ip)) {
                LocalPayload<SocketReqMsg> p = new LocalPayload<>(INODE_CACHE_OPT, msg);

                FastMonoTimeOut.fastTimeout(InodeOperator.mapToOperator(p), Duration.ofSeconds(30))
                        .subscribe(payload -> {
                            if (payload instanceof LocalPayload) {
                                LocalPayload<Inode> resP = (LocalPayload<Inode>) payload;
                                execRes(nodeId, msg, retry, res, resP.type, resP.data, vnode);
                            } else {
                                log.error("nodeId:{},msg:{},payload type error",nodeId, msg);
                            }
                        }, e -> {
                            log.error("nodeId:{},msg:{},", nodeId, msg, e);
                            if (retry < MAX_RETRY_NUM) {
                                ErasureServer.DISK_SCHEDULER.schedule(() -> exec(nodeId, msg, retry + 1, res), (long) (retry + 1) * RETRY_TIME_OUT, TimeUnit.MILLISECONDS);
                            } else {
                                res.onNext(ERROR_INODE);
                            }
                        });
            } else {
                List<Tuple3<String, String, String>> nodeList = Collections.singletonList(new Tuple3<>(ip, "", ""));

                ResponseInfo<Inode> responseInfo = ClientTemplate.oneResponse(Collections.singletonList(msg), INODE_CACHE_OPT, Inode.class, nodeList);
                responseInfo.responses.subscribe(t -> {
                    execRes(nodeId, msg, retry, res, t.var2, t.var3, vnode);
                }, e -> {
                    log.error("nodeId:{},msg:{},", nodeId, msg, e);
                    if (retry < MAX_RETRY_NUM) {
                        ErasureServer.DISK_SCHEDULER.schedule(() -> exec(nodeId, msg, retry + 1, res), (long) (retry + 1) * RETRY_TIME_OUT, TimeUnit.MILLISECONDS);
                    } else {
                        res.onNext(ERROR_INODE);
                    }
                });
            }
        }
    }

    private void hearFail(List<Tuple3<String, String, String>> nodeList, long nodeId, int retry) {
        List<SocketReqMsg> msgList = nodeList.stream().map(tuple3 -> {
            return new SocketReqMsg("", 0)
                    .put("nodeId", String.valueOf(nodeId));
        }).collect(Collectors.toList());
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgList, INODE_CACHE_HEART_FAIL, String.class, nodeList);
        responseInfo.responses.subscribe(t -> {
                }
                , e -> {
                    log.error("", e);
                    if (retry < (MAX_RETRY_NUM / 2)) {
                        ErasureServer.DISK_SCHEDULER.schedule(() -> hearFail(nodeList, nodeId, retry + 1));
                    }
                }, () -> {
                    if ((responseInfo.successNum + 1) < nodeList.size() && (retry < (MAX_RETRY_NUM / 2))) {
                        ErasureServer.DISK_SCHEDULER.schedule(() -> hearFail(nodeList, nodeId, retry + 1));
                    }
                });
    }
}
