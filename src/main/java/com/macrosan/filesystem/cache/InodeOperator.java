package com.macrosan.filesystem.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.server.WriteCacheServer;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.async.AsyncUtils;
import com.macrosan.filesystem.cache.model.BTreeModel;
import com.macrosan.filesystem.cache.model.DataModel;
import com.macrosan.filesystem.cifs.notify.NotifyServer;
import com.macrosan.filesystem.cifs.reply.smb2.NotifyReply.NotifyAction;
import com.macrosan.filesystem.cifs.types.smb2.SID;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.quota.FSQuotaScannerTask;
import com.macrosan.filesystem.utils.ChunkFileUtils;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.filesystem.utils.acl.NFSACL;
import com.macrosan.filesystem.utils.timeout.FastMonoTimeOut;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.jsonmsg.ChunkFile.CreateChunkOpt;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple3;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.Utils.DEFAULT_META_HASH;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.FLUSH_WRITE_CACHE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.filesystem.FsConstants.NFSACLType.*;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.INHERIT_ONLY_ACE;
import static com.macrosan.filesystem.FsConstants.SMB2ACEType.*;
import static com.macrosan.filesystem.FsUtils.setFsDelMark;
import static com.macrosan.filesystem.async.AsyncUtils.ASYNC_NODEID;
import static com.macrosan.filesystem.async.AsyncUtils.ASYNC_RECHECK;
import static com.macrosan.filesystem.cifs.api.smb2.SetInfo.IS_ALLOCATE;
import static com.macrosan.filesystem.cifs.call.smb2.NotifyCall.*;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.filesystem.quota.FSQuotaRealService.FS_QUOTA_EXECUTOR;
import static com.macrosan.filesystem.quota.FSQuotaRealService.changeFsQuotaAlarmNormal;
import static com.macrosan.filesystem.quota.FSQuotaScannerTask.QUOTA_CACHE_MODIFY_OPT;
import static com.macrosan.filesystem.utils.FSQuotaUtils.getIdByQuotaType;
import static com.macrosan.filesystem.utils.acl.ACLUtils.existNFSMask;
import static com.macrosan.filesystem.utils.acl.ACLUtils.needChangeGroupMode;
import static com.macrosan.message.jsonmsg.ChunkFile.ERROR_CHUNK;
import static com.macrosan.message.jsonmsg.Inode.*;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.rsocket.LocalPayload.RETRY_INODE_PAYLOAD;

@Log4j2
@RequiredArgsConstructor
public class InodeOperator {
    final long nodeId;
    final int putType;
    //是否为读请求
    final boolean readOpt;
    MonoProcessor<Inode> res = MonoProcessor.create();
    final GetOpt getOpt;
    final UpdateOpt updateOpt;
    final PutOpt putOpt;
    final int opt;
    @Setter
    @Getter
    /**
     * 复制功能使用，用于在实际exec时生成差异记录
     */
            SocketReqMsg msg;

    public interface GetOpt {
        Mono<Inode> get();
    }

    public static class UpdateArgs {
        public boolean needDelete = false;
        public boolean updateRootTime = false;
        public boolean updateAtime = true;
        public boolean updateMtime = true;
        public boolean updateCtime = true;
        public boolean removeCache = false;
        public boolean isRecover = false;
        public boolean hasReName = false;
        public boolean repairCookieAndInode = false;

        public int notifyFilters = 0;
        public Map<String, List<InodeData>> needDeleteMap;
    }

    public interface UpdateOpt {
        void update(UpdateArgs args, Inode inode);
    }

    public interface PutOpt {
        Mono<Inode> put(UpdateArgs args, Inode oldInode, Inode inode);
    }

    //TODO 处理升级 支持升级前inode用 ChunkModel
    static DataModel dataModel = new BTreeModel();

    public static Mono<Payload> mapToOperator(Payload payload) {
        boolean local = payload instanceof LocalPayload;

        SocketReqMsg msg = local ? ((LocalPayload<SocketReqMsg>) payload).data : SocketReqMsg.toSocketReqMsg(payload.data());
        int opt = Integer.parseInt(msg.get("opt"));
        String bucket = msg.get("bucket");
        long nodeId = Long.parseLong(msg.get("nodeId"));
        Vnode vnode = Node.getInstance().getInodeV(nodeId);
        InodeOperator operator = null;

        switch (opt) {
            //get inode
            case 0:
                boolean isNotRepair = "1".equals(msg.get("notRepair"));
                operator = new InodeOperator(nodeId, -1, true, () -> getInode(vnode, bucket, nodeId, isNotRepair, true), null, null, opt);
                break;
            //write. update inode data
            case 1:
                long fileOffset = Long.parseLong(msg.get("fileOffset"));
                String updateVersionNum = msg.get("versionNum");
                InodeData inodeData = Json.decodeValue(msg.get("inodeData"), InodeData.class);
                int updateTime = Integer.parseInt(msg.dataMap.getOrDefault("updateTime", "0"));
                String quotaDir = msg.get(QUOTA_KEY);

                String oldInodeData = msg.get("oldInodeData");

                if (StringUtils.isNotBlank(oldInodeData)) {
                    //处理升级，检查oldInodeData是否为V3.0.1版本完整的inodeData或者v3.0.6版本的"inode"或"fileName"
                    if (!oldInodeData.equals("oldInodeData")) {
                        return Mono.just(DefaultPayload.create(Json.encode(ERROR_INODE), SUCCESS.name()));
                    }
                }

                operator = new InodeOperator(nodeId, 0, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> {
                            dataModel.updateInodeData(fileOffset, inodeData, inode, args, msg.get("oldInodeData"));
                            if (StringUtils.isNotBlank(quotaDir)) {
                                inode.getXAttrMap().put(QUOTA_KEY, quotaDir);
                            }
                            long mtime = System.currentTimeMillis() / 1000;
                            int mtimeNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
                            if (updateTime == 1) {
                                updateInodeTime(mtime, mtimeNano, inode, args, false, true, true);
                            }
                        },
                        (args, old, inode) -> putAndCacheInode(bucket, nodeId, args, old, inode, vnode, updateVersionNum), opt);
                break;
            //update nfs acl
            case 2:
                String ACEs = msg.get("ACEs");
                //根目录修改mode，需更新桶信息，不走inodeCache
                if (nodeId == 1L && StringUtils.isNotBlank(ACEs)) {
                    return updateRootACL(bucket, ACEs, nodeId, msg, opt, local);
                }
                operator = new InodeOperator(nodeId, 0, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> updateInodeACL(ACEs, inode, args),
                        (args, oldInode, inode) -> putAndCacheInode(bucket, nodeId, args, oldInode, inode, vnode, ""), opt);
                break;
            //delete inode
            case 3:
                String objName = msg.get("objName");
//                String versionNum = msg.get("versionNum");
                operator = new InodeOperator(nodeId, 1, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode0) -> updateLinkN(inode0, args, false, ""),
                        (args, oldInode, inode1) -> deleteInode(vnode, inode1, args, objName), opt);
                break;
            //create hardLink
            case 4:
                final String linkName = msg.get("objName");
                operator = new InodeOperator(nodeId, 2, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> updateLinkN(inode, args, true, linkName),
                        (args, oldInode, inode) -> createHardLink(vnode, inode, oldInode), opt);
                break;
            //rename objName
            case 5:
                String oldObjName = msg.get("oldObj");
                String newObjName = msg.get("newObj");
                operator = new InodeOperator(nodeId, 3, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> updateInodeName(args, inode, newObjName, oldObjName),
                        (args, oldInode, inode) -> rename(args, vnode, oldObjName, newObjName, bucket, inode), opt);
                break;
            //set attr
            case 6:
                ObjAttr attr = Json.decodeValue(msg.get("attr"), ObjAttr.class);
                boolean isAllocate = IS_ALLOCATE.equals(msg.get("isAllocate"));
                String bucketOwner = msg.dataMap.getOrDefault(BUCKET_USER_ID, "");

                //根目录修改mode，需更新桶信息，不走inodeCache
                if (nodeId == 1L && attr.hasMode != 0) {
                    UpdateArgs args = new UpdateArgs();
                    return updateRootAttr(bucket, attr, args, isAllocate, nodeId, msg, opt, local);
                }

                operator = new InodeOperator(nodeId, 9, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> setattr(inode, attr, args, isAllocate, bucketOwner),
                        (args, oldInode, inode) -> putInodeAfterSetAttr(args, oldInode, inode, bucket, attr, nodeId, vnode), opt);
                break;
            //update inode stamp
            case 7:
                long updateStamp = Long.parseLong(msg.get("stamp"));
                int updateStampNano = Integer.parseInt(msg.get("stampNano"));
                boolean isUpdAtime = Boolean.parseBoolean(msg.get("isUpdAtime"));
                boolean isUpdMtime = Boolean.parseBoolean(msg.get("isUpdMtime"));
                boolean isUpdCtime = Boolean.parseBoolean(msg.get("isUpdCtime"));
                if (nodeId == 1) {
                    operator = new InodeOperator(nodeId, -2, false, () -> getInode(vnode, bucket, nodeId, false, false),
                            (args, inode) -> InodeUtils.updateRootInodeCache(bucket, updateStamp, updateStampNano, isUpdAtime, isUpdMtime, isUpdCtime),
                            (args, oldInode, inode) -> Mono.just(inode), opt);
                } else {
                    operator = new InodeOperator(nodeId, 0, false, () -> getInode(vnode, bucket, nodeId, false, false),
                            (args, inode) -> updateInodeTime(updateStamp, updateStampNano, inode, args, isUpdAtime, isUpdMtime, isUpdCtime),
                            (args, old, inode) -> putAndCacheInode(bucket, nodeId, args, old, inode, vnode, ""), opt);
                }
                break;
            //empty update
            case 8:
                String inodeStr = msg.get("inode");
                operator = new InodeOperator(nodeId, 4, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> updateInode(inodeStr, inode, args),
                        (args, old, inode) -> putAndCacheInode(bucket, nodeId, args, old, inode, vnode, inode.getVersionNum()), opt);
                break;
            //get chunk
            case 9:
                String chunkKey = msg.get("chunkKey");
                operator = new InodeOperator(nodeId, -3, true, () -> getInode(vnode, bucket, nodeId, false, false)
                        .flatMap(i -> ChunkFileUtils.getChunk(bucket, chunkKey).map(chunkFile -> {
                            if (chunkFile.equals(ERROR_CHUNK)) {
                                log.debug("【GET_CHUNK】inodeId:{},objName:{}", i.getNodeId(), i.getObjName());
                                return RETRY_INODE;
                            }
                            Inode resInode = i.clone();  // 有可能克隆的时候 chunk 正在被修改，那么此时会克隆得到一个modifyChunkOpt
                            Inode.updateChunk(resInode, chunkKey, new CreateChunkOpt(chunkFile));
                            return resInode;
                        })),
                        null, null, opt);
                break;
            //update linkN
            case 10:
                boolean isAdd = "1".equals(msg.get("isAdd"));
                operator = new InodeOperator(nodeId, 0, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> {
                            updateLinkN(inode, args, isAdd, inode.getObjName());
                        },
                        (args, oldInode, inode) -> putAndCacheInode(inode.getBucket(), inode.getNodeId(), args, oldInode, inode, vnode, ""), opt);
                break;
            //s3 create inode
            case 11:
                String s3ObjectName = msg.get("objName");
                String s3VersionId = msg.get("versionId");
                String metaHash = msg.get("metaHash");
                String dirDefACEs = msg.dataMap.get(NFS_ACE);

                if (AsyncUtils.checkOptForAsync(msg) && !"1".equals(ASYNC_RECHECK)) {
                    // 站点A收到文件客户端发来的请求，记下nodeId存入差异记录，后续同步时将使用该id作为更新依据。
                    msg.put(ASYNC_NODEID, String.valueOf(VersionUtil.newInode()));
                }
                //用readOpt防止batch
                operator = new InodeOperator(nodeId, -1, true,
                        () -> createS3Inode(vnode, bucket, s3ObjectName, s3VersionId, metaHash, dirDefACEs, msg),
                        null, null, opt);
                break;
            //创建inode时，从nodeId对应的节点获得versionNum
            case 12:
                String isCache = msg.dataMap.getOrDefault("isCache", "1");
                if ("1".equals(isCache)) {
                    operator = new InodeOperator(nodeId, -1, true,
                            () -> Mono.just(new Inode().setVersionNum(VersionUtil.getVersionNum())),
                            null, null, opt);
                } else {
                    String versionNum = msg.dataMap.getOrDefault("oldVersion", "");
                    operator = new InodeOperator(nodeId, -1, true,
                            () -> Mono.just(new Inode().setVersionNum(VersionUtil.getLastVersionNum(versionNum))),
                            null, null, opt);
                }
                break;
            //S3接口删除时，需查询inode元数据，对于硬链接数为1的inode缓存应该删除，避免之后在客户端上传同名对象时，不走create()方法
            case 13:
                operator = new InodeOperator(nodeId, 6, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> {
                        },
                        (args, oldInode, inode) -> {
                            vnode.inodeCache.cache.remove(inode.getNodeId());
                            return Mono.just(inode);
                        }, opt);
                break;
            //控制台重命名或者移动文件，对inode中inodeData中的fileName进行更新，并删除旧的chunk元数据
            case 14:
                Inode inodeChunk = Json.decodeValue(msg.get("inode"), Inode.class);
                boolean isAdd0 = Boolean.parseBoolean(msg.get("isAdd"));
                operator = new InodeOperator(nodeId, 7, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> updateLinkNAndInodeDataFileName(inode, args, isAdd0, inodeChunk),
                        (args, oldInode, inode) -> deleteChunkAndPutInode(bucket, nodeId, args, oldInode, inode, vnode, ""), opt);
                break;
            //处理旧版本升级新版本后inode中createTime属性和cookieInode的修复；versionNum不变
            case 15:
                long createTime = Long.parseLong(msg.get("createTime"));
                operator = new InodeOperator(nodeId, 8, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> updateCreateTime(inode, createTime, args),
                        (args, old, inode) -> putAndCacheInode(bucket, nodeId, args, old, inode, vnode, inode.getVersionNum()), opt);
                break;
            case 16:
                String objAcl = msg.get("objAcl");
                operator = new InodeOperator(nodeId, 0, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> updateObjACL(objAcl, inode, args),
                        (args, oldInode, inode) -> putAndCacheInode(bucket, nodeId, args, oldInode, inode, vnode, ""), opt);
                break;
            case 17:
                //将要设置的属主
                String ownerSID = msg.get("user");
                //将要设置的属组
                String groupSID = msg.get("group");
                //将要设置的dACL列表
                String ACE = msg.get("ACEs");
                String bucketUser = msg.dataMap.getOrDefault(BUCKET_USER_ID, "");

                if (nodeId == 1L && StringUtils.isNotBlank(ACE)) {
                    return updateRootACL(bucket, ACE, nodeId, msg, opt, local);
                }

                operator = new InodeOperator(nodeId, 11, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode) -> updateCIFSACL(ACE, ownerSID, groupSID, inode, args, bucketUser),
                        (args, oldInode, inode) -> putAndCacheInode(bucket, nodeId, args, oldInode, inode, vnode, ""), opt);
                break;
            // 下刷写缓存
            case 18:
                Inode flushInode = Json.decodeValue(msg.get("inode"), Inode.class);
                // 独立执行, 不影响其他操作
                return FastMonoTimeOut.fastTimeout(flushWriteCache(bucket, nodeId, flushInode, msg), Duration.ofSeconds(300))
                        .map(i -> local ? new LocalPayload<>(SUCCESS, i) : DefaultPayload.create(Json.encode(i), SUCCESS.name()))
                        .onErrorResume(e -> {
                            if (e instanceof TimeoutException || "closed connection".equals(e.getMessage())
                                    || (e.getMessage() != null && e.getMessage().startsWith("No keep-alive acks for"))) {
                                log.error("vnode exec timeout, nodeId: {}, opt: {}, bucket: {}, msg: {}, {}", nodeId, opt, bucket, msg, e.getMessage());
                            } else {
                                log.error("vnode exec error, nodeId: {}, opt: {}, bucket: {}, msg: {}", nodeId, opt, bucket, msg, e);
                            }
                            if (local) {
                                return Mono.just(RETRY_INODE_PAYLOAD);
                            } else {
                                return Mono.just(DefaultPayload.create(Json.encode(RETRY_INODE), SUCCESS.name()));
                            }
                        });
            case 19:
                operator = new InodeOperator(nodeId, 10, false, () -> getInode(vnode, bucket, nodeId, false, false),
                        (args, inode0) -> updateLinkN(inode0, args, false, ""),
                        (args, old, inode1) -> markDeleteInode(vnode, inode1, args), opt);
                break;
            case 50:
                Inode createInode = Json.decodeValue(msg.get("inode"), Inode.class);
                long stamp = Long.parseLong(msg.get("stamp"));
                String createVersion = msg.get("version");
                String s3Account = msg.get("s3Account");
                String displayName = msg.get("displayName");

                Mono<Inode> res = InodeUtils.create0(bucket, createInode, nodeId, stamp, createVersion, s3Account, displayName);
                Mono<Inode> mono;
                if (AsyncUtils.checkOptForAsync(msg)) {
                    // 异步复制写预提交记录操作放在这里是为了保证写预提交记录的顺序和waitLit中任务处理的顺序一致
                    // todo del
                    log.debug("opt1 : {}, {}, {}", nodeId, opt, msg);
                    List<SocketReqMsg> msgs = Stream.of(msg).collect(Collectors.toList());
                    mono = AsyncUtils.asyncBatch(msg.get("bucket"), nodeId, msgs, res);
                } else {
                    mono = res;
                }
                return FastMonoTimeOut.fastTimeout(mono, Duration.ofSeconds(300))
                        .map(i -> local ? new LocalPayload<>(SUCCESS, i) : DefaultPayload.create(Json.encode(i), SUCCESS.name()))
                        .onErrorResume(e -> {
                            if (e instanceof TimeoutException || "closed connection".equals(e.getMessage())
                                    || (e.getMessage() != null && e.getMessage().startsWith("No keep-alive acks for"))) {
                                log.error("vnode exec timeout, nodeId: {}, opt: {}, bucket: {}, msg: {}, {}", nodeId, opt, bucket, msg, e.getMessage());
                            } else {
                                log.error("vnode exec error, nodeId: {}, opt: {}, bucket: {}, msg: {}", nodeId, opt, bucket, msg, e);
                            }
                            if (local) {
                                return Mono.just(RETRY_INODE_PAYLOAD);
                            } else {
                                return Mono.just(DefaultPayload.create(Json.encode(RETRY_INODE), SUCCESS.name()));
                            }
                        });
            default:
        }

        if (operator == null) {
            log.info("no such inode opt {}:{}", opt, msg);
            return Mono.just(local ? RETRY_INODE_PAYLOAD : DefaultPayload.create(Json.encode(RETRY_INODE), SUCCESS.name()));
        } else {
            Mono<Inode> mono;
            if (AsyncUtils.checkOptForAsync(msg)) {
                // 文件复制加入objName作为差异记录并发处理的依据。同名对象/文件需要串行处理。
                // 根目录的objName为空，单独处理
                if (StringUtils.isBlank(msg.get("objName")) && nodeId != 1L) {
                    InodeOperator finalOperator = operator;
                    mono = Node.getInstance().getInode(bucket, nodeId)
                            .doOnNext(inode -> {
                                msg.put("objName", inode.getObjName());
                                finalOperator.setMsg(msg);
                            })
                            .flatMap(inode -> vnode.exec(finalOperator));
                } else {
                    operator.setMsg(msg);
                    mono = vnode.exec(operator);
                }
            } else {
                operator.setMsg(null);
                mono = vnode.exec(operator);
            }
            return FastMonoTimeOut.fastTimeout(mono, Duration.ofSeconds(300))
                    .map(i -> local ? new LocalPayload<>(SUCCESS, i) : DefaultPayload.create(Json.encode(i), SUCCESS.name()))
                    .onErrorResume(e -> {
                        if (e instanceof TimeoutException || "closed connection".equals(e.getMessage())
                                || (e.getMessage() != null && e.getMessage().startsWith("No keep-alive acks for"))) {
                            log.error("vnode exec timeout, nodeId: {}, opt: {}, bucket: {}, msg: {}, {}", nodeId, opt, bucket, msg, e.getMessage());
                        } else {
                            log.error("vnode exec error, nodeId: {}, opt: {}, bucket: {}, msg: {}", nodeId, opt, bucket, msg, e);
                        }
                        if (local) {
                            return Mono.just(RETRY_INODE_PAYLOAD);
                        } else {
                            return Mono.just(DefaultPayload.create(Json.encode(RETRY_INODE), SUCCESS.name()));
                        }
                    });
        }
    }

    private static void updateObjACL(String objAcl, Inode inode, UpdateArgs args) {
        if (StringUtils.isNotBlank(objAcl)) {
            inode.setObjAcl(objAcl);
        }
    }

    private static Mono<Payload> updateRootACL(String bucket, String ACEs, long nodeId, SocketReqMsg msg, int opt, boolean local) {
        return Mono.just(1L)
                .publishOn(ACLUtils.FS_ACL_SCHEDULER)
                .flatMap(l -> InodeUtils.getAndPutRootInodeReactive(bucket, false))
                .flatMap(inode -> {
                    Inode clone = inode.clone();
                    boolean changeMode = false;
                    if (StringUtils.isNotBlank(ACEs)) {
                        List<Inode.ACE> curAcl = Json.decodeValue(ACEs, new TypeReference<List<ACE>>() {
                        });

                        if (opt == 17) {
                            //获取inode对应的属主和属组
                            String inoOwnerSID = FSIdentity.getUserSIDByUid(inode.getUid());
                            String inoGroupSID = FSIdentity.getGroupSIDByGid(inode.getGid());

                            //遍历当前所有的cifs ace，仅allowed ace中的ugo权限要对应修改mode，其余不做处理
                            for (ACE ace : curAcl) {
                                byte cType = ace.getCType();
                                short flag = ace.getFlag();
                                String sid = ace.getSid();
                                long access = ace.getMask();
                                switch (cType) {
                                    case ACCESS_ALLOWED_ACE_TYPE:
                                        if (inode.getObjName().endsWith("/") && (flag & INHERIT_ONLY_ACE) != 0) {
                                            //继承权限不需要处理
                                            break;
                                        } else {
                                            if (inoOwnerSID.equals(sid)) {
                                                //对应user权限
                                                int mode = inode.getMode();
                                                mode = CIFSACL.refreshModeByCifsAccess(access, mode, NFSACL_USER_OBJ, true);
                                                clone.setMode(mode);
                                            } else if (inoGroupSID.equals(sid)) {
                                                //对应group权限
                                                int mode = inode.getMode();
                                                mode = CIFSACL.refreshModeByCifsAccess(access, mode, NFSACL_GROUP_OBJ, true);
                                                clone.setMode(mode);
                                            } else if (SID.EVERYONE.getDisplayName().equals(sid)) {
                                                //对应other权限
                                                int mode = inode.getMode();
                                                mode = CIFSACL.refreshModeByCifsAccess(access, mode, NFSACL_OTHER, true);
                                                clone.setMode(mode);
                                            }
                                            if (CIFSACL.cifsACL) {
                                                log.info("【set cifs acl】 sid: {}, obj: {}, mode: {}", sid, inode.getObjName(), inode.getMode());
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
                            }

                            //cifs ace覆盖原本的nfs ace
                            clone.setACEs(curAcl);
                        } else if (opt == 2) {
                            int mode = clone.getMode();
                            ListIterator<Inode.ACE> iterator = curAcl.listIterator();
                            int newMode = mode;
                            while (iterator.hasNext()) {
                                Inode.ACE ace = iterator.next();
                                int type = ace.getNType();
                                int permission = ace.getRight();
                                if (Arrays.stream(UGO_ACE_ARR).anyMatch(e -> e == type)) {
                                    if (permission != NFSACL.parseModeToInt(mode, type)) {
                                        newMode = NFSACL.refreshMode(newMode, type);
                                        newMode |= NFSACL.parsePermissionToMode(permission, type);
                                    }
                                    iterator.remove();
                                }
                            }

                            if (newMode > 0) {
                                changeMode = true;
                                clone.setMode(newMode);
                                Inode.updateMask(clone, 1, newMode);
                            }

                            clone.setACEs(curAcl);
                        }
                    }
                    return ACLUtils.updateRootACL(bucket, clone, changeMode)
                            .flatMap(b -> {
                                return Mono.just(InodeUtils.updateRootInodeCache(bucket, clone));
                            });
                })
                .timeout(Duration.ofSeconds(300))
                .map(i -> local ? new LocalPayload<>(SUCCESS, i) : DefaultPayload.create(Json.encode(i), SUCCESS.name()))
                .onErrorResume(e -> {
                    if (e instanceof TimeoutException || "closed connection".equals(e.getMessage())
                            || (e.getMessage() != null && e.getMessage().startsWith("No keep-alive acks for"))) {
                        log.error("vnode exec timeout, nodeId: {}, opt: {}, bucket: {}, msg: {}, {}", nodeId, opt, bucket, msg, e.getMessage());
                    } else {
                        log.error("vnode exec error, nodeId: {}, opt: {}, bucket: {}, msg: {}", nodeId, opt, bucket, msg, e);
                    }
                    if (local) {
                        return Mono.just(RETRY_INODE_PAYLOAD);
                    } else {
                        return Mono.just(DefaultPayload.create(Json.encode(RETRY_INODE), SUCCESS.name()));
                    }
                });
    }

    private static Mono<Payload> updateRootAttr(String bucket, ObjAttr attr, UpdateArgs args, boolean isAllocate, long nodeId, SocketReqMsg msg, int opt, boolean local) {
        return Mono.just(1L)
                .publishOn(ACLUtils.FS_ACL_SCHEDULER)
                .flatMap(l -> InodeUtils.getAndPutRootInodeReactive(bucket, false))
                .flatMap(inode -> {
                    setattr(inode, attr, args, isAllocate, null);
                    return ACLUtils.updateRootMode(bucket, inode)
                            .flatMap(b -> {
                                return Mono.just(InodeUtils.updateRootInodeCache(bucket, inode));
                            });
                })
                .timeout(Duration.ofSeconds(300))
                .map(i -> local ? new LocalPayload<>(SUCCESS, i) : DefaultPayload.create(Json.encode(i), SUCCESS.name()))
                .doOnError(e -> {
                    if (e instanceof TimeoutException || "closed connection".equals(e.getMessage())
                            || (e.getMessage() != null && e.getMessage().startsWith("No keep-alive acks for"))) {
                        log.error("vnode exec timeout, nodeId: {}, opt: {}, bucket: {}, msg: {}, {}", nodeId, opt, bucket, msg, e.getMessage());
                    } else {
                        log.error("vnode exec error, nodeId: {}, opt: {}, bucket: {}, msg: {}", nodeId, opt, bucket, msg, e);
                    }
                })
                .onErrorReturn(local ? RETRY_INODE_PAYLOAD : DefaultPayload.create(Json.encode(RETRY_INODE), SUCCESS.name()));
    }

    private static void updateCreateTime(Inode inode, long createTime, UpdateArgs args) {
        if (inode.getCreateTime() == 0) {
            inode.setCreateTime(createTime);
        }
        args.notifyFilters |= FILE_NOTIFY_CHANGE_CREATION;
        args.repairCookieAndInode = true;
    }

    private static void updateInode(String inodeStr, Inode inode, UpdateArgs args) {
        if (StringUtils.isNotEmpty(inodeStr)) {
            Inode newInode = Json.decodeValue(inodeStr, Inode.class);
            if (StringUtils.isNotBlank(newInode.getVersionNum())
                    && StringUtils.isNotBlank(inode.getVersionNum())
                    && newInode.getVersionNum().compareTo(inode.getVersionNum()) > 0) {
                inode.setLinkN(newInode.getLinkN());
                inode.setVersionNum(newInode.getVersionNum());
            }
            inode.setXAttrMap(newInode.getXAttrMap());
        }
        args.isRecover = true;
    }

    /**
     * 只对相近时间内 对同一对象进行createInode的操作加锁
     * 间隔较长的，可以认为get到的Metadata中，inode已经>0了，不会进行重复的创建操作
     */
    private static Mono<Inode> createS3Inode(Vnode vnode, String bucket, String objName, String versionId, String metaHash, String dirDefACEs, SocketReqMsg msg) {
        String key = Utils.getVersionMetaDataKey(metaHash, bucket, objName, versionId);
        AtomicReference<Inode> inode = new AtomicReference<>(new Inode());
        vnode.inodeCache.s3InodeCache.compute(key, (k, v) -> {
            if (v == null) {
                v = new Inode();
                v.setLinkN(0);
                v.setVersionNum(VersionUtil.getVersionNum());
                inode.set(v.clone());
                return v;
            } else {
                if (v.getLinkN() > 0) {
                    inode.set(v.clone());
                    return v;
                } else {
                    v.setLinkN(-1);
                    inode.set(v.clone());
                    return v;
                }
            }
        });

        // 对于文件复制来说可能两边的索引inode不一致，需要再确认是否要创建。
        boolean isAsync = "1".equals(msg.get("async"));
        //加锁成功
        if (inode.get().getLinkN() == 0 || isAsync) {
            return InodeUtils.createS3Inode(bucket, objName, versionId, metaHash, dirDefACEs, msg)
                    .doOnNext(i -> {
                        if (InodeUtils.isError(i)) {
                            //释放锁
                            vnode.inodeCache.s3InodeCache.compute(key, (k, v) -> {
                                if (v == null) {
                                    return null;
                                } else {
                                    if (v.getVersionNum().equals(inode.get().getVersionNum())) {
                                        return null;
                                    } else {
                                        return v;
                                    }
                                }
                            });
                        } else {
                            //设置当前i到缓存中
                            vnode.inodeCache.s3InodeCache.compute(key, (k, v) -> {
                                if (v == null) {
                                    return null;
                                } else {
                                    if (v.getVersionNum().equals(inode.get().getVersionNum())) {
                                        return i;
                                    } else {
                                        return v;
                                    }
                                }
                            }, DEFAULT_META_HASH.equals(metaHash) ? 1_000_000_000L : 0L);
                        }
                    });
        } else if (inode.get().getLinkN() == -1) {
            return Mono.just(RETRY_INODE);
        } else {
            //已经创建好了
            return Mono.just(inode.get().clone());
        }
    }

    private static void updateInodeTime(long stamp, int stampNano, Inode inode, UpdateArgs args, boolean isUpdAtime, boolean isUpdMtime, boolean isUpdCtime) {
        if (isUpdAtime) {
            args.notifyFilters |= FILE_NOTIFY_CHANGE_LAST_ACCESS;
            inode.setAtime(stamp);
            inode.setAtimensec(stampNano);
        } else {
            args.updateAtime = false;
        }

        if (isUpdMtime) {
            args.notifyFilters |= FILE_NOTIFY_CHANGE_LAST_WRITE;
            inode.setMtime(stamp);
            inode.setMtimensec(stampNano);
        } else {
            args.updateMtime = false;
        }

        if (isUpdCtime) {
            args.notifyFilters |= FILE_NOTIFY_CHANGE_LAST_WRITE;
            inode.setCtime(stamp);
            inode.setCtimensec(stampNano);
        } else {
            args.updateCtime = false;
        }
    }

    private static void updateInodeACL(String ACEs, Inode inode, UpdateArgs args) {
        if (StringUtils.isNotBlank(ACEs)) {
            List<Inode.ACE> curAcl = Json.decodeValue(ACEs, new TypeReference<List<ACE>>() {
            });

            if (curAcl.isEmpty()) {
                return;
            }

            //并非所有ACE权限都要保存，其中user_obj、group_obj和other的ACE应当直接修改
            int mode = inode.getMode();
            ListIterator<Inode.ACE> iterator = curAcl.listIterator();
            int newMode = mode;
            while (iterator.hasNext()) {
                Inode.ACE ace = iterator.next();
                int type = ace.getNType();
                int permission = ace.getRight();

                //检查ACE中与UGO权限对应的部分是否有所改变
                if (Arrays.stream(UGO_ACE_ARR).anyMatch(e -> e == type)) {
                    if (permission != NFSACL.parseModeToInt(mode, type)) {
                        newMode = NFSACL.refreshMode(newMode, type);
                        newMode |= NFSACL.parsePermissionToMode(permission, type);
                    }
                    iterator.remove();
                }
            }

            if (newMode > 0) {
                inode.setMode(newMode);
                // 如果存在ACEs，且将要更改的文件ugo group权限与当前记录的mask值不一致时，mask需要同步修改为group权限
                Inode.updateMask(inode, 1, newMode);
            }

            inode.setACEs(curAcl);
            long stamp = System.currentTimeMillis();
            int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
            inode.setCtime(stamp / 1000)
                    .setCtimensec(stampNano);
            args.updateRootTime = true;
            args.notifyFilters |= FILE_NOTIFY_CHANGE_ATTRIBUTES;
        }
    }

    private static void updateCIFSACL(String ACEs, String user, String group, Inode inode, UpdateArgs args, String bucketOwner) {
        boolean updateTime = false;
        //设置属主；保留原本的ace，仅更改uid
        if (StringUtils.isNotEmpty(user)) {
            int uid = FSIdentity.getUidBySID(user);
            int oriUid = inode.getUid();
            if (uid >= 0 && uid != oriUid) {
                //属主发生更改，s3拥有者也需要同步更改
                ACLUtils.updateObjAcl(inode, oriUid, uid, bucketOwner);
                inode.setUid(uid);
                updateTime = true;
            }
        }

        //设置属组；保留原本的ace，仅更改gid
        if (StringUtils.isNotEmpty(group)) {
            int gid = FSIdentity.getGidBySID(group);
            if (gid >= 0 && gid != inode.getGid()) {
                inode.setGid(gid);
                updateTime = true;
            }
        }

        //设置访问控制列表
        if (StringUtils.isNotBlank(ACEs)) {
            List<Inode.ACE> curAcl = Json.decodeValue(ACEs, new TypeReference<List<ACE>>() {
            });

            //获取inode对应的属主和属组
            String inoOwnerSID = FSIdentity.getUserSIDByUid(inode.getUid());
            String inoGroupSID = FSIdentity.getGroupSIDByGid(inode.getGid());

            boolean isDir = inode.getNodeId() == 1 || (null != inode.getObjName() && inode.getObjName().endsWith("/"));

            //分别存储要修正的user、group和other权限
            long[] modeAccess = {-1, -1, -1};
            long[] denyAccess = {-1, -1, -1};

            //遍历当前所有的cifs ace，仅allowed ace中的ugo权限要对应修改mode，其余不做处理
            for (ACE ace : curAcl) {
                byte cType = ace.getCType();
                short flag = ace.getFlag();
                String sid = ace.getSid();
                long access = ace.getMask();
                switch (cType) {
                    case ACCESS_ALLOWED_ACE_TYPE:
                        if (inode.getObjName().endsWith("/") && (flag & INHERIT_ONLY_ACE) != 0) {
                            //继承权限不需要处理
                            break;
                        } else {
                            if (inoOwnerSID.equals(sid)) {
                                //对应user权限
                                if (modeAccess[0] == -1) {
                                    modeAccess[0] = access;
                                } else {
                                    modeAccess[0] |= access;
                                }
                            } else if (inoGroupSID.equals(sid)) {
                                //对应group权限
                                if (modeAccess[1] == -1) {
                                    modeAccess[1] = access;
                                } else {
                                    modeAccess[1] |= access;
                                }
                            } else if (SID.EVERYONE.getDisplayName().equals(sid)) {
                                //对应other权限
                                if (modeAccess[2] == -1) {
                                    modeAccess[2] = access;
                                } else {
                                    modeAccess[2] |= access;
                                }
                            }
                            if (CIFSACL.cifsACL) {
                                log.info("【set cifs acl】 sid: {}, obj: {}, mode: {}, arr: {}", sid, inode.getObjName(), inode.getMode(), Arrays.toString(modeAccess));
                            }
                        }
                        break;
                    case ACCESS_DENIED_ACE_TYPE:
                        if (inode.getObjName().endsWith("/") && (flag & INHERIT_ONLY_ACE) != 0) {
                            //继承权限不需要处理
                            break;
                        } else {
                            if (inoOwnerSID.equals(sid)) {
                                //对应user权限
                                if (denyAccess[0] == -1) {
                                    denyAccess[0] = access;
                                } else {
                                    denyAccess[0] |= access;
                                }
                            } else if (inoGroupSID.equals(sid)) {
                                //对应group权限
                                if (denyAccess[1] == -1) {
                                    denyAccess[1] = access;
                                } else {
                                    denyAccess[1] |= access;
                                }
                            } else if (SID.EVERYONE.getDisplayName().equals(sid)) {
                                //对应other权限
                                if (denyAccess[2] == -1) {
                                    denyAccess[2] = access;
                                } else {
                                    denyAccess[2] |= access;
                                }
                            }
                            if (CIFSACL.cifsACL) {
                                log.info("【set cifs acl】 sid: {}, obj: {}, mode: {}, deny: {}", sid, inode.getObjName(), inode.getMode(), Arrays.toString(denyAccess));
                            }
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
            }

            //根据ace修正mode权限
            //当前代码默认每次修改ace，只会修改其中某个ace，而不全重新设置；其实每次修改ace，都是根据之前获取的ace重新设置，因此每次发过来的
            //ace都是包含所有的ace
            for (int i = 0; i < UGO_ACE_ARR.length; i++) {
                long curAllow = modeAccess[i];
                long curDenied = denyAccess[i];
                int type = UGO_ACE_ARR[i];
                if (curAllow > -1 || curDenied > -1) {
                    int mode = inode.getMode();
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
                } else if (curAllow == -1 && curDenied == -1) {
                    //说明该条ace被清除了，则对应该位置的mode权限应当置为0
                    int mode = inode.getMode();
                    mode = CIFSACL.refreshModeByCifsAccess(0L, mode, type, isDir);
                    inode.setMode(mode);
                }
            }

            //cifs ace覆盖原本的nfs ace
            inode.setACEs(curAcl);
            updateTime = true;
        }

        if (updateTime) {
            long stamp = System.currentTimeMillis();
            int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
            inode.setCtime(stamp / 1000)
                    .setCtimensec(stampNano);
            args.updateRootTime = true;
        }
    }

    static Mono<Inode> getInode(Vnode vnode, String bucket, long nodeId, boolean isNotRepair, boolean needMergeMeta) {
        if (nodeId == 1L) {
            return InodeUtils.getAndPutRootInodeReactive(bucket, false)
                    .map(Inode::clone);
        }
        Inode inode = vnode.inodeCache.cache.get(nodeId);
        boolean isACLStart = ACLUtils.NFS_ACL_START || ACLUtils.CIFS_ACL_START;
        if (inode == null ||
                (needMergeMeta && isACLStart && inode.getNodeId() > 1 && !inode.isDeleteMark() && inode.getLinkN() > 0
                        && inode.getObjAcl() == null)) {
            //1、一致性缓存中不存在对应inode，则从磁盘重新获取；
            //2、needMerge为true的情况，且开启了文件权限，若缓存中的inode还是老版本缺失objAcl、owner字段的inode，则重新从磁盘获取
            return InodeUtils.getInode(bucket, nodeId, isNotRepair)
                    .flatMap(resInode -> {
                        if (!InodeUtils.isError(resInode)) {
                            CifsUtils.setDefaultCifsMode(resInode);
                        }
                        return Mono.just(resInode);
                    })
                    .doOnNext(i -> {
                        if (i.getNodeId() > 0 && i.getNodeId() != 1 && !i.isDeleteMark()) {
                            vnode.inodeCache.cache.put(nodeId, i.clone());
                        }
                    });
        } else {
            return Mono.just(inode.clone());
        }
    }

    private static Mono<Inode> putInode(String bucket, UpdateArgs args, Inode old, Inode inode) {
        return InodeUtils.updateInode(bucket, inode, args.isRecover, args.repairCookieAndInode)
                .doOnNext(updateRes -> {
                    List<InodeData> needDelete = new LinkedList<>();
                    if (updateRes && args.needDelete && !old.getInodeData().isEmpty()) {
                        Set<String> newSet = inode.getInodeData().stream().map(i -> i.fileName).collect(Collectors.toSet());
                        for (InodeData inodeData : old.getInodeData()) {
                            String chunkFileName = ChunkFile.getChunkFileName(inode.getNodeId(), inodeData.fileName);
                            if (!newSet.contains(inodeData.fileName) && !newSet.contains(chunkFileName)) {
                                needDelete.add(inodeData);
                            }
                        }

                        log.debug("need delete {}", needDelete);
                        FsUtils.deleteFile0(needDelete, bucket).subscribe();
                    }
                })
                .map(b -> {
                    if (b) {
                        return inode;
                    } else {
                        return RETRY_INODE;
                    }
                });
    }

    private static void updateLinkN(Inode inode, UpdateArgs args, boolean isAdd, String linkName) {
        int linkN = inode.getLinkN();
        // 新增链接的操作
        if (isAdd) {
            inode.setLinkN(linkN + 1);
            inode.setObjName(linkName);
        } else {
            // 删除链接的操作
            if (linkN > 1) {
                inode.setLinkN(linkN - 1);
                args.needDelete = false;
            } else {
                args.needDelete = true;
            }
        }

        inode.setCtime(System.currentTimeMillis() / 1000)
                .setCtimensec((int) (System.nanoTime() % ONE_SECOND_NANO));

    }

    private static Mono<Inode> deleteInode(Vnode vnode, Inode inode, UpdateArgs args, String objName) {
        // 检查inode的objName是否为本次删除请求的objName，若不是则重置objName
        String lastObjName = inode.getObjName();
        AtomicBoolean hasRename = new AtomicBoolean(false);
        AtomicBoolean isFsDelMark = new AtomicBoolean(false);
        if (!inode.getObjName().equals(objName)) {
            if (inode.getLinkN() == 1) {
                hasRename.set(true);
            }
            inode.setObjName(objName);
        }

        String bucket = inode.getBucket();
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(inode.getBucket());
        String bucketVnode = pool.getBucketVnodeId(bucket);
        if (inode.isDeleteMark()) {
            return Mono.just(NOT_FOUND_INODE);
        }
        String[] newVersionNum = new String[1];
        return pool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> setFsDelMark(inode, nodeList, newVersionNum, args.needDelete, isFsDelMark))
                .doOnNext(delRes -> {
                    if ((inode.getMode() & S_IFMT) == S_IFDIR) {
                        //删除配额信息
                        FSQuotaUtils.delQuotas(bucket, objName);
                    }
                })
                .flatMap(b -> {
                    if (!b) {
                        log.error("delete object or link internal error!nodeId:{},objName:{}", inode.getNodeId(), objName);
                        return Mono.just(RETRY_INODE);
                    } else {
                        if (hasRename.get() && isFsDelMark.get()) {
                            inode.setObjName(lastObjName);
                            //删除前，原文件已经被重命名
                            return Mono.just(NOT_FOUND_INODE);
                        }
                        // 不存在硬链接，直接删除数据块
                        if (args.needDelete) {
                            return FsUtils.deleteChunkFile(inode.getInodeData(), inode.getBucket())
                                    .flatMap(delRes -> {
                                        if (delRes) {
                                            return deleteChunkMeta(inode);
                                        }
                                        return Mono.just(delRes);
                                    })
                                    .map(b0 -> inode)
                                    .doOnNext(i -> {
                                        if (i.getNodeId() > 0) {
                                            vnode.inodeCache.cache.remove(i.getNodeId());
                                        }
                                    });
                        } else {
                            // 存在硬链接，更新inode
                            inode.setObjName(lastObjName);
                            return BucketSyncSwitchCache.isSyncSwitchOffMono(inode.getBucket())
                                    .flatMap(isSync -> {
                                        if (StringUtils.isNotBlank(newVersionNum[0])) {
                                            return Mono.just(newVersionNum[0]);
                                        }
                                        return Mono.just(VersionUtil.getVersionNumMaybeUpdate(isSync, inode.getNodeId()));
                                    })
                                    .flatMap(lastVersionNum -> {
                                        inode.setVersionNum(lastVersionNum);
                                        return InodeUtils.updateInode(inode.getBucket(), inode, args.isRecover, false)
                                                .map(b0 -> {
                                                    if (b0) {
                                                        return inode;
                                                    } else {
                                                        return RETRY_INODE;
                                                    }
                                                })
                                                .doOnNext(i -> {
                                                    if (i.getNodeId() > 0) {
                                                        Inode newInode = i.clone();
                                                        newInode.getXAttrMap().remove(QUOTA_KEY);
                                                        vnode.inodeCache.cache.put(i.getNodeId(), newInode);
                                                    }
                                                });
                                    });
                        }

                    }
                });
    }

    private static Mono<Inode> markDeleteInode(Vnode vnode, Inode inode, UpdateArgs args) {
        String bucket = inode.getBucket();
        return BucketSyncSwitchCache.isSyncSwitchOffMono(bucket)
                .doOnNext(isSyncSwitchOff -> {
                    inode.setVersionNum(VersionUtil.getVersionNumMaybeUpdate(isSyncSwitchOff, inode.getNodeId()));
                }).flatMap(v -> {
                    Inode markDeleteInode = inode;
                    if (!inode.isDeleteMark()) {
                        markDeleteInode = INODE_DELETE_MARK.clone().setVersionNum(inode.getVersionNum())
                                .setNodeId(inode.getNodeId())
                                .setBucket(inode.getBucket())
                                .setUid(inode.getUid())
                                .setGid(inode.getGid())
                                .setCookie(inode.getCookie())
                                .setVersionId(inode.getVersionId() == null ? "null" : inode.getVersionId())
                                .setMtime(inode.getMtime())
                                .setMtimensec(inode.getMtimensec())
                                .setObjName(inode.getObjName());
                    }
                    return InodeUtils.updateInode(inode.getBucket(), markDeleteInode, args.isRecover, true)
                            .flatMap(b -> {
                                if (b) {
                                    return deleteChunkMeta(inode);
                                } else {
                                    return Mono.just(b);
                                }
                            }).map(b0 -> inode)
                            .doOnNext(i -> {
                                if (i.getNodeId() > 0) {
                                    vnode.inodeCache.cache.remove(i.getNodeId());
                                }
                            });
                });

    }

    private static Mono<Inode> createHardLink(Vnode vnode, Inode linkInode, Inode oldInode) {
        long stamp = System.currentTimeMillis();
        linkInode.setCtime(stamp / 1000)
                .setCtimensec((int) (System.nanoTime() % ONE_SECOND_NANO));
        String bucket = linkInode.getBucket();
        return FSQuotaUtils.addQuotaDirInfo(linkInode, stamp, false)
                .onErrorResume(e -> {
                    log.info("create hard link error,bucket:{},objName:{}", bucket, linkInode.getObjName(), e);
                    if (e instanceof NFSException) {
                        if (((NFSException) e).getErrCode() == NfsErrorNo.NFS3ERR_DQUOT) {
                            return Mono.just(FILES_QUOTA_EXCCED_INODE);
                        }
                    }
                    return Mono.just(RETRY_INODE);
                })
                .flatMap(newInode -> {
                    if (newInode.getLinkN() < 0) {
                        return Mono.just(newInode);
                    }
                    return NFSBucketInfo.getBucketInfoReactive(bucket)
                            .flatMap(bucketInfo -> {
                                String s3Id = bucketInfo.get("user_id");
                                String s3Name = bucketInfo.get("user_name");
                                MetaData metaData = InodeUtils.newFsMetaMeta(bucket, bucketInfo, linkInode, stamp, VersionUtil.getVersionNum(), s3Id, s3Name);
                                metaData.cookie = VersionUtil.newInode();
                                StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
                                String bucketVnode = pool.getBucketVnodeId(bucket);
                                List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();
                                String key = Utils.getMetaDataKey(bucketVnode, bucket, linkInode.getObjName(), metaData.versionId, metaData.stamp);
                                return ErasureClient.getFsMetaVerUnlimited(oldInode.getBucket(), oldInode.getObjName(), oldInode.getVersionId(), nodeList, null, null, null,
                                        linkInode.getXAttrMap().get(QUOTA_KEY))
                                        .flatMap(sourceMeta -> {
                                            if (sourceMeta.isAvailable()) {
                                                //创建硬链接时，硬链接metaData中的sysMeta与源文件保持一致，owner和名称也一致
                                                metaData.setSysMetaData(sourceMeta.getSysMetaData());
                                                metaData.setStamp(sourceMeta.getStamp());
                                            }
                                            EsMeta esMeta = null;
                                            String mda = NFSBucketInfo.getBucketInfo(oldInode.getBucket()).get(ES_SWITCH);
                                            if (ES_ON.equals(mda)) {
                                                esMeta = EsMeta.inodeMetaMapEsMeta(metaData, linkInode);
                                            }
                                            return ErasureClient.putMetaData(key, metaData, nodeList, esMeta, mda)
                                                    .map(b -> b ? linkInode : RETRY_INODE)
                                                    .doOnNext(i -> {
                                                        if (i.getNodeId() > 0) {
                                                            i.getXAttrMap().remove(QUOTA_KEY);
                                                            vnode.inodeCache.cache.put(i.getNodeId(), i.clone());
                                                        } else {
                                                            log.info("put meta {} fail", metaData);
                                                        }
                                                    });
                                        });
                            });
                });

    }

    private static void updateInodeName(UpdateArgs args, Inode inode, String newObjName, String oldObjName) {
//        //该文件已被重命名
//        if (inode.getObjName().equals(newObjName)) {
//            args.hasReName = true;
//            return;
//        }
        inode.setObjName(newObjName);
        //软连接不修改引用名
        if ((inode.getMode() & S_IFMT) != S_IFLNK) {
            inode.setReference(newObjName);
        }
    }

    private static Mono<Inode> rename(UpdateArgs args, Vnode vnode, String oldObjName, String newObjName, String
            bucket, Inode renameInode) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);
        // 修改inode ctime 和 ctimensec
        long stamp = System.currentTimeMillis();
        int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
        renameInode.setCtime(stamp / 1000)
                .setCtimensec(stampNano);
        renameInode.setCifsMode(CifsUtils.changeToHiddenCifsMode(newObjName, renameInode.getCifsMode(), true));


        // 源文件置为deleteMark；versionNum置为最新，使当存在异常节点未rename时，up后会在list修复getObjectMetaVersion时修复为deleteMark
        MetaData deleteMark = new MetaData()
                .setBucket(bucket)
                .setKey(oldObjName)
                .setDeleteMark(true)
                .setVersionId("null")  // 暂时未适配versionId
                .setStamp(String.valueOf(stamp));

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("vnode", bucketVnode)
                .put("bucket", bucket)
                .put("newObj", newObjName)
                .put("oldObj", oldObjName)
                .put("deleteMark", Json.encode(deleteMark));

        String key = Inode.getKey(bucketVnode, renameInode.getBucket(), renameInode.getNodeId());

        return RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX)
                .hget(bucket, "storage_strategy")
                .flatMap(storageStrategy -> {
                    return RedisConnPool.getInstance().getReactive(REDIS_POOL_INDEX).hexists(storageStrategy, "cache")
                            .flatMap(existCache -> {
                                if (existCache) {
                                    return RedisConnPool.getInstance()
                                            .getReactive(REDIS_POOL_INDEX)
                                            .hget(storageStrategy, "cache")
                                            .map(cacheList -> !"[]".equals(cacheList));
                                }
                                return Mono.just(false);
                            });
                }).
                        flatMap(r -> {
                            //存在缓存池，则更新deleteMark，防止下刷失败
                            if (r) {
                                deleteMark.inode = renameInode.getNodeId();
                                deleteMark.partInfos = new PartInfo[0];
                                deleteMark.partUploadId = "inode";
                                msg.put("deleteMark", Json.encode(deleteMark));
                            }
                            return pool.mapToNodeInfo(bucketVnode);
                        })
                .flatMap(nodeList -> ErasureClient.getFsMetaVerUnlimited(renameInode.getBucket(), oldObjName, renameInode.getVersionId(), nodeList, null, null, null,
                        renameInode.getXAttrMap().get(QUOTA_KEY))
                        .flatMap(sourceMeta -> {
                            if (sourceMeta.isAvailable() && sourceMeta.inode == renameInode.getNodeId()) {
                                return BucketSyncSwitchCache.isSyncSwitchOffMono(bucket)
                                        .flatMap(isSync -> Mono.just(VersionUtil.getVersionNumMaybeUpdate(isSync, renameInode.getNodeId()))).flatMap(versionNum0 -> {
                                            deleteMark.setVersionNum(versionNum0);
                                            msg.put("versionNum", versionNum0);
                                            msg.put("deleteMark", Json.encode(deleteMark));
                                            return FsUtils.rename(pool, msg, nodeList, stamp / 1000, stampNano, renameInode.getNodeId(), key);
                                        });
                            }
                            int r = ERROR_META.equals(sourceMeta) ? -1 : 0;
                            return Mono.just(r);
                        }))
                .flatMap(b -> {
                    if (b >= 0) {
                        if (b == 0) {
                            return Mono.just(NOT_FOUND_INODE);
                        }
                        if ((renameInode.getMode() & S_IFMT) == S_IFDIR) {
                            String quotaBucketKey = FSQuotaUtils.getQuotaBucketKey(bucket);
                            Map<String, FSQuotaConfig> newQuotaConfigs = new ConcurrentHashMap<>();
                            //重命名目录，如果目录配置了文件配额，需更新redis中该目录的quotaConfig信息
                            return Flux.fromIterable(FSQuotaUtils.getAllTypeQuotaKeys(bucket, renameInode.getNodeId()))
                                    .flatMap(quotaKey -> {
                                        return RedisConnPool.getInstance().getReactive(REDIS_FS_QUOTA_INFO_INDEX)
                                                .hget(quotaBucketKey, quotaKey)
                                                .defaultIfEmpty("")
                                                .flatMap(quotaStr -> {
                                                    if (StringUtils.isBlank(quotaStr)) {
                                                        return Mono.just(true);
                                                    }
                                                    newQuotaConfigs.compute(quotaKey, (k, v) -> {
                                                        FSQuotaConfig fsQuotaConfig = Json.decodeValue(quotaStr, FSQuotaConfig.class);
                                                        fsQuotaConfig.setDirName(newObjName);
                                                        return fsQuotaConfig;
                                                    });
                                                    return Mono.just(true);
                                                });
                                    })
                                    .collectList()
                                    .map(b1 -> {
                                        if (!newQuotaConfigs.isEmpty()) {
                                            //异步处理防止阻塞
                                            FS_QUOTA_EXECUTOR.schedule(() -> {
                                                for (String k : newQuotaConfigs.keySet()) {
                                                    FSQuotaConfig newConfig = newQuotaConfigs.get(k);
                                                    RedisConnPool.getInstance().getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX)
                                                            .hset(quotaBucketKey, k, Json.encode(newConfig));
                                                    int id = getIdByQuotaType(newConfig);

                                                    //去除旧目录名的告警
                                                    changeFsQuotaAlarmNormal(bucket, newConfig.getNodeId(), newConfig.getQuotaType(), id, newConfig.getS3AccountName(), oldObjName, true);
                                                }
                                            });
                                            FS_QUOTA_EXECUTOR.schedule(() -> {
                                                FSQuotaScannerTask.syncQuotaCache(bucket, null, QUOTA_CACHE_MODIFY_OPT, newObjName, renameInode.getNodeId()).subscribe();
                                            });
                                        }
                                        return renameInode;
                                    })
                                    .timeout(Duration.ofSeconds(20))
                                    .onErrorResume(e -> {
                                        return Mono.just(renameInode);
                                    });
                        }
                        return Mono.just(renameInode);
                    } else {
                        return Mono.just(RETRY_INODE);
                    }
                })
                .doOnNext(i -> {
                    if (i.getNodeId() > 0) {
                        vnode.inodeCache.cache.put(i.getNodeId(), i.clone());

                        if (i.getLinkN() > 0) {
                            int filter = oldObjName.endsWith("/") ? FILE_NOTIFY_CHANGE_DIR_NAME : FILE_NOTIFY_CHANGE_FILE_NAME;
                            if (CifsUtils.getParentDirName(newObjName).equals(CifsUtils.getParentDirName(oldObjName))) {
                                String[] notifyKeys = new String[]{oldObjName, newObjName};
                                NotifyAction[] actions = new NotifyAction[]{NotifyAction.FILE_ACTION_RENAMED_OLD_NAME, NotifyAction.FILE_ACTION_RENAMED_NEW_NAME};

                                NotifyServer.getInstance().maybeNotify(bucket, notifyKeys, filter, actions);
                            } else {
                                NotifyServer.getInstance().maybeNotify(bucket, oldObjName, filter, NotifyAction.FILE_ACTION_REMOVED);
                                NotifyServer.getInstance().maybeNotify(bucket, newObjName, filter, NotifyAction.FILE_ACTION_ADDED);
                            }
                        }
                    }
                });

    }

    private static Mono<Inode> putAndCacheInode(String bucket, long nodeId, UpdateArgs args, Inode oldInode, Inode inode,
                                                Vnode vnode, String versionNum) {
        return BucketSyncSwitchCache.isSyncSwitchOffMono(bucket)
                .flatMap(isSync -> {
                    if (!StringUtils.isBlank(versionNum)) {
                        return Mono.just(versionNum);
                    }
                    return Mono.just(VersionUtil.getVersionNumMaybeUpdate(isSync, inode.getNodeId()));
                })
                .flatMap(versionNum0 -> {
                    inode.setVersionNum(versionNum0);
                    if (args.needDeleteMap == null) {
                        args.needDeleteMap = new ConcurrentHashMap<>();
                    }
                    return dataModel.updateChunk(inode, args.needDeleteMap)
                            .flatMap(i -> {
                                if (i.equals(ERROR_INODE)) {
                                    return Mono.just(RETRY_INODE);
                                }

                                if (inode.getSize() != inode.getInodeData().stream().mapToLong(d -> d.size).sum()) {
                                    log.info("nodeId:{},error inode size", nodeId);
                                }

                                return putInode(bucket, args, oldInode, inode);
                            })
                            .doOnNext(i -> {
                                if (i.getNodeId() > 0 && !i.isDeleteMark()) {
                                    i.setChunkFileMap(null);
                                    // 在存储cache时释放存储file更新状态的map
                                    Inode newInode = i.clone();
                                    newInode.setUpdateInodeDataStatus(null);
                                    newInode.getXAttrMap().remove(QUOTA_KEY);
                                    vnode.inodeCache.cache.put(nodeId, newInode);
                                    args.needDeleteMap.forEach((k, v) -> {
                                        if (!v.isEmpty()) {
                                            if ("split".equals(k)) {
                                                FsUtils.fsExecutor.submit(() -> {
                                                    for (InodeData data : v) {
                                                        FsUtils.checkFileInInode(bucket, nodeId, data.fileName, "", "")
                                                                .flatMap(t -> {
                                                                    if (t.var1 <= 0) {
                                                                        return FsUtils.deleteFile0(Collections.singletonList(data), inode.getBucket());
                                                                    } else {
                                                                        return Mono.just(true);
                                                                    }
                                                                }).subscribe();
                                                    }
                                                });
                                            } else {
                                                FsUtils.fsExecutor.submit(() -> {
                                                    FsUtils.deleteFile0(v, inode.getBucket()).subscribe();
                                                });
                                            }
                                        }
                                    });
                                }
                                // inode元数据更新异常时，为确保缓存与索引盘中数据一致，需清除缓存
                                if (i.getLinkN() == RETRY_INODE.getLinkN()) {
                                    vnode.inodeCache.cache.remove(nodeId);
                                }
                                if (args.removeCache) {
                                    vnode.inodeCache.cache.remove(nodeId);
                                }
                            });
                });

    }

    private static void setattr(Inode inode, ObjAttr attr, UpdateArgs args, boolean isAllocate, String bucketOwner) {
        long stamp = System.currentTimeMillis();
        int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
        inode.setCtime(stamp / 1000)
                .setCtimensec(stampNano);
        if (attr.hasMode != 0) {
            int oldMode = inode.getMode();
            if (CIFSACL.isExistCifsACE(inode)) {
                InodeUtils.changeMode(inode, attr.mode);
                //除了修改原本的mode之外，如果存在cifsACE，还要将对应u、g、o的cifsACE修改
                Inode.updateCifsUGOACL(inode, oldMode);
            } else {
                //仅存在mask权限，且要修改group权限时，实际只修改mask权限
                if (existNFSMask(inode)) {
                    if (needChangeGroupMode(inode.getMode(), attr.mode)) {
                        int groupRight = NFSACL.parseModeToInt(attr.mode, NFSACL_GROUP_OBJ);

                        //修改mask
                        ListIterator<Inode.ACE> listIterator = inode.getACEs().listIterator();
                        while (listIterator.hasNext()) {
                            Inode.ACE ace = listIterator.next();
                            int type = ace.getNType();
                            if (NFSACL_CLASS == type) {
                                int mask = ace.getRight();
                                if (groupRight != mask) {
                                    listIterator.remove();
                                    listIterator.add(new ACE(type, groupRight, ace.getId()));
                                }
                                break;
                            }
                        }

                        //修改除group外的权限
                        int newMode = NFSACL.refreshMode(attr.mode, NFSACL_GROUP_OBJ);
                        newMode |= (NFSACL.refreshMode(inode.getMode(), NFSACL_USER_OBJ) & NFSACL.refreshMode(inode.getMode(), NFSACL_OTHER));
                        InodeUtils.changeMode(inode, newMode);
                    } else {
                        InodeUtils.changeMode(inode, attr.mode);
                    }
                } else {
                    InodeUtils.changeMode(inode, attr.mode);
                }
            }

            args.notifyFilters |= FILE_NOTIFY_CHANGE_ATTRIBUTES;
        }

        if (attr.hasCifsMode != 0) {
            InodeUtils.changeCifsMode(inode, attr.cifsMode);
            args.notifyFilters |= FILE_NOTIFY_CHANGE_ATTRIBUTES;
        }

        if (attr.hasUid != 0) {
            int oriUid = inode.getUid();
            //改变uid时，需要同步更改对象ACL，清除原本对象acl中所有的owner相关权限，同时将owner改为attr.uid对应的s3Id
            ACLUtils.updateObjAcl(inode, oriUid, attr.uid, bucketOwner);
            inode.setUid(attr.uid);
            args.notifyFilters |= FILE_NOTIFY_CHANGE_ATTRIBUTES;
        }
        if (attr.hasGid != 0) {
            inode.setGid(attr.gid);
            args.notifyFilters |= FILE_NOTIFY_CHANGE_ATTRIBUTES;
        }
        if (attr.hasMtime != 0) {
            args.notifyFilters |= FILE_NOTIFY_CHANGE_LAST_WRITE;
            inode.setMtime(attr.hasMtime == 1 ? stamp / 1000 : attr.mtime);
            inode.setMtimensec(attr.hasAtime == 1 ? stampNano : attr.mtimeNano);
        }
        if (attr.hasAtime != 0) {
            args.notifyFilters |= FILE_NOTIFY_CHANGE_LAST_ACCESS;
            inode.setAtime(attr.hasAtime == 1 ? stamp / 1000 : attr.atime);
            inode.setAtimensec(attr.hasAtime == 1 ? stampNano : attr.atimeNano);
        }

        if (attr.hasCtime != 0) {
            args.notifyFilters |= FILE_NOTIFY_CHANGE_LAST_WRITE;
            inode.setCtime(attr.hasCtime == 1 ? stamp / 1000 : attr.ctime);
            inode.setCtimensec(attr.hasCtime == 1 ? stampNano : attr.ctimeNano);
        }

        if (attr.hasCreateTime != 0) {
            args.notifyFilters |= FILE_NOTIFY_CHANGE_CREATION;
            inode.setCreateTime(attr.createTime);
        }
        if (attr.hasSize != 0) {
            args.notifyFilters |= FILE_NOTIFY_CHANGE_ATTRIBUTES & FILE_NOTIFY_CHANGE_SIZE;
            if (attr.size == inode.getSize()) {
                return;
            }
            if (isAllocate && attr.size != 0) {
                return;
            }
            args.needDelete = true;
            inode.setMtime(stamp / 1000)
                    .setMtimensec(stampNano);
            if (attr.size < inode.getSize()) {
                inode.setSize(attr.size);
                List<Inode.InodeData> list = inode.getInodeData();
                List<Inode.InodeData> newList = new LinkedList<>();
                long offset = 0L;
                for (Inode.InodeData inodeData : list) {
                    if (offset + inodeData.size <= attr.size) {
                        newList.add(inodeData);
                        offset += inodeData.size;
                    } else if (offset < attr.size) {
                        inodeData.size0 += inodeData.size - (attr.size - offset);
                        inodeData.size = attr.size - offset;
                        newList.add(inodeData);
                        offset = attr.size;
                    } else {
                        break;
                    }
                }
                inode.setInodeData(newList);
            } else {
                //add hole file
                List<InodeData> list = inode.getInodeData();
                dataModel.append(list, InodeData.newHoleFile(attr.size - inode.getSize()), inode, inode.getSize());
                inode.setSize(attr.size);
            }
            args.removeCache = true;
        }

        args.updateRootTime = true;
    }

    private static Mono<Inode> putInodeAfterSetAttr(UpdateArgs args, Inode oldInode, Inode inode, String bucket, ObjAttr attr, long nodeId, Vnode vnode) {
        return Mono.just(attr.hasSize != 0)
                .flatMap(setSize -> {
                    if (setSize) {
                        return FSQuotaUtils.addQuotaDirInfo(inode, System.currentTimeMillis(), true)
                                .onErrorResume(e -> {
                                    log.error("addQuotaDirInfo error.", e);
                                    if (e instanceof NFSException) {
                                        NFSException nfsException = (NFSException) e;
                                        if (nfsException.getErrCode() == NfsErrorNo.NFS3ERR_DQUOT) {
                                            return Mono.just(CAP_QUOTA_EXCCED_INODE);
                                        }
                                    }
                                    return Mono.just(ERROR_INODE);
                                })
                                .flatMap(i -> {
                                    if (i.getLinkN() == CAP_QUOTA_EXCCED_INODE.getLinkN()) {
                                        return Mono.just(CAP_QUOTA_EXCCED_INODE);
                                    }
                                    return putAndCacheInode(bucket, nodeId, args, oldInode, inode, vnode, "");
                                });
                    } else {
                        if (attr.hasUid != 0 || attr.hasGid != 0) {
                            List<Integer> uidList = new ArrayList<>();
                            List<Integer> gidList = new ArrayList<>();
                            if (oldInode.getUid() != inode.getUid()) {
                                uidList.add(inode.getUid());
                                uidList.add(oldInode.getUid());
                            }
                            if (oldInode.getGid() != inode.getGid()) {
                                gidList.add(inode.getGid());
                                gidList.add(oldInode.getGid());
                            }
                            return FSQuotaUtils.existQuotaInfoListUserOrGroup(bucket, inode.getObjName(), System.currentTimeMillis(), uidList, gidList)
                                    .flatMap(t2 -> {
                                        if (t2.var1) {
                                            inode.getXAttrMap().put(QUOTA_KEY, Json.encode(t2.var2));
                                        }
                                        return putAndCacheInode(bucket, nodeId, args, oldInode, inode, vnode, "");
                                    });
                        }
                        return putAndCacheInode(bucket, nodeId, args, oldInode, inode, vnode, "");
                    }
                });
    }

    private static void updateLinkNAndInodeDataFileName(Inode sourceInode, UpdateArgs args, boolean isAdd, Inode
            newInode) {
        updateLinkN(sourceInode, args, isAdd, newInode.getObjName());
        if (sourceInode.getInodeData().size() == newInode.getInodeData().size()) {
            for (int i = 0; i < sourceInode.getInodeData().size(); i++) {
                sourceInode.getInodeData().get(i).setFileName(newInode.getInodeData().get(i).getFileName());
            }
        }
    }

    private static Mono<Inode> deleteChunkAndPutInode(String bucket, long nodeId, UpdateArgs args, Inode
            oldInode, Inode inode, Vnode vnode, String versionNum) {
        return FsUtils.deleteChunkFile(oldInode.getInodeData(), oldInode.getBucket())
                .flatMap(delRes -> {
                    if (!delRes) {
                        return Mono.just(delRes);
                    }
                    return deleteChunkMeta(oldInode);
                })
                .flatMap(res -> {
                    if (res) {
                        return putAndCacheInode(bucket, nodeId, args, oldInode, inode, vnode, "")
                                .flatMap(i0 -> {
                                    return InodeUtils.updateParentDirTime(inode.getObjName(), inode.getBucket());
                                })
                                .map(i -> oldInode);
                    }
                    return Mono.just(RETRY_INODE);
                });
    }

    private static Mono<Boolean> deleteChunkMeta(Inode inode) {
        String[] chunkFiles = inode.getInodeData()
                .stream()
                .filter(inodeData -> {
                    return StringUtils.isNotEmpty(inodeData.getFileName())
                            && inodeData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY);
                })
                .map(l -> l.fileName)
                .toArray(String[]::new);
        return Flux.fromArray(chunkFiles)
                .flatMap(fileName -> FsUtils.deleteChunkMeta(fileName, inode.getBucket()), 24)
                .collectList()
                .map(l -> true);
    }

    private static Mono<Inode> flushWriteCache(String bucket, long nodeId, Inode inode, SocketReqMsg msg0) {
        if (WriteCacheServer.writeCacheDebug) {
            log.info("opt flushWriteCache {}", nodeId);
        }

        if (InodeUtils.isError(inode)) {
            log.info("flushWriteCache error, bucket: {}, inode: {}", bucket, inode);
            return Mono.just(ERROR_INODE);
        }

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String vnode = pool.getBucketVnodeId(bucket);
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(vnode).block();

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("inode", msg0.get("inode"))
                .put("offset", msg0.get("offset"))
                .put("count", msg0.get("count"))
                .put("type", msg0.get("type"));

        if (msg0.get("end") != null) {
            msg.put("end", msg0.get("end"));
        }

        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> msg.copy())
                .collect(Collectors.toList());

        int type = Integer.parseInt(msg0.get("type"));
        if (type <= 2) {
            return flushWriteCache(nodeList, msgs, nodeId)
                    .flatMap(b -> {
                        if (b) {
                            // 返回inode仅用于flush判断成功失败，
                            return tryDeleteWriteCache(nodeList, msgs, nodeId)
                                    .map(bb -> inode);
                        } else {
                            return Mono.just(ERROR_INODE);
                        }
                    });
        } else if (type == 3) {
            return flushWriteCache(nodeList, msgs, nodeId)
                    .map(b -> b ? inode : ERROR_INODE);
        } else {
            return tryDeleteWriteCache(nodeList, msgs, nodeId)
                    .map(bb -> inode);
        }
    }

    private static Mono<Boolean> flushWriteCache(List<Tuple3<String, String, String>> nodeList, List<SocketReqMsg> msgs, long nodeId) {
        if (nodeList.size() > 0) {
            List<Tuple3<String, String, String>> writeTuple = Collections.singletonList(nodeList.remove(0));
            List<SocketReqMsg> writeMsg = Collections.singletonList(msgs.remove(0).put("write", "1"));

            return tryFlushWriteCache(writeTuple, writeMsg, nodeId)
                    .flatMap(b -> {
                        if (b) {
                            return Mono.just(true);
                        } else {
                            return flushWriteCache(nodeList, msgs, nodeId);
                        }
                    });
        } else {
            return Mono.just(false);
        }
    }

    private static Mono<Boolean> tryFlushWriteCache(List<Tuple3<String, String, String>> writeTuple, List<SocketReqMsg> writeMsg, long nodeId) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(writeMsg, FLUSH_WRITE_CACHE, String.class, writeTuple);
        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("opt flushWriteCache {} fail", writeMsg.get(0));
            res.onNext(false);
        }, () -> {
            if (WriteCacheServer.writeCacheDebug) {
                log.info("opt flushWriteCache {}, success {}, fail {}", nodeId, responseInfo.successNum, responseInfo.errorNum);
            }
            if (responseInfo.successNum < 1) {
                res.onNext(false);
            } else {
                // 返回inode仅用于flush判断成功失败，
                res.onNext(true);
            }
        });

        return res;
    }

    private static Mono<Boolean> tryDeleteWriteCache(List<Tuple3<String, String, String>> nodeList, List<SocketReqMsg> msgs, long nodeId) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, FLUSH_WRITE_CACHE, String.class, nodeList);
        responseInfo.responses.subscribe(tuple3 -> {
        }, e -> {
            log.error("opt deleteWriteCache {} fail", msgs.get(0));
            res.onNext(false);
        }, () -> {
            if (WriteCacheServer.writeCacheDebug) {
                log.info("opt deleteWriteCache {}, success {}, fail {}", nodeId, responseInfo.successNum, responseInfo.errorNum);
            }
            if (responseInfo.successNum <= nodeList.size() / 2) {
                res.onNext(false);
            } else {
                res.onNext(true);
            }
        });

        return res;
    }
}
