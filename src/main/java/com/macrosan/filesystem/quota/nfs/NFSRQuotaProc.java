package com.macrosan.filesystem.quota.nfs;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ErasureClient;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.call.rquota.GetQuotaCall;
import com.macrosan.filesystem.nfs.call.rquota.SetQuotaCall;
import com.macrosan.filesystem.nfs.reply.rquota.QuotaReply;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.FSQuotaConfig;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import static com.macrosan.constants.ServerConstants.SLASH;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_FS_QUOTA_INFO_INDEX;
import static com.macrosan.filesystem.FsConstants.NFSQuotaStatus.Q_EPERM;
import static com.macrosan.filesystem.FsConstants.NFSQuotaStatus.Q_NOQUOTA;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_I0;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_NOENT;
import static com.macrosan.filesystem.quota.FSQuotaConstants.*;
import static com.macrosan.filesystem.utils.FSQuotaUtils.getQuotaBucketKey;
import static com.macrosan.filesystem.utils.FSQuotaUtils.getQuotaTypeKey;
import static com.macrosan.message.jsonmsg.BucketInfo.ERROR_BUCKET_INFO;
import static com.macrosan.message.jsonmsg.BucketInfo.NOT_FOUND_BUCKET_INFO;
import static com.macrosan.message.jsonmsg.DirInfo.ERROR_DIR_INFO;

@Log4j2
public class NFSRQuotaProc {

    protected static RedisConnPool redisConnPool = RedisConnPool.getInstance();

    public Mono<RpcReply> getQuota(RpcCallHeader callHeader, ReqInfo reqHeader, GetQuotaCall call) {
        QuotaReply reply = new QuotaReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        return Mono.just(call.mountPath)
                .flatMap(pathNameBytes -> {
                    String path = new String(pathNameBytes);
                    if (path.endsWith("/")) {
                        path = path.substring(0, path.length() - 1);
                    }
                    String[] split = path.split("/");
                    if (split.length < 3) {
                        reply.status = NFS3ERR_NOENT;
                        return Mono.just(reply);
                    }
                    String bucket = split[2];
                    //查询的为根目录，桶配额
                    if (split.length == 3) {
                        return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX)
                                .exists(bucket)
                                .flatMap(isExist -> {
                                    if (isExist != 1) {
                                        reply.status = NFS3ERR_NOENT;
                                        return Mono.just(reply);
                                    }
                                    return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX)
                                            .hgetall(bucket)
                                            .flatMap(bucketInfo -> {
                                                return ErasureClient.reduceBucketInfo(bucket)
                                                        .flatMap(info -> {
                                                            if (ERROR_BUCKET_INFO.getVersionNum().equals(info.getVersionNum())) {
                                                                reply.status = NFS3ERR_I0;
                                                                return Mono.just(reply);
                                                            }
                                                            if (NOT_FOUND_BUCKET_INFO.getVersionNum().equals(info.getVersionNum())) {
                                                                reply.status = NFS3ERR_NOENT;
                                                                return Mono.just(reply);
                                                            }
                                                            QuotaReply.buildReply(bucketInfo.getOrDefault(S3_BUCKET_CAP_HARD_LIMIT, "0"),
                                                                    bucketInfo.getOrDefault(S3_BUCKET_CAP_SOFT_LIMIT, "0"),
                                                                    info.getBucketStorage(),
                                                                    bucketInfo.getOrDefault(S3_BUCKET_OBJ_NUM_HARD_LIMIT, "0"),
                                                                    bucketInfo.getOrDefault(S3_BUCKET_OBJ_NUM_SOFT_LIMIT, "0"),
                                                                    info.getObjectNum(),
                                                                    reply);
                                                            if (NFS.nfsDebug) {
                                                                log.info("getQuota reply:{}", reply);
                                                            }
                                                            return Mono.just(reply);
                                                        });
                                            });
                                });
                    }
                    //目录配额信息
                    String dirName = path.substring(path.indexOf(bucket) + bucket.length() + 1) + SLASH;
                    return getQuotaInfo(dirName, bucket, reply, false, call.type, call.id);
                });
    }

    public Mono<RpcReply> setQuota(RpcCallHeader callHeader, ReqInfo reqHeader, SetQuotaCall call) {
        QuotaReply reply = new QuotaReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        //检测配额值是否合法
        if (!FSQuotaUtils.checkLimitValue(call.blockHardLimit, call.blockSoftLimit)
                || !FSQuotaUtils.checkLimitValue(call.filesHardLimit, call.filesSoftLimit)
        ) {
            reply.status = Q_EPERM;
            return Mono.just(reply);
        }
        return Mono.just(call.path)
                .flatMap(pathNameBytes -> {
                    String[] path = new String[]{new String(pathNameBytes)};
                    if (path[0].endsWith("/")) {
                        path[0] = path[0].substring(0, path[0].length() - 1);
                    }
                    String[] split = path[0].split("/");

                    //不允许对根目录设置配额，如需设置，要在控制台设置桶配额
                    if (split.length <= 3) {
                        reply.status = Q_EPERM;
                        return Mono.just(reply);
                    }
                    String bucket = split[2];
                    return NFSBucketInfo.getBucketInfoReactive(bucket)
                            .flatMap(bucketInfo -> {
                                if (bucketInfo.isEmpty()) {
                                    reply.status = Q_EPERM;
                                    return Mono.just(reply);
                                }
                                String dirName = path[0].substring(path[0].indexOf(bucket) + bucket.length() + 1) + SLASH;
                                int type = call.quotaType;
                                int[] id_ = new int[1];
                                if (type == FS_USER_QUOTA || type == FS_GROUP_QUOTA) {
                                    id_[0] = call._id;
                                } else {
                                    reply.status = Q_EPERM;
                                    return Mono.just(reply);
                                }
                                return FsUtils.lookup(bucket, dirName, null, false, -1, null)
                                        .flatMap(inode -> {
                                            if (InodeUtils.isError(inode)) {
                                                log.info("setQuota path:{} type:{} id:{}，dirName:{}", path[0], type, id_[0], dirName);
                                                reply.status = Q_EPERM;
                                                return Mono.just(reply);
                                            }
                                            return redisConnPool.getReactive(REDIS_FS_QUOTA_INFO_INDEX)
                                                    .hget(getQuotaBucketKey(bucket), getQuotaTypeKey(bucket, inode.getNodeId(), type, id_[0]))
                                                    .defaultIfEmpty("")
                                                    .flatMap(configStr -> {
                                                        //后续需更加call.type来判断配额的类型，目前只支持目录的配额设置
                                                        FSQuotaConfig fsQuotaConfig = null;
                                                        if (StringUtils.isNotBlank(configStr)) {
                                                            fsQuotaConfig = Json.decodeValue(configStr, FSQuotaConfig.class);
                                                            fsQuotaConfig.setFilesSoftQuota((long) call.filesSoftLimit);
                                                            fsQuotaConfig.setFilesHardQuota((long) call.filesHardLimit);
                                                            fsQuotaConfig.setCapacityHardQuota(1024L * call.blockHardLimit);
                                                            fsQuotaConfig.setCapacitySoftQuota(1024L * call.blockSoftLimit);
                                                            fsQuotaConfig.setModifyTime(System.currentTimeMillis());
                                                            fsQuotaConfig.setModify(true);
                                                        } else {
                                                            long stamp = System.currentTimeMillis();
                                                            fsQuotaConfig = new FSQuotaConfig()
                                                                    .setBucket(bucket)
                                                                    .setDirName(dirName)
                                                                    .setQuotaType(type)
                                                                    .setFilesSoftQuota((long) call.filesSoftLimit)
                                                                    .setFilesHardQuota((long) call.filesHardLimit)
                                                                    .setCapacityHardQuota(1024L * call.blockHardLimit)
                                                                    .setCapacitySoftQuota(1024L * call.blockSoftLimit)
                                                                    .setUid(call._id)
                                                                    .setGid(call._id)
                                                                    .setS3AccountName(bucketInfo.get("user_name"))
                                                                    .setStartTime(stamp)
                                                                    .setModifyTime(stamp)
                                                                    .setModify(false)
                                                                    .setNodeId(inode.getNodeId())
                                                                    .setFilesTimeLeft(call.filesTimeLeft)
                                                                    .setBlockTimeLeft(call.blockTimeLeft);
                                                        }
                                                        return FSQuotaRealService.setFsQuotaInfo(fsQuotaConfig, bucketInfo)
                                                                .onErrorResume(e -> Mono.just(-1))
                                                                .flatMap(res -> {
                                                                    if (res == -2) {
                                                                        reply.status = Q_NOQUOTA;
                                                                        return Mono.just(reply);
                                                                    }
                                                                    if (res != 0) {
                                                                        reply.status = Q_EPERM;
                                                                        return Mono.just(reply);
                                                                    }
                                                                    int id = call._id;
                                                                    return getQuotaInfo(dirName, bucket, reply, true, call.quotaType, id);
                                                                });

                                                    });
                                        });

                            });


                });
    }

    public Mono<RpcReply> getQuotaInfo(String dirName, String bucket, QuotaReply reply, boolean setFlag, int type, int id) {
        return FsUtils.lookup(bucket, dirName, null, false, -1, null)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        reply.status = Q_NOQUOTA;
                        return Mono.just(reply);
                    }
                    return Mono.just(setFlag)
                            .flatMap(flag -> {
                                if (flag) {
                                    return getQuotaInfo0(inode.getNodeId(), bucket, reply, type, id);
                                }
                                return redisConnPool.getReactive(REDIS_FS_QUOTA_INFO_INDEX)
                                        .hexists(getQuotaBucketKey(bucket), getQuotaTypeKey(bucket, inode.getNodeId(), type, id))
                                        .flatMap(isExist -> {
                                            if (!isExist) {
                                                reply.status = Q_NOQUOTA;
                                                return Mono.just(reply);
                                            }
                                            return getQuotaInfo0(inode.getNodeId(), bucket, reply, type, id);
                                        });

                            });
                });

    }

    public Mono<RpcReply> getQuotaInfo0(long dirNodeId, String bucket, QuotaReply reply, int type, int id) {
        return redisConnPool.getReactive(REDIS_FS_QUOTA_INFO_INDEX)
                .hget(getQuotaBucketKey(bucket), getQuotaTypeKey(bucket, dirNodeId, type, id))
                .flatMap(quotaConfig -> {
                    FSQuotaConfig fsQuotaConfig = Json.decodeValue(quotaConfig, FSQuotaConfig.class);
                    return FSQuotaRealService.getFsQuotaInfo(bucket, dirNodeId, type, id)
                            .flatMap(dirInfo -> {
                                if (ERROR_DIR_INFO.getFlag().equals(dirInfo.getFlag())) {
                                    reply.status = Q_NOQUOTA;
                                    return Mono.just(reply);
                                }
                                QuotaReply.buildReply(fsQuotaConfig.getCapacityHardQuota() + "",
                                        fsQuotaConfig.getCapacitySoftQuota() + "",
                                        dirInfo.getUsedCap(),
                                        fsQuotaConfig.getFilesHardQuota() + "",
                                        fsQuotaConfig.getFilesSoftQuota() + "",
                                        dirInfo.getUsedObjects(),
                                        reply);
                                return Mono.just(reply);
                            });
                });
    }
}
