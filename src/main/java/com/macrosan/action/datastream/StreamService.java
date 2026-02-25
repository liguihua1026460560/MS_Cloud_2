package com.macrosan.action.datastream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.BaseEncoding;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.doubleActive.AssignClusterHandler;
import com.macrosan.doubleActive.DataSynChecker;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.part.PartClient;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.RpcCallHeader;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.ReadObjClient;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.S3ACL;
import com.macrosan.httpserver.MossHttpClient;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.RestfulVerticle;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.consturct.ErrMsgBuilder;
import com.macrosan.message.consturct.RequestBuilder;
import com.macrosan.message.consturct.SocketReqMsgBuilder;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.Error;
import com.macrosan.message.xmlmsg.*;
import com.macrosan.message.xmlmsg.section.*;
import com.macrosan.message.xmlmsg.versions.ListVersionsResult;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.storage.client.channel.ListObjectMergeChannel;
import com.macrosan.storage.client.channel.ListVersionsMergeChannel;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.storage.crypto.common.CryptoAlgorithm;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.cache.Md5DigestPool;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.LongLongTuple;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import com.macrosan.utils.params.DelMarkParams;
import com.macrosan.utils.perf.KeepAliveRequest;
import com.macrosan.utils.policy.ReactorPolicyCheckUtils;
import com.macrosan.utils.quota.QuotaRecorder;
import com.macrosan.utils.regex.PatternConst;
import com.macrosan.utils.serialize.JaxbUtils;
import com.macrosan.utils.serialize.JsonUtils;
import com.macrosan.utils.store.StoreManagementServer;
import io.netty.handler.codec.http.HttpRequest;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.Http1xServerConnection;
import io.vertx.core.http.impl.HttpServerRequestImpl;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.net.NetSocket;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.macrosan.action.datastream.ActiveService.*;
import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.AssignClusterHandler.ClUSTER_NAME_HEADER;
import static com.macrosan.doubleActive.AssignClusterHandler.SKIP_SIGNAL;
import static com.macrosan.doubleActive.DoubleActiveUtil.buildSyncRecord;
import static com.macrosan.doubleActive.DoubleActiveUtil.streamDispose;
import static com.macrosan.doubleActive.HeartBeatChecker.isMultiAliveStarted;
import static com.macrosan.doubleActive.HeartBeatChecker.isNotDeleteEs;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.*;
import static com.macrosan.ec.ECUtils.*;
import static com.macrosan.ec.ErasureClient.*;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.OVER_WRITE;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.filesystem.utils.FSQuotaUtils.s3AddQuotaDirInfo;
import static com.macrosan.filesystem.utils.InodeUtils.isError;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.httpserver.ResponseUtils.*;
import static com.macrosan.httpserver.RestfulVerticle.REQUEST_VERSIONNUM;
import static com.macrosan.httpserver.RestfulVerticle.requestVersionNumMap;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.buildRefactorCopyMessage;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.buildUploadMsg;
import static com.macrosan.message.jsonmsg.BucketInfo.ERROR_BUCKET_INFO;
import static com.macrosan.message.jsonmsg.Inode.CAP_QUOTA_EXCCED_INODE;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;
import static com.macrosan.storage.crypto.impl.AES256Crypto.DEFAULT_AES256_SECRET_KEY;
import static com.macrosan.storage.strategy.StorageStrategy.POOL_STRATEGY_MAP;
import static com.macrosan.utils.authorize.AuthorizeV4.X_AMZ_DECODED_CONTENT_LENGTH;
import static com.macrosan.utils.msutils.MsAclUtils.*;
import static com.macrosan.utils.msutils.MsException.dealException;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;
import static com.macrosan.utils.notification.BucketNotification.checkNotification;
import static com.macrosan.utils.notification.BucketNotification.saveInfoToQueue;
import static com.macrosan.utils.perf.KeepAliveRequest.KEEP_SOCKET_PERIOD;
import static com.macrosan.utils.regex.PatternConst.OBJECT_NAME_PATTERN;
import static com.macrosan.utils.store.StoreManagementServer.*;
import static com.macrosan.utils.store.StoreUtils.validateRequest;
import static com.macrosan.utils.trash.TrashUtils.bucketTrash;
import static com.macrosan.utils.worm.WormUtils.*;
import static reactor.adapter.rxjava.RxJava2Adapter.singleToMono;

/**
 * StreamService
 * 处理上传下载等数据流
 *
 * @author liyixin
 * @date 2018/10/28
 */
@SuppressWarnings("LanguageDetectionInspection")
public class StreamService extends BaseService {

    private static final Logger logger = LogManager.getLogger(StreamService.class);

    private static final Logger delObjLogger = LogManager.getLogger("DeleteObjLog.StreamService");

    private static StreamService instance = null;

    private final String format = String.format("%-1024s", "");

    public static final Vertx vertx = ServerConfig.getInstance().getVertx();

    private static final String LIST_TYPE_VER = "2";

    private StreamService() {
        super();
        processor.subscribe(this::listObjects);
    }

    public static StreamService getInstance() {
        if (instance == null) {
            instance = new StreamService();
        }
        return instance;
    }

    private int copyObject(MsHttpRequest request) {
        String copySource = request.getHeader(X_AMZ_COPY_SOURCE);
        if (StringUtils.isNotBlank(request.getHeader(SYNC_COPY_SOURCE))) {
            copySource = request.getHeader(SYNC_COPY_SOURCE);
        }
        String sourceVersionId;
        try {
            String[] copySourceArr = copySource.split("\\?", 2);
            sourceVersionId = copySourceArr.length == 1 ? "" : copySourceArr[1].substring(copySourceArr[1].indexOf("=") + 1);
            copySource = URLDecoder.decode(copySourceArr[0], "UTF-8");
            if (copySource.startsWith(SLASH)) {
                copySource = copySource.substring(1);
            }
        } catch (UnsupportedEncodingException e) {
            throw new MsException(INVALID_ARGUMENT, "The charset of x-amz-copy-source is incorrect!");
        }
        String[] array = copySource.split("/", 2);
        if (array.length < 2) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "The format of x-amz-copy-source is incorrect!");
        }

        final String wormExpire = request.getHeader(X_AMZ_WORM_DATE_HEADER);
        final String wormMode = request.getHeader(X_AMZ_WORM_MODE_HEADER);

        final String sourceBucket = ServerConfig.isBucketUpper() ? array[0].toLowerCase() : array[0];
        final String sourceObject = array[1];
        final String targetBucket = request.getBucketName();
        final String targetObject = request.getObjectName();
        final String userId = request.getUserId();
        final String requestId = getRequestId();

        //进行请求头中参数校验
        Map<String, String> heads = new HashMap<>(16);
        checkMetaDirective(sourceBucket, sourceObject, request);
        checkCondition(heads, request);
        checkObjectName(targetObject);
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(targetBucket);

        com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(targetBucket, targetObject);
        final String targetBucketVnode = bucketVnodeIdTuple.var1;
        final String targetBucketMigrateVnode = bucketVnodeIdTuple.var2;
        final String userMeta = getUserMeta(request).toString();
        //获取桶策略校验所需要的参数 policy[0]存储源桶校验结果;policy[1]存储目标桶校验结果
        final int[] policy = new int[1];
        final String sourceMethod = "".equals(sourceVersionId) ? "GetObject" : "GetObjectVersion";
        final String targetMethod = "PutObject";

        final String userName = request.getUserName() == null ? "" : request.getUserName();
        String stamp = StringUtils.isNoneBlank(request.getHeader("stamp")) ? request.getHeader("stamp") : String.valueOf(System.currentTimeMillis());
        int nanoSeconds = (int) (System.nanoTime() % ONE_SECOND_NANO);
        String lastModify = MsDateUtils.stampToGMT(Long.parseLong(stamp));
        final Long[] currentWormStamp = new Long[1]; // 当前worm时钟时间
        String wormStamp = request.getHeader(WORM_STAMP);

        final SocketReqMsg msg = buildRefactorCopyMessage(request, sourceBucket, sourceObject);
        JsonObject aclJson = new JsonObject();
        final Map<String, String> sysMetaMap = new HashMap<>();

        String[] sysHeader = {CONTENT_DISPOSITION, CONTENT_LANGUAGE, CACHE_CONTROL, CONTENT_TYPE, CONTENT_ENCODING, EXPIRES};

        MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();

        Disposable[] disposables = new Disposable[3];
        String[] accountQuota = new String[]{null};
        String[] objNumQuota = new String[]{null};

        final boolean[] isInnerObject = {request.headers().contains(CLUSTER_ALIVE_HEADER)};

        final String[] crypto = {request.getHeader(X_AMZ_SERVER_SIDE_ENCRYPTION)};
        final String originIndexCrypto = StringUtils.isBlank(request.getHeader(ORIGIN_INDEX_CRYPTO)) ? null : request.getHeader(ORIGIN_INDEX_CRYPTO);
        AtomicBoolean needCopyWrite = new AtomicBoolean(false);
        AtomicBoolean updateFileMeta = new AtomicBoolean(true);
        final byte[] xmlHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>".getBytes();
        final AtomicInteger times = new AtomicInteger(0);
        long[] timerId = {-1};
        AtomicBoolean isRunningUpdateEC = new AtomicBoolean(false);
        AtomicBoolean useNewDedupFile = new AtomicBoolean(false);
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        boolean[] sourceBucketOpenFS = {false};
        boolean[] targetBucketOpenNFS = new boolean[1];
        int[] sourceStartFs = {0};
        int[] targetStartFs = {0};
        Map<String, Integer> chunkNumMap = new HashMap<>();
        Map<String, ChunkFile> chunkFileMap = new HashMap<>();
        List<String> updateQuotaDir = new LinkedList<>();
        boolean[] inodeOverWrite = new boolean[]{false};
        boolean[] diffStrategy = {false};
        Inode[] sourceDirInodes = {null};
        Inode[] targetDirInodes = {null};
        disposables[0] = currentWormTimeMillis()
                .doOnNext(x -> CryptoUtils.containsCryptoAlgorithm(crypto[0]))
                .doOnNext(l -> currentWormStamp[0] = wormStamp == null ? l : Long.valueOf(wormStamp))
                .flatMap(l -> pool.getReactive(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_NAME))
                .flatMap(username -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(username))
                .doOnNext(accountInfo -> {
                    accountQuota[0] = accountInfo.get(USER_DATABASE_ACCOUNT_QUOTA_FLAG);
                    objNumQuota[0] = accountInfo.get(USER_DATABASE_ACCOUNT_OBJNUM_FLAG);
                })
                .flatMap(b -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(sourceBucket))
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "no such bucket. bucket name :" + sourceBucket + ".")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .doOnNext(bucketInfo -> msg.put("bucketUserId", bucketInfo.get(BUCKET_USER_ID)))
                .flatMap(bucketInfo -> {
                    CheckUtils.hasStartFS(bucketInfo, sourceBucketOpenFS);
                    sourceStartFs[0] = ACLUtils.setFsProtoType(bucketInfo);
                    return ReactorPolicyCheckUtils.getPolicyCheckResult(request, sourceBucket, sourceObject, sourceMethod).zipWith(Mono.just(bucketInfo));
                })
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .flatMap(obj -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(targetBucket).zipWith(Mono.just(obj.getT2())))
                .doOnNext(tuple2 -> copySyncCheck(tuple2.getT2().get(DATA_SYNC_SWITCH), tuple2.getT1().get(DATA_SYNC_SWITCH)))
                .map(Tuple2::getT1)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "no such bucket. bucket name :" + targetBucket + ".")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .doOnNext(bucketInfo -> {
                    if (!request.headers().contains(IS_SYNCING)) {
                        quotaCheck(bucketInfo, accountQuota[0], objNumQuota[0]);
                    }
                })
                .doOnNext(bucketInfo -> setObjectRetention(bucketInfo, sysMetaMap, currentWormStamp[0], wormExpire, wormMode))
                .doOnNext(bucketInfo -> crypto[0] = StringUtils.isBlank(crypto[0]) ? request.headers().contains(ORIGIN_INDEX_CRYPTO) ? originIndexCrypto : bucketInfo.get("crypto") : crypto[0])
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCopyCheckResult(request, sourceBucket, sourceObject, targetBucket, targetObject, targetMethod).zipWith(Mono.just(bucketInfo)))
                .flatMap(t2 -> {
                    Map<String, String> bucketInfo = t2.getT2();
                    if (bucketInfo.containsKey("fsid")) {
                        targetBucketOpenNFS[0] = true;
                        targetStartFs[0] = ACLUtils.setFsProtoType(bucketInfo);
                    }
                    if (!targetBucketOpenNFS[0]) {
                        return Mono.just(t2);
                    }
                    return s3AddQuotaDirInfo(targetBucket, targetObject, stamp, updateQuotaDir, userId)
                            .map(b -> t2);
                })
                .flatMap(tuple2 -> {
                    Map<String, String> bucketInfo = tuple2.getT2();
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    msg.put("mda", bucketInfo.getOrDefault("mda", "off"));
                    if (0 == tuple2.getT1()) {
                        if (targetBucketOpenNFS[0]) {
                            return S3ACL.checkFSACL(targetObject, targetBucket, userId, targetDirInodes, 7, false, targetStartFs[0], ACLUtils.setDirOrFile(false))
                                    .flatMap(b -> MsAclUtils.checkWriteAclReactive(bucketInfo, userId, targetBucket, msg));
                        } else {
                            return MsAclUtils.checkWriteAclReactive(bucketInfo, userId, targetBucket, msg);
                        }
                    }
                    return Mono.empty();
                })
                .switchIfEmpty(Mono.just(bucketPool.getBucketVnodeId(sourceBucket, sourceObject)))
                .zipWith(BucketSyncSwitchCache.isSyncSwitchOffMono(targetBucket))
                .map(tuple2 -> {
                    String versionNum = VersionUtil.getVersionNum();
                    request.addMember(REQUEST_VERSIONNUM, versionNum);
                    requestVersionNumMap.computeIfAbsent(targetBucket, k -> new ConcurrentSkipListMap<>()).put(versionNum, request);
                    return tuple2.getT1();
                })
                .flatMap(vnode -> bucketPool.mapToNodeInfo(String.valueOf(vnode)))
                .flatMap(list -> getObjectMetaVersionUnlimited(sourceBucket, sourceObject, sourceVersionId, list, request, currentSnapshotMark[0], snapshotLink[0]))
                .flatMap(metaData -> {
                    checkMetaData(sourceObject, metaData, sourceVersionId, currentSnapshotMark[0]);
                    if (policy[0] == 0) {
                        if (sourceBucketOpenFS[0]) {
                            checkFsObjectReadAcl(msg, metaData);
                            S3ACL.judgeMetaFSACL(userId, metaData, 6, sourceStartFs[0]);
                            return S3ACL.checkFSACL(sourceObject, sourceBucket, userId, sourceDirInodes, 6, false, sourceStartFs[0], 0)
                                    .map(b -> metaData);
                        } else {
                            checkObjectReadAcl(msg, metaData);
                        }
                    }
                    return Mono.just(metaData);
                })
                .doOnNext(metadata -> {
                    JsonObject sysMeta = new JsonObject(metadata.sysMetaData);

                    // 包含此key-value标记的对象为系统内部生成的对象，不进行多站点间的同步
                    isInnerObject[0] = isInnerObject[0]
                            && sysMeta.containsKey(NO_SYNCHRONIZATION_KEY)
                            && NO_SYNCHRONIZATION_VALUE.equals(sysMeta.getString(NO_SYNCHRONIZATION_KEY));

                    // 对象的worm属性不复制
                    sysMeta.fieldNames()
                            .stream()
                            .filter(key -> !EXPIRATION.equals(key))
                            .forEach(key -> sysMetaMap.put(key, String.valueOf(sysMeta.getValue(key))));
                    checkIfMatch(sysMetaMap, heads, false);
                    createObjAcl(request, aclJson);
                })
                .flatMap(sourceMeta -> {
                    StoragePool sourcePool = StoragePoolFactory.getNoUsedStoragePool(sourceMeta.storage);
                    if (!StoragePoolFactory.inStorageMAP(sourceMeta.storage) && null != sourcePool && StoragePoolFactory.inStorageMAP(sourcePool.getUpdateECPrefix())) {//如果此时源对象需要进行修改ec
                        // 处理，但还未更新元数据，那么copy的对象也需要存储在调整ec后的存储池中
                        isRunningUpdateEC.set(true);
                    }
                    if (StringUtils.isNotEmpty(sourceMeta.duplicateKey)) {
                        String metaDeduplicateKey = sourceMeta.duplicateKey.split(ROCKS_DEDUPLICATE_KEY)[0] + "#" + requestId;
                        String md5 = sourceMeta.duplicateKey.split(File.separator)[1];
                        StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);
                        String dedupVnode = dedupPool.getBucketVnodeId(md5);
                        if (isRunningUpdateEC.get()) {
                            return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(md5))
                                    .flatMap(nodeList -> getDeduplicateMeta(md5, sourcePool.getUpdateECPrefix(), nodeList, request))
                                    .flatMap(dedupMeta -> {
                                        if (dedupMeta.equals(DedupMeta.ERROR_DEDUP_META)) {
                                            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get Object Deduplicate Meta Data fail");
                                        }
                                        if (dedupMeta.equals(DedupMeta.NOT_FOUND_DEDUP_META)) {//表示此MD5还未生成新ec数据池的重删记录
                                            return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(md5))
                                                    .flatMap(nodeList -> getDeduplicateMeta(md5, sourceMeta.storage, nodeList, request))
                                                    .flatMap(dedupMeta0 -> {
                                                        if (dedupMeta0.equals(DedupMeta.ERROR_DEDUP_META)) {
                                                            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get Object Deduplicate Meta Data fail");
                                                        }

                                                        if (StringUtils.isNotEmpty(dedupMeta0.fileName)) {
                                                            sourceMeta.setFileName(dedupMeta0.fileName);
                                                        }
                                                        return Mono.just(sourceMeta);
                                                    });
                                        } else {
                                            if (StringUtils.isNotEmpty(dedupMeta.fileName)) {//这里获取新数据池中该md5对应的数据块进行copy
                                                sourceMeta.setFileName(dedupMeta.fileName);
                                                sourceMeta.setStorage(sourcePool.getUpdateECPrefix());
                                                useNewDedupFile.set(true);
                                            }
                                            return Mono.just(sourceMeta);
                                        }

                                    });
                        }
                        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(md5))
                                .flatMap(nodeList -> getDeduplicateMeta(md5, sourceMeta.storage, nodeList, request))
                                .flatMap(dedupMeta -> {

                                    if (dedupMeta.equals(DedupMeta.ERROR_DEDUP_META)) {
                                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get Object Deduplicate Meta Data fail");
                                    }

                                    if (StringUtils.isNotEmpty(dedupMeta.fileName)) {
                                        sourceMeta.setFileName(dedupMeta.fileName);
                                    }
                                    return Mono.just(sourceMeta);
                                });

                    }
                    return Mono.just(sourceMeta);
                })
                .zipWith(BucketSyncSwitchCache.isSyncSwitchOffMono(targetBucket))
                .subscribe(tuple2S -> {
                    MetaData sourceMeta = tuple2S.getT1();
                    Boolean isSyncSwitchOff = tuple2S.getT2();
                    boolean sourceIsAggregatedObj = StringUtils.isNotEmpty(sourceMeta.aggregationKey);
                    //加密算法不一致、正在扩副本、不在一个存储策略里、多站点环境，进行真copy
                    needCopyWrite.set(!StringUtils.equals(sourceMeta.getCrypto(), crypto[0]) || isRunningUpdateEC.get()
                            || sourceMeta.inode > 0 || VersionUtil.isMultiCluster || sourceIsAggregatedObj);

                    StoragePool sourcePool = StoragePoolFactory.getStoragePool(sourceMeta);
                    StoragePool targetPool;
                    if (needCopyWrite.get()) {
                        StorageOperate operate = new StorageOperate(DATA, targetObject, sourceMeta.endIndex + 1);
                        targetPool = StoragePoolFactory.getStoragePool(operate, targetBucket);
                    } else {
                        targetPool = sourcePool;
                    }

                    String[] targetObjSuffix = new String[]{ROCKS_FILE_META_PREFIX + targetPool.getObjectVnodeId(targetBucket, targetObject).var2};
                    MetaData metaData = createMeta(targetPool, targetBucket, targetObject, requestId).setUserMetaData(userMeta);
                    metaData.setCrypto(crypto[0]);
                    metaData.setSyncStamp(StringUtils.isBlank(request.getHeader(SYNC_STAMP)) ? VersionUtil.getVersionNum(isSyncSwitchOff) : request.getHeader(SYNC_STAMP));
                    metaData.setShardingStamp(VersionUtil.getVersionNum());
                    disposables[2] = MsObjVersionUtils.versionStatusReactive(targetBucket)
                            .flatMap(status -> MsObjVersionUtils.checkObjVersionsLimit(targetBucket, targetObject, request))
                            .flatMap(b -> MsObjVersionUtils.getObjVersionIdReactive(targetBucket))
                            .zipWith(VersionUtil.getVersionNum(targetBucket, targetObject))
                            .doOnNext(tuple2 -> {
                                String versionId = tuple2.getT1();
                                String versionNum = tuple2.getT2();
                                versionId = request.headers().contains(NEW_VERSION_ID) ?
                                        request.headers().get(NEW_VERSION_ID) : versionId;
                                targetObjSuffix[0] = targetObjSuffix[0] + versionId + requestId;
                                sysMetaMap.put("owner", userId);
                                sysMetaMap.put(LAST_MODIFY, lastModify);
                                if ("REPLACE".equals(request.getHeader(X_AMZ_METADATA_DIRECTIVE))) {
                                    sysMetaMap.keySet().removeIf(key -> SPECIAL_HEADER.contains(key.toLowerCase().hashCode()));
                                }
                                setSysMeta(sysHeader, sysMetaMap, request);
                                metaData.setVersionId(versionId).setVersionNum(versionNum).setStamp(stamp)
                                        .setFileName(sourceMeta.fileName).setEndIndex(sourceMeta.endIndex)
                                        .setObjectAcl(aclJson.toString()).setReferencedBucket(sourceMeta.referencedBucket)
                                        .setReferencedKey(sourceMeta.referencedKey)
                                        .setReferencedVersionId(sourceMeta.referencedVersionId);
                                if (sourceMeta.key.equals(targetObject) && sourceMeta.bucket.equals(targetBucket) && sourceMeta.versionId.equals(versionId)) {
                                    updateFileMeta.set(false);
                                    //修改对象元数据时，inode也需要复制
                                    if (sourceMeta.inode > 0) {
                                        metaData.inode = sourceMeta.inode;
                                        metaData.cookie = sourceMeta.cookie;
                                    }

                                    if (StringUtils.isNotEmpty(sourceMeta.aggregationKey)) {
                                        needCopyWrite.set(false);
                                        metaData.aggregationKey = sourceMeta.aggregationKey;
                                        metaData.storage = sourceMeta.storage;
                                        metaData.fileName = sourceMeta.fileName;
                                        metaData.offset = sourceMeta.offset;
                                    }
                                }
                                metaData.setSnapshotMark(updateFileMeta.get() ? currentSnapshotMark[0] : sourceMeta.snapshotMark);
                                String stampKey = Utils.getMetaDataKey(targetBucketVnode, targetBucket, targetObject, versionId, stamp, metaData.snapshotMark);
                                String fileMetaKey = Utils.getVersionMetaDataKey(targetBucketVnode, targetBucket, targetObject, versionId, metaData.snapshotMark);
                                msg.put("metaKey", stampKey);
                                msg.put("fileMetaKey", fileMetaKey);
                                if (sourceMeta.getPartInfos() != null) {
                                    String uploadId = StringUtils.isNotBlank(request.getHeader(SYNC_COPY_PART_UPLOADID)) ?
                                            request.getHeader(SYNC_COPY_PART_UPLOADID) :
                                            RandomStringUtils.randomAlphabetic(32);
                                    PartInfo[] metaPartInfos = sourceMeta.getPartInfos();
                                    PartInfo[] partInfos = new PartInfo[metaPartInfos.length];
                                    int i = 0;
                                    for (PartInfo info : metaPartInfos) {
                                        PartInfo info0 = info.clone();
                                        info0.setBucket(targetBucket);
                                        info0.setObject(targetObject);
                                        info0.setUploadId(uploadId);
                                        info0.setSnapshotMark(updateFileMeta.get() ? currentSnapshotMark[0] : info0.snapshotMark);
                                        if (StringUtils.isNotEmpty(info0.deduplicateKey) && !(sourceMeta.key.equals(targetObject) && sourceMeta.bucket.equals(targetBucket) && sourceMeta.versionId.equals(versionId))) {
                                            info0.setDeduplicateKey(null);
                                            String md5 = info0.etag;
                                            StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);

                                            Disposable reSubscribe = bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(md5))
                                                    .flatMap(nodeList -> getDeduplicateMeta(md5, info0.storage, nodeList, request))
                                                    .subscribe(dedupMeta -> {

                                                        if (dedupMeta.equals(DedupMeta.ERROR_DEDUP_META)) {
                                                            throw new MsException(UNKNOWN_ERROR, "Get Object Deduplicate Meta Data fail");
                                                        }

                                                        if (StringUtils.isNotEmpty(dedupMeta.fileName)) {
                                                            info0.setFileName(dedupMeta.fileName);
                                                        }
                                                    }, e -> logger.error("reset fileName error!", e));

                                            Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> reSubscribe.dispose()));
                                        }

                                        info0.setStorage(targetPool.getVnodePrefix());
                                        partInfos[i++] = info0;
                                    }
                                    metaData.setPartUploadId(uploadId);
                                    metaData.setPartInfos(partInfos);
                                }

                                if (!"REPLACE".equals(request.getHeader(X_AMZ_METADATA_DIRECTIVE))) {
                                    metaData.setUserMetaData(sourceMeta.userMetaData);
                                }
                                metaData.setSysMetaData(JsonUtils.toString(sysMetaMap, HashMap.class));
                                if (sourceMeta.referencedKey.equals(targetObject) && sourceMeta.referencedBucket.equals(targetBucket)
                                        && sourceMeta.referencedVersionId.equals(versionId)) {
                                    targetObjSuffix[0] = "#" + requestId;
                                }
                            }).flatMap(b -> {
                                //copy的对象需重写一份数据
                                if (needCopyWrite.get()) {
                                    timerId[0] = vertx.setPeriodic(9_000, (idP) -> {
                                        if (times.incrementAndGet() == 1) {
                                            CryptoUtils.addCryptoResponse(request, crypto[0]);
                                            addPublicHeaders(request, requestId).putHeader(CONTENT_TYPE, "application/xml")
                                                    .putHeader(TRANSFER_ENCODING, "chunked")
                                                    .write(Buffer.buffer(xmlHeader));
                                        }
                                        request.response().write("\n");
                                    });

                                    metaData.setReferencedBucket(targetBucket);
                                    metaData.setReferencedKey(targetObject);
                                    metaData.setReferencedVersionId(metaData.getVersionId());
                                    return ErasureClient.copyCryptoData(sourcePool, targetPool, sourceMeta, metaData, request, recoverDataProcessor, requestId, msg.get("fileMetaKey"));
                                }
                                if (updateFileMeta.get()) {
                                    return updateFileMeta(metaData, targetObjSuffix[0], recoverDataProcessor, request, null);
                                }
                                return Mono.just(true);
                            })
                            .doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(UNKNOWN_ERROR, "copy object internal error");
                                }
                            })
                            .flatMap(b -> bucketPool.mapToNodeInfo(targetBucketVnode))
                            .flatMap(bucketList -> {
                                        boolean isReplaceBucket = !sourceBucket.equals(targetBucket);
                                        if (metaData.getPartInfos() != null) {
                                            if (!needCopyWrite.get() && updateFileMeta.get()) {
                                                PartInfo[] metaPartInfos = metaData.getPartInfos();
                                                for (PartInfo info : metaPartInfos) {
                                                    String finalObjSuffix = targetObjSuffix[0];
                                                    if (isReplaceBucket && info.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                                                        info.fileName = info.fileName.replaceFirst(sourceBucket, targetBucket);
                                                    }
                                                    info.setFileName(info.fileName.split(ROCKS_FILE_META_PREFIX)[0] + finalObjSuffix);
                                                }
                                                metaData.setPartInfos(metaPartInfos);
                                            }
                                        } else {
                                            if (!needCopyWrite.get() && updateFileMeta.get()) {
                                                if (isReplaceBucket && metaData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                                                    metaData.fileName = metaData.fileName.replaceFirst(sourceBucket, targetBucket);
                                                }
                                                metaData.setFileName(metaData.fileName.split(ROCKS_FILE_META_PREFIX)[0] + targetObjSuffix[0]);
                                            }
                                        }
                                        EsMeta esMeta = new EsMeta()
                                                .setUserId(userId)
                                                .setSysMetaData(JsonUtils.toString(sysMetaMap, HashMap.class))
                                                .setUserMetaData(metaData.userMetaData)
                                                .setBucketName(targetBucket)
                                                .setObjName(targetObject)
                                                .setVersionId(metaData.versionId)
                                                .setStamp(stamp)
                                                .setObjSize(String.valueOf(metaData.endIndex + 1))
                                                .setInode(metaData.inode);
                                        return Mono.just(targetBucketMigrateVnode != null)
                                                .flatMap(b -> {
                                                    //控制台重命名与控制台覆盖文件不可区分，当前全部视为触发后为新对象，拥有者为请求者，
                                                    //解除硬链接
                                                    if (targetBucketOpenNFS[0]
                                                            && updateFileMeta.get()
                                                            && metaData.inode == 0 &&
                                                            !updateQuotaDir.isEmpty()) {
                                                        Inode inode = Inode.defaultInode(metaData);
                                                        inode.getXAttrMap().put(QUOTA_KEY, Json.encode(updateQuotaDir));
                                                        inode.getInodeData().clear();
                                                        metaData.tmpInodeStr = Json.encode(inode);
                                                    }
                                                    if (b) {
                                                        if (targetBucketMigrateVnode.equals(targetBucketVnode)) {
                                                            return Mono.just(true);
                                                        }
                                                        return bucketPool.mapToNodeInfo(targetBucketMigrateVnode)
                                                                .flatMap(nodeList -> putMetaData(Utils.getMetaDataKey(targetBucketMigrateVnode, targetBucket,
                                                                        targetObject, metaData.versionId, metaData.stamp, metaData.snapshotMark), metaData, nodeList, recoverDataProcessor, request,
                                                                        msg.get("mda"), esMeta, updateFileMeta.get() ? snapshotLink[0] : null));
                                                    }
                                                    return Mono.just(true);
                                                })
                                                .doOnNext(b -> {
                                                    if (!b) {
                                                        throw new MsException(UNKNOWN_ERROR, "put object meta to new mapping error!");
                                                    }
                                                }).flatMap(r -> putMetaData(msg.get("metaKey"), metaData, bucketList, recoverDataProcessor, request, msg.get("mda"), esMeta, updateFileMeta.get() ?
                                                        snapshotLink[0] : null)
                                                        .delayUntil(res ->
                                                                FsUtils.lookup(metaData.bucket, metaData.key, null, false, -1, null)
                                                                        .flatMap(inode -> {
                                                                            if (InodeUtils.isError(inode) || inode.getNodeId() == 0) {
                                                                                return Mono.empty();
                                                                            }

                                                                            return Node.getInstance().updateInodeTime(
                                                                                    metaData.inode,
                                                                                    metaData.bucket,
                                                                                    Long.parseLong(stamp) / 1000L,
                                                                                    nanoSeconds,
                                                                                    true, true, true
                                                                            );
                                                                        })
                                                        )
                                                        .flatMap(res -> {
                                                            Mono<Boolean> updateSource = Mono.empty();
                                                            if (sourceMeta.inode > 0) {
                                                                updateSource = InodeUtils.updateSpeciDirTime(sourceDirInodes[0], sourceObject, sourceBucket)
                                                                        .map(i -> res);
                                                            }

                                                            Mono<Boolean> updateTarget = Mono.empty();
                                                            if (targetBucketOpenNFS[0]) {
                                                                updateTarget = InodeUtils.updateSpeciDirTime(targetDirInodes[0], targetObject, targetBucket)
                                                                        .map(i -> res);
                                                            }

                                                            return Flux.merge(updateSource, updateTarget)
                                                                    .count()
                                                                    .map(count -> res);
                                                        })
                                                        .doOnNext(b -> {
                                                            if (!b) {
                                                                throw new MsException(UNKNOWN_ERROR, "put meta data error!");
                                                            }
                                                            Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(true));
                                                        }))
                                                .flatMap(b -> {
                                                    if (ES_ON.equals(msg.get("mda"))) {
                                                        MonoProcessor<Boolean> esRes = MonoProcessor.create();
                                                        Mono<Boolean> putEsMono = EsMetaTask.putEsMeta(esMeta, false);
                                                        if (metaData.inode > 0) {
                                                            esMeta.inode = metaData.inode;
                                                            String size = sysMetaMap.get(CONTENT_LENGTH);
                                                            if (size != null && !Objects.equals(esMeta.objSize, size)) {
                                                                esMeta.objSize = size;
                                                            }
                                                            putEsMono = EsMetaTask.overWriteEsMeta(esMeta, ERROR_INODE, false);
                                                        }
                                                        Mono<Boolean> finalPutEsMono = putEsMono;
                                                        disposables[1] = Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> finalPutEsMono).subscribe(esRes::onNext);
                                                        return esRes;
                                                    } else {
                                                        return Mono.just(b);
                                                    }
                                                });
                                    }
                            )
                            .doFinally(f -> {
                                if (needCopyWrite.get()) {
                                    vertx.cancelTimer(timerId[0]);
                                }
                            })
                            .subscribe(b -> {
                                try {
                                    QuotaRecorder.addCheckBucket(targetBucket);
                                    CopyObjectResult objectResult = new CopyObjectResult();
                                    objectResult.setEtag('"' + sysMetaMap.get(ETAG) + '"');
                                    objectResult.setLastModified(MsDateUtils.dateToISO8601(lastModify));
                                    byte[] res = JaxbUtils.toByteArray(objectResult);
                                    if (needCopyWrite.get()) {
                                        vertx.cancelTimer(timerId[0]);
                                        if (times.get() > 0) {
                                            request.response().end(Buffer.buffer(res).slice(xmlHeader.length, res.length));
                                            return;
                                        }
                                    }
                                    HttpServerResponse httpServerResponse = addPublicHeaders(request, requestId)
                                            .putHeader(CONTENT_TYPE, "application/xml")
                                            .putHeader(CONTENT_LENGTH, String.valueOf(res.length))
                                            .putHeader(X_AMX_VERSION_ID, metaData.versionId);
                                    if (isInnerObject[0]) {
                                        httpServerResponse.putHeader(IFF_TAG, "1");
                                    }
                                    CryptoUtils.addCryptoResponse(request, crypto[0]);
                                    httpServerResponse
                                            .write(Buffer.buffer(res));
                                    addAllowHeader(request.response()).end();
                                    request.addMember("etag", sysMetaMap.get(ETAG));
                                } catch (Exception e) {
                                    logger.error(e);
                                }
                            }, e -> dealException(request, e));
                }, e -> dealException(request, e));
        request.addResponseCloseHandler(v -> {
            for (Disposable d : disposables) {
                if (null != d) {
                    d.dispose();
                }
            }
        });
        return ErrorNo.SUCCESS_STATUS;
    }

    private void setSysMeta(String[] sysHeader, Map<String, String> sysMetaMap, MsHttpRequest request) {
        for (String s : sysHeader) {
            if (request.getHeader(s) != null && "REPLACE".equals(request.getHeader(X_AMZ_METADATA_DIRECTIVE))) {
                sysMetaMap.put(s, request.getHeader(s));
            }
        }
        if (StringUtils.isEmpty(request.headers().get(CONTENT_TYPE))
                && StringUtils.isEmpty(sysMetaMap.get(CONTENT_TYPE))) {
            sysMetaMap.put(CONTENT_TYPE, "application/octet-stream");
        }
    }

    private void checkMetaDirective(String sourceBucket, String sourceObject, MsHttpRequest request) {
        String directive = request.getHeader(X_AMZ_METADATA_DIRECTIVE);
        directive = StringUtils.isEmpty(directive) ? "COPY" : directive;
        String targetBucket = request.getBucketName();
        String targetObject = request.getObjectName();

        if (!"COPY".equals(directive) && !"REPLACE".equals(directive)) {
            logger.info("directive  is " + directive);
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "the x-amz-metadata-directive input is invalid.");
        }

        if (sourceBucket.equals(targetBucket) && sourceObject.equals(targetObject) && "COPY".equals(directive)) {
            logger.info("directive  is " + directive);
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "the x-amz-metadata-directive input is invalid.");
        }
    }

    /**
     * 处理上传对象的请求
     *
     * @param request 请求对象
     * @return 会向request中写入处理结果
     */
    public int putObject(MsHttpRequest request) {
        if (request.headers().contains("x-amz-copy-source")) {
            return copyObject(request);
        }
        MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();
        try {
            if (request.getHeader(TRANSFER_ENCODING) != null && request.getHeader(TRANSFER_ENCODING).contains("chunked")) {//分块传输
                if ((request.headers().contains(CONTENT_LENGTH) && !request.getHeader(CONTENT_LENGTH).equals("0")) || (request.headers().contains(X_AMZ_DECODED_CONTENT_LENGTH) && !request.getHeader(X_AMZ_DECODED_CONTENT_LENGTH).equals("0"))) {//不传CONTENT_LENGTH和X_AMZ_DECODED_CONTENT_LENGTH
                    throw new MsException(CONTENT_LENGTH_NOT_ALLOWED, "content-length is not allowed when using Chunked transfer encoding");
                }
            } else {
                if (!request.headers().contains(CONTENT_LENGTH)) {
                    throw new MsException(ErrorNo.MISSING_CONTENT_LENGTH, "put object error, no content-length param");
                } else {
                    if (Long.parseLong(request.getHeader("content-length")) > MAX_PUT_SIZE) {
                        if (!request.headers().contains(X_AMZ_STORE_MANAGEMENT_SERVER_HEADER) || StringUtils.isEmpty(request.getHeader(X_AMZ_STORE_MANAGEMENT_SERVER_HEADER))) {
                            throw new MsException(OBJECT_SIZE_OVERFLOW, "object size too large");
                        } else {
                            if (!validateRequest(request.getHeader(X_AMZ_STORE_MANAGEMENT_SERVER_HEADER))) {
                                throw new MsException(OBJECT_SIZE_OVERFLOW, "object size too large");
                            }
                        }
                    }
                }
            }
        } catch (NumberFormatException e) {
            throw new MsException(INVALID_ARGUMENT, "content length is not number");
        }
        if (HttpMethod.POST == request.method() && request.headers().contains(CONTENT_TYPE) && request.getHeader(CONTENT_TYPE).contains("multipart/form-data")) {
            if (request.getHeader(TRANSFER_ENCODING) != null && request.getHeader(TRANSFER_ENCODING).contains("chunked")) {
                //分块传输不检测File-Length
            } else {
                if (!request.headers().contains("File-Length")) {
                    throw new MsException(MISSING_FILE_LENGTH, "miss File-Length header");
                }
                try {
                    long l = Long.parseLong(request.getHeader("File-Length"));
                } catch (NumberFormatException e) {
                    throw new MsException(INVALID_ARGUMENT, "file length is not number");
                }
            }
        }


        final int[] policy = new int[1];
        final boolean[] dedup = new boolean[1];
        request.setExpectMultipart(true);
        final String bucketName = request.getBucketName();
        final String objName = request.getObjectName();
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        //final String bucketVnode = bucketPool.getBucketVnodeId(bucketName, objName);
        final String[] bucketVnode = new String[]{null};
        // 判断当前节点是否正在进行扩容，是否开启双写
        final String[] migrateVnode = new String[]{null};

        final String requestId = getRequestId();
        final String user = request.headers().contains(COMPONENT_USER_ID) ? request.headers().get(COMPONENT_USER_ID) : request.getUserId();
        final String backupUserId = request.getHeader("backup_userId");
        checkObjectName(objName);
        final JsonObject userMeta = getUserMeta(request);

        final String method = "PutObject";

        final String wormExpire = request.getHeader(X_AMZ_WORM_DATE_HEADER);
        final String wormMode = request.getHeader(X_AMZ_WORM_MODE_HEADER);
        final String syncWormExpire = request.getHeader(SYNC_WORM_EXPIRE);

        final String[] crypto = {request.getHeader(X_AMZ_SERVER_SIDE_ENCRYPTION)};
        boolean flag = false;
        final Long[] objectSize = {Long.MAX_VALUE};
        if (request.getHeader(TRANSFER_ENCODING) != null && request.getHeader(TRANSFER_ENCODING).contains("chunked")) {
            flag = true;
        } else {//不使用分块
            objectSize[0] = Long.parseLong(request.getHeader(CONTENT_LENGTH));
            if (HttpMethod.POST == request.method() && request.headers().contains(CONTENT_TYPE) && request.getHeader(CONTENT_TYPE).contains("multipart/form-data")) {
                objectSize[0] = Long.parseLong(request.getHeader("File-Length"));
            }
        }
        final boolean isChunked = flag;
        StorageOperate dataOperate = new StorageOperate(DATA, objName, objectSize[0]);
        StoragePool dataPool = StoragePoolFactory.getStoragePool(dataOperate, bucketName);
        MetaData metaData = createMeta(dataPool, bucketName, objName, requestId)
                .setUserMetaData(userMeta.encode())
                .setEndIndex(objectSize[0] - 1);
        String stamp = StringUtils.isBlank(request.getHeader("stamp")) ? String.valueOf(System.currentTimeMillis()) : request.getHeader("stamp");
        String lastModify = MsDateUtils.stampToGMT(Long.parseLong(stamp));
        metaData.setStamp(stamp);
        metaData.setSmallFile(objectSize[0] < MAX_SMALL_SIZE);
        // 无论是否开启NFS，均对元数据生成唯一cookie，防止NFS多客户端list无cookie对象时生成不一样的cookie
//        metaData.setCookie(VersionUtil.newInode());

        final JsonObject sysMetaMap = getSysMetaMap(request);
        sysMetaMap.put("owner", user);

        String contentMD5 = request.getHeader(CONTENT_MD5);
        String wormStamp = request.getHeader(WORM_STAMP);
        String originIndexCrypto = StringUtils.isBlank(request.getHeader(ORIGIN_INDEX_CRYPTO)) ? null : request.getHeader(ORIGIN_INDEX_CRYPTO);
        SocketReqMsg msg = buildUploadMsg(request, bucketName, objName, requestId);

        sysMetaMap.put(LAST_MODIFY, lastModify);
        if (StringUtils.isNotEmpty(backupUserId)) {
            msg.put("backup_userId", backupUserId);
            msg.put("backup_time", request.getHeader("backup_time"));
            sysMetaMap.put("owner", backupUserId);
        }

        String[] accountQuota = new String[]{null};
        String[] objNumQuota = new String[]{null};
        long[] currentWormStamp = new long[1];
        JsonObject aclJson = new JsonObject();
        boolean[] hasStartFS = new boolean[1];
        int[] startFsType = {0};
        final String[] snapshotLink = new String[1];
        List<String> updateQuotaDir = new LinkedList<>();
        boolean[] inodeOverWrite = new boolean[]{false};
        Inode[] dirInodes = {null};
        Disposable disposable = createObjAcl(request, aclJson)
                .doOnNext(x -> CryptoUtils.containsCryptoAlgorithm(crypto[0]))
//                .doOnNext(aBoolean -> AuthorizeV4.checkTransferEncoding(request))
                .flatMap(b -> wormStamp != null ? Mono.just(Long.valueOf(wormStamp)) : currentWormTimeMillis())
                .doOnNext(wormTime -> currentWormStamp[0] = wormTime)
                .flatMap(b -> pool.getReactive(REDIS_USERINFO_INDEX).hget(user, USER_DATABASE_ID_NAME))
                .flatMap(username -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(username))
                .flatMap(b -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo,
                        new MsException(NO_SUCH_BUCKET, "No such bucket")))
                .doOnNext(bucketInfo -> metaData.setCrypto((crypto[0] = StringUtils.isBlank(crypto[0]) ? request.headers().contains(ORIGIN_INDEX_CRYPTO) ? originIndexCrypto : bucketInfo.get("crypto"
                ) : crypto[0])))
                .doOnNext(bucketInfo -> {
                    if (StringUtils.isEmpty(syncWormExpire) && !request.headers().contains(IS_SYNCING)) {
                        setObjectRetention(bucketInfo, sysMetaMap, currentWormStamp[0], wormExpire, wormMode);
                    } else if (StringUtils.isNotEmpty(syncWormExpire)) {
                        sysMetaMap.put(EXPIRATION, syncWormExpire);
                    }

                    if (request.getQuickReturn() == 2
                            && "1".equals(bucketInfo.getOrDefault("quickReturn", "0"))) {
                        request.setQuickReturn(1);
                    }
                })
                .flatMap(bucketInfo -> {
                    CheckUtils.hasStartFS(bucketInfo, hasStartFS);
                    startFsType[0] = ACLUtils.setFsProtoType(bucketInfo);
                    return Mono.just(bucketInfo);
                })
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyPutCheckResult(request, bucketName, objName, method, aclJson
                        , crypto[0]).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .flatMap(tuple2 -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(tuple2.getT2().get("user_name")).zipWith(Mono.just(tuple2.getT2())))
                .doOnNext(tuple2 -> {
                    accountQuota[0] = tuple2.getT1().get(USER_DATABASE_ACCOUNT_QUOTA_FLAG);
                    objNumQuota[0] = tuple2.getT1().get(USER_DATABASE_ACCOUNT_OBJNUM_FLAG);
                })
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> {
                    if (!hasStartFS[0]) {
                        return Mono.just(bucketInfo);
                    }
                    return s3AddQuotaDirInfo(bucketName, objName, stamp, updateQuotaDir, Optional.ofNullable(backupUserId).orElse(user))
                            .map(x -> bucketInfo);
                })
                .flatMap(bucketInfo -> {
                    // 对象元数据添加快照标记
                    metaData.setSnapshotMark(bucketInfo.get(CURRENT_SNAPSHOT_MARK));
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    request.addMember("address", bucketInfo.getOrDefault("address", ""));
                    regionCheck(bucketInfo.get(REGION_NAME));
                    siteCheck(bucketInfo);
                    metaData.setObjectAcl(aclJson.encode());
                    if (!request.headers().contains(IS_SYNCING)) {
                        quotaCheck(bucketInfo, accountQuota[0], objNumQuota[0]);
                    }
                    msg.put("mda", bucketInfo.getOrDefault("mda", "off"));
                    if ("on".equals(bucketInfo.getOrDefault("mda", "off"))) {
                        msg.put("mda", "on");
                    } else {
                        msg.put("mda", "off");
                    }
                    String versionStatus = bucketInfo.getOrDefault(BUCKET_VERSION_STATUS, "NULL");
                    String versionId = "NULL".equals(versionStatus) || VERSION_SUSPENDED.equals(versionStatus) ?
                            "null" : RandomStringUtils.randomAlphanumeric(32);
                    versionId = StringUtils.isNoneBlank(request.getHeader(VERSIONID)) ? request.getHeader(VERSIONID) : versionId;
                    metaData.setVersionId(versionId);
                    metaData.setReferencedVersionId(versionId);
                    msg.put("status", versionStatus);

                    if (!PASSWORD.equals(request.getHeader(SYNC_AUTH)) && policy[0] == 0) {
                        if (hasStartFS[0]) {
                            boolean isPutDir = StringUtils.isBlank(objName) || (StringUtils.isNotBlank(objName) && objName.endsWith("/"));
                            return S3ACL.checkFSACL(objName, bucketName, Optional.ofNullable(backupUserId).orElse(user), dirInodes, 7, false, startFsType[0], ACLUtils.setDirOrFile(isPutDir))
                                    .flatMap(b -> MsAclUtils.checkWriteAclReactive(bucketInfo, Optional.ofNullable(backupUserId)
                                            .orElse(user), bucketName, msg));
                        } else {
                            return MsAclUtils.checkWriteAclReactive(bucketInfo, Optional.ofNullable(backupUserId)
                                    .orElse(user), bucketName, msg);
                        }
                    } else {
                        return Mono.empty();
                    }
                })
                .switchIfEmpty(pool.getReactive(REDIS_USERINFO_INDEX).hget(Optional.ofNullable(backupUserId)
                        .orElse(user), "name"))
                .doOnNext(userName -> msg.put("name", (String) userName))
                .flatMap(userName -> MsObjVersionUtils.checkObjVersionsLimit(bucketName, objName, request))
                .flatMap(list -> dataPool.mapToNodeInfo(dataPool.getObjectVnodeId(metaData.fileName)))
                .zipWith(BucketSyncSwitchCache.isSyncSwitchOffMono(bucketName))
                .map(tuple2 -> {
                    String versionNum = VersionUtil.getVersionNum();
                    request.addMember(REQUEST_VERSIONNUM, versionNum);
                    requestVersionNumMap.computeIfAbsent(bucketName, k -> new ConcurrentSkipListMap<>()).put(versionNum, request);
                    return tuple2.getT1();
                })
                .flatMap(list -> ErasureClient.putObject(dataPool, list, request, metaData, recoverDataProcessor, isChunked, sysMetaMap))
                .doOnNext(md5 -> {
                    if (StringUtils.isBlank(md5)) {
                        throw new MsException(UNKNOWN_ERROR, "put file fail");
                    }

                    if (contentMD5 != null) {
                        checkMd5(contentMD5, md5);
                    }
                })
                .flatMap(md5 -> StoragePoolFactory.getDeduplicateByBucketName(bucketName).zipWith(Mono.just(md5)))
                .doOnNext(tuple2 -> dedup[0] = tuple2.getT1())
                .flatMap(tuple2 -> {
                    String md5 = tuple2.getT2();
                    sysMetaMap.put(ETAG, md5);

                    if (!dedup[0] || metaData.partInfos != null || metaData.storage.startsWith("cache")) {
                        // 数据块落在缓存池，不进行重删处理，缓存下刷时处理小文件的重删
                        return Mono.just(true);
                    }
                    StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);
                    String dedupVnode = dedupPool.getBucketVnodeId(md5);
                    String duplicate = Utils.getDeduplicatMetaKey(dedupVnode, md5, metaData.storage, requestId);
                    return dedupPool.mapToNodeInfo(dedupVnode)
                            .flatMap(nodeList -> getAndUpdateDeduplicate(dedupPool, duplicate, nodeList, request, recoverDataProcessor, metaData, md5))
                            .flatMap(b -> {
                                if (b) {
                                    metaData.duplicateKey = duplicate;
                                }
                                return Mono.just(b);
                            });
                })
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(UNKNOWN_ERROR, "put deduplicate meta failed");
                    }
                })
                // 设置对象元数据的全局唯一递增ID
                .flatMap(usrName -> VersionUtil.getVersionNum(bucketName, objName))
                .flatMap(versionNum -> {
                    if (StringUtils.isNotEmpty(request.getHeader(CONTENT_LENGTH)) && request.getHeader(CONTENT_LENGTH).contains("change-")) {
                        String realLength = request.getHeader(CONTENT_LENGTH).replace("change-", "");
                        sysMetaMap.put(CONTENT_LENGTH, realLength);
                        metaData.endIndex = Long.parseLong(realLength) - 1;
                    }
                    sysMetaMap.put("displayName", msg.get("name"));
                    metaData.setVersionNum(versionNum);
                    metaData.setSysMetaData(sysMetaMap.encode());
                    metaData.setSyncStamp(StringUtils.isBlank(request.getHeader(SYNC_STAMP)) ? versionNum : request.getHeader(SYNC_STAMP));
                    metaData.setShardingStamp(VersionUtil.getVersionNum());
                    metaData.setReferencedKey(objName);
                    metaData.setReferencedBucket(bucketName);
                    com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucketName, objName);
                    bucketVnode[0] = bucketVnodeIdTuple.var1;
                    // 判断当前节点是否正在进行扩容，是否开启双写
                    migrateVnode[0] = bucketVnodeIdTuple.var2;
                    return bucketPool.mapToNodeInfo(bucketVnode[0]);
                })
                .flatMap(bucketList -> {
                    EsMeta esMeta = new EsMeta()
                            .setUserId(user)
                            .setSysMetaData(sysMetaMap.encode())
                            .setUserMetaData(userMeta.encode())
                            .setBucketName(bucketName)
                            .setObjName(objName)
                            .setVersionId(metaData.versionId)
                            .setStamp(stamp)
                            .setObjSize(String.valueOf(metaData.endIndex + 1))
                            .setInode(metaData.inode);
                    Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(false));

                    return Mono.just(migrateVnode[0] != null)
//                            .flatMap(m -> {
//                                if (hasStartFS[0]) {
//                                    return InodeUtils.checkOverWrite(metaData, m, inodeOverWrite, updateQuotaDir);
//                                }
//                                return Mono.just(m);
//                            })
                            .flatMap(b -> {
                                if (b) {
                                    if (migrateVnode[0].equals(bucketVnode[0])) {
                                        return Mono.just(true);
                                    }
                                    return bucketPool.mapToNodeInfo(migrateVnode[0])
                                            .flatMap(nodeList -> putMetaData(Utils.getMetaDataKey(migrateVnode[0], bucketName,
                                                    objName, metaData.versionId, metaData.stamp, metaData.snapshotMark), metaData, nodeList, recoverDataProcessor, request, msg.get("mda"), esMeta,
                                                    snapshotLink[0]));
                                }
                                return Mono.just(true);
                            })
                            .doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(UNKNOWN_ERROR, "put object meta to new mapping error!");
                                }
                            })
                            .flatMap(b -> {
                                if (!updateQuotaDir.isEmpty()) {
                                    Inode inode = Inode.defaultInode(metaData);
                                    inode.getXAttrMap().put(QUOTA_KEY, Json.encode(updateQuotaDir));
                                    inode.getInodeData().clear();
                                    metaData.tmpInodeStr = Json.encode(inode);
                                }
                                return putMetaData(Utils.getMetaDataKey(bucketVnode[0], bucketName,
                                        objName, metaData.versionId, metaData.stamp, metaData.snapshotMark), metaData, bucketList, recoverDataProcessor, request, msg.get("mda"), esMeta,
                                        snapshotLink[0]);
                            })
                            .doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(UNKNOWN_ERROR, "put meta data error!");
                                }
                                Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(true));
                            })
                            .flatMap(b -> {
                                if (!"off".equals(msg.get("mda"))) {
                                    MonoProcessor<Boolean> esRes = MonoProcessor.create();
                                    Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> EsMetaTask.putEsMeta(esMeta, false)).subscribe(esRes::onNext);
                                    return esRes;
                                } else {
                                    return Mono.just(b);
                                }
                            })
                            .flatMap(b0 -> {
                                try {
                                    // 如果当前桶已开启NFS功能，则上传对象后需更新父目录时间；
                                    if (hasStartFS[0]) {
                                        return InodeUtils.updateSpeciDirTime(dirInodes[0], objName, bucketName)
                                                .map(i -> b0);
                                    }
                                } catch (Exception e) {
                                    logger.error("{} ", e);
                                }
                                return Mono.just(b0);
                            });
                })
                .subscribe(b -> {
                    try {
                        if (isChunked) {
                            request.headers().add(ACTUAL_OBJECT_SIZE, String.valueOf(metaData.endIndex - metaData.startIndex + 1));
                        }
                        QuotaRecorder.addCheckBucket(bucketName);
                        addPublicHeaders(request, requestId)
                                .putHeader(CONTENT_LENGTH, "0")
                                .putHeader(META_ETAG, '"' + sysMetaMap.getString(ETAG) + '"')
                                .putHeader(X_AMX_VERSION_ID, metaData.versionId);
                        if ("true".equals(request.getIsSwift())) {
                            request.response().setStatusCode(CREATED);
                        }
                        CryptoUtils.addCryptoResponse(request, crypto[0]);
                        addAllowHeader(request.response()).end();
                    } catch (Exception e) {
                        logger.error("{} ", e);
                    }
                }, e -> dealException(requestId, request, e, false));

        request.addResponseCloseHandler(v -> disposable.dispose());

        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 删除MOSS上指定的对象(指定版本)
     *
     * @param request 请求参数
     * @return 删除的结果
     */
    public int deleteObjectVersion(MsHttpRequest request) {
        return deleteObject(request);
    }


    /**
     * 处理删除对象的请求
     *
     * @param request
     * @return
     */
    public int deleteObject(MsHttpRequest request) {
        final String bucketName = request.getBucketName();
        final String objName = request.getObjectName();
        final String requestId = getRequestId();
        String versionId = request.headers().contains(VERSIONID) ? request.headers().get(VERSIONID) : request.headers().get("versionid");
        versionId = request.params().contains(VERSIONID) ? request.params().get(VERSIONID) : request.params().get("versionid");

        String finalVersionId = versionId;
        Disposable subscribe = deleteObj(request, null, requestId)
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(DEL_OBJ_ERR, "delete object internal error");
                    }
                })
                .subscribe(b -> {
                    try {
                        QuotaRecorder.addCheckBucket(bucketName);
                        addPublicHeaders(request, requestId).setStatusCode(204);
                        if (request.headers().contains(IS_SYNCING)) {
                            addPublicHeaders(request, requestId).setStatusMessage("success_del");
                        }
                        addAllowHeader(request.response()).end();
                        delObjLogger.info("deleteObject success,bucketName:{} , objName: {}, version: {}", bucketName, objName, finalVersionId);
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }, e -> {
                    if (e instanceof MsException) {
                        MsException msException = (MsException) e;
                        if (NO_SUCH_OBJECT == msException.getErrCode() || SYNC_DATA_MODIFIED == msException.getErrCode()) {
                            if (NO_SUCH_OBJECT == msException.getErrCode()) {
                                if (!request.headers().contains(IS_SYNCING)) {
                                    delObjLogger.error(msException.getMessage() + " bucket: {} object: {} request id: {}",
                                            request.getBucketName(), request.getObjectName(), requestId);
                                }
                            } else {
                                logger.error(msException.getMessage() + " bucket: {} object: {} request id: {}",
                                        request.getBucketName(), request.getObjectName(), requestId);
                            }
                            try {
                                if (NO_SUCH_OBJECT == msException.getErrCode() && request.headers().contains(X_AUTH_TOKEN)) {
                                    addPublicHeaders(request, requestId).setStatusCode(404).end();
                                } else if (NO_SUCH_OBJECT == msException.getErrCode() && request.headers().contains(IS_SYNCING)) {
                                    addPublicHeaders(request, requestId).setStatusCode(204).setStatusMessage("NO_SUCH_OBJECT").end();
                                } else {
                                    addPublicHeaders(request, requestId).setStatusCode(204).end();
                                }
                            } catch (Exception exception) {
                                logger.error("add public headers to response failed. {}", exception.getMessage());
                                request.response().close();
                                request.connection().close();
                            }
                            return;
                        }
                    }
                    if (SKIP_SIGNAL.equals(e.getMessage())) {
                        return;
                    }
                    dealException(request, e);
                });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 添加多版本删除标记
     */
    private Mono<Boolean> addVersionDeleteMarker(MsHttpRequest request0, String bucketVnode, String migrateVnode, SocketReqMsg msg, MetaData meta, MsHttpRequest subRequest,
                                                 String currentSnapshotMark, String snapshotLink) {
        MsHttpRequest request = subRequest == null ? request0 : subRequest;
        final String bucketName = request.getBucketName();
        final String objName = subRequest != null ? subRequest.getObjectName() : request.getObjectName();
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);

        return MsObjVersionUtils.getObjVersionIdReactive(bucketName)
                .zipWith(VersionUtil.getVersionNum(bucketName, objName))
//                .flatMap(tuple -> MsObjVersionUtils.checkObjVersionsLimit(bucketName, objName, request).map(info -> tuple))
                .flatMap(tuple2 -> {
                    String versionId = tuple2.getT1();
                    String versionNum = tuple2.getT2();
                    versionId = request.headers().contains(NEW_VERSION_ID) ? request.headers().get(NEW_VERSION_ID) : versionId;
                    Map<String, String> sysMetaData = new HashMap<>(4);
                    sysMetaData.put("displayName", msg.get("bucket_userId"));
                    String stamp = StringUtils.isNoneBlank(request.getHeader("stamp")) ? request.getHeader("stamp") : String.valueOf(System.currentTimeMillis());
                    String lastModify = MsDateUtils.stampToGMT(Long.parseLong(stamp));
                    sysMetaData.put(LAST_MODIFY, lastModify);
                    sysMetaData.put("owner", request.getUserId());
                    meta.setSysMetaData(JsonUtils.toString(sysMetaData, HashMap.class));
                    String metaKey = Utils.getMetaDataKey(bucketVnode, bucketName, objName, versionId, stamp, currentSnapshotMark);
                    if (migrateVnode != null) {
                        msg.put("migrateKey", Utils.getMetaDataKey(bucketVnode, bucketName, objName, versionId, stamp, currentSnapshotMark));
                    }
                    msg.put("metaKey", metaKey);
                    meta.setVersionId(versionId);
                    meta.setReferencedVersionId(versionId);
                    meta.setStamp(stamp);
                    meta.setDeleteMarker(true);
                    meta.setVersionNum(versionNum);
                    meta.setFileName(null);
                    meta.setAggregationKey(null);
                    meta.setPartUploadId(null);
                    meta.setPartInfos(null);
                    meta.setSyncStamp(StringUtils.isBlank(request.getHeader(SYNC_STAMP)) ? versionNum : request.getHeader(SYNC_STAMP));
                    meta.setShardingStamp(VersionUtil.getVersionNum());
                    meta.setSnapshotMark(currentSnapshotMark);
                    return Mono.just(versionId);
                })
                .flatMap(b -> {
                    if (migrateVnode != null) {
                        return storagePool.mapToNodeInfo(migrateVnode)
                                .flatMap(migrateVnodeList -> putMetaData(msg.get("migrateKey"), meta, migrateVnodeList, null, request, msg.get("mda"), null, snapshotLink));
                    }
                    return Mono.just(true);
                })
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(DEL_OBJ_ERR, "delete object migrate " + migrateVnode + " internal error");
                    }
                })
                .flatMap(v -> storagePool.mapToNodeInfo(bucketVnode))
                .flatMap(bucketVnodeList -> putMetaData(msg.get("metaKey"), meta, bucketVnodeList, null, request, msg.get("mda"), null, snapshotLink))
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(DEL_OBJ_ERR, "delete object internal error");
                    } else {
                        if (subRequest != null) {
                            subRequest.headers().set(X_AMX_VERSION_ID, meta.versionId)
                                    .set(X_AMX_DELETE_MARKER, "true");
                        } else {
                            request.response().putHeader(X_AMX_VERSION_ID, meta.versionId)
                                    .putHeader(X_AMX_DELETE_MARKER, "true");
                        }
                    }
                });
    }

    private Mono<Boolean> delVersionObj(MsHttpRequest request0, List<String> needDeleteFile, SocketReqMsg msg, MetaData[] metaData, MsHttpRequest subRequest, String currentSnapshotMark) {
        MsHttpRequest request = subRequest == null ? request0 : subRequest;
        final String bucketName = request.getBucketName();
        final String objName = request.getObjectName();
        String requestUserId = request.getUserId();
        StoragePool storagePool = StoragePoolFactory.getStoragePool(metaData[0]);
        Set<String> dedupFile = new HashSet<>();
        List<String> allFile = new ArrayList<>();
        Mono<Boolean> result = null;
        boolean deleteESSource = !isNotDeleteEs || request.getHeader("deleteSource") == null || !"1".equals(request.getHeader("deleteSource"));
        if (metaData[0].partUploadId != null) {
            PartInfo[] partInfos = metaData[0].partInfos;
            for (int i = 0; i < partInfos.length; i++) {
                if (partInfos[i].isCurrentSnapshotObject(currentSnapshotMark)) {// 只删除当前快照下上传的数据块
                    if (StringUtils.isNotEmpty(partInfos[i].deduplicateKey)) {
                        //缓存池对象删除时需要置换data名字
                        dedupFile.add(partInfos[i].deduplicateKey);
                    } else {
                        allFile.add(partInfos[i].fileName);
                    }
                }
            }
        } else if (StringUtils.isNotEmpty(metaData[0].duplicateKey)) {
            if (currentSnapshotMark != null && !currentSnapshotMark.equals(metaData[0].snapshotMark)) { // 只删除当前快照下上传的数据块
                return Mono.just(true);
            }
            Map<String, String> sysMetaMap = Json.decodeValue(metaData[0].sysMetaData, new TypeReference<Map<String, String>>() {
            });

            StorageOperate dataOperate = new StorageOperate(DATA, objName, Long.MAX_VALUE);
            StoragePool dataPool = StoragePoolFactory.getStoragePool(dataOperate, bucketName);
            dedupFile.add(metaData[0].duplicateKey);

            if (metaData[0].storage.startsWith("cache")) {
                String dedup = metaData[0].duplicateKey.replace(metaData[0].storage, dataPool.getVnodePrefix());
                dedupFile.add(dedup);
            }
            StoragePoolFactory.getAllStorage(metaData[0].duplicateKey, dedupFile);

            String md5 = sysMetaMap.get(ETAG);
            StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);
            String dedupVnode = dedupPool.getBucketVnodeId(md5);
            return dedupPool.mapToNodeInfo(dedupVnode)
                    .flatMap(nodeList -> deleteDedupObjectFile(dedupPool, dedupFile.toArray(new String[0]), request, false))
                    .flatMap(b -> {
                        if (!"off".equals(msg.get("mda"))) {
                            return EsMetaTask.putOrDelS3EsMeta(requestUserId, bucketName, objName, metaData[0].versionId, metaData[0].endIndex + 1, request, false, !deleteESSource);
                        } else {
                            return Mono.just(b);
                        }
                    });
        } else if (StringUtils.isNotEmpty(metaData[0].aggregationKey)) {
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
            String vnode = Utils.getVnode(metaData[0].aggregationKey);
            return metaStoragePool.mapToNodeInfo(vnode)
                    .flatMap(nodeList -> AggregateFileClient.freeAggregationSpace(metaData[0].aggregationKey, nodeList));
        }

        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        if (!dedupFile.isEmpty() && dedupFile.size() > 0) {
            result = ErasureClient.deleteDedupObjectFile(metaStoragePool, dedupFile.toArray(new String[0]), request, false)
                    .flatMap(b -> {
                        if (!allFile.isEmpty()) {
                            return deleteObjectFile(storagePool, allFile.toArray(new String[0]), request);
                        }
                        return Mono.just(b);
                    });
        } else {
            result = deleteObjectFile(storagePool, needDeleteFile.toArray(new String[0]), request);
        }

        return result
                .flatMap(b -> {
                    if (!"off".equals(msg.get("mda"))) {
                        return EsMetaTask.putOrDelS3EsMeta(requestUserId, bucketName, objName, metaData[0].versionId, metaData[0].endIndex + 1, request, false, !deleteESSource);
                    } else {
                        return Mono.just(b);
                    }
                });
    }

    /**
     * 批量删除指定bucket下的对象
     *
     * @param request 请求参数
     * @return 对象元数据信息，不包含对象内容
     */
    public int deleteMultipleObjects(MsHttpRequest request) {

        final String bucketName = request.getBucketName();
        String requestUserId = request.getUserId();
        String contentLength = request.getHeader("Content-Length");
        String method = "DeleteMultipleObjects";
        final int[] policy = new int[1];
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_DEL_ALL_OBJECT, 0);
        pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName)))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .doOnNext(bucketInfo -> request.headers().add(BUCKET_VERSION_STATUS, bucketInfo.getOrDefault(BUCKET_VERSION_STATUS, "NULL"))
                        .add(DATA_SYNC_SWITCH, bucketInfo.getOrDefault(DATA_SYNC_SWITCH, "off")))
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> {
                    if (Long.parseLong(contentLength) > 2 * 1024 * 1024) {
                        throw new MsException(REQUEST_BODY_TOO_LONG, "The number of request body is too long");
                    }
                    if (policy[0] != 0) {
                        return Mono.empty();
                    }
                    return MsAclUtils.checkWriteAclReactive(bucketInfo, requestUserId, bucketName, msg);
                })
                .switchIfEmpty(MsObjVersionUtils.versionStatusReactive(bucketName))
                .flatMap(status -> {
                    Buffer buffer = Buffer.buffer();
                    request.handler(buffer::appendBuffer);
                    request.endHandler(v -> {
                        if (buffer.toString() == null) {
                            responseError(request, INPUT_EMPTY_OBJECT);
                        } else {
                            Delete delete = (Delete) JaxbUtils.toObject(buffer.toString());
                            if (delete == null) {
                                responseError(request, MALFORMED_XML);
                            } else {
                                if (delete.getObjects() == null) {
                                    responseError(request, MALFORMED_XML);
                                } else if (delete.getObjects().size() > 1000) {
                                    responseError(request, TOO_MANY_OBJECTS);
                                } else {
                                    boolean valid = true;
                                    // 判断对象列表中是否存在对象名称为空的对象
                                    for (ObjectsList object : delete.getObjects()) {
                                        if (object == null || StringUtils.isBlank(object.getKey())) {
                                            valid = false;
                                            break;
                                        }
                                    }
                                    if (valid) {
                                        try {
                                            checkContentMD5(request, buffer.toString());
                                        } catch (MsException e) {
                                            logger.error("", e);
                                            responseError(request, e.getErrCode());
                                            return;
                                        } catch (Exception e) {
                                            logger.error("", e);
                                            responseError(request, UNKNOWN_ERROR);
                                            return;
                                        }
                                        if (isMultiAliveStarted && request.headers().contains(ClUSTER_NAME_HEADER)) {
                                            try {
                                                int clusterIndex = AssignClusterHandler.getInstance().checkClusterIndexHeader(request);
                                                if (clusterIndex != LOCAL_CLUSTER_INDEX) {
                                                    AssignClusterHandler.getInstance().sendLongRequest(request, buffer.getBytes())
                                                            .doOnError(e -> dealException(request, e))
                                                            .subscribe();
                                                    return;
                                                }
                                            } catch (Exception e) {
                                                dealException(request, e);
                                                return;
                                            }
                                        }
                                        if (Objects.nonNull(request.getMember(STORE_MANAGEMENT_HEADER))) {
                                            final AtomicInteger times = new AtomicInteger(0);
                                            String requestId = getRequestId();
                                            request.addMember(REQUESTID, requestId);
                                            final byte[] xmlHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>".getBytes();
                                            long timerId = vertx.setPeriodic(20000, (idP) -> {
                                                try {
                                                    if (times.incrementAndGet() == 1) {
                                                        addPublicHeaders(request, requestId).putHeader(CONTENT_TYPE, "application/xml")
                                                                .putHeader(TRANSFER_ENCODING, "chunked")
                                                                .write(Buffer.buffer(xmlHeader));
                                                    }
                                                    request.response().write("\n");
                                                } catch (Exception e) {
                                                    logger.info("still run timer, {}", e.getMessage());
                                                    vertx.cancelTimer(idP);
                                                }
                                            });
                                            forwardRequest(request, false, buffer.getBytes())
                                                    .doFinally(f -> {
                                                        vertx.cancelTimer(timerId);
                                                        if (times.incrementAndGet() != 1) {
                                                            request.addMember("timerId", "");
                                                        }
                                                        deleteObjects(request, delete);
                                                    })
                                                    .subscribe();
                                            return;
                                        }
                                        deleteObjects(request, delete);
                                    } else {
                                        responseError(request, MALFORMED_XML);
                                    }
                                }
                            }
                        }
                    }).resume();
                    return Mono.empty();
                })
                .subscribe(b -> {
                }, e -> dealException(request, e));
        return ErrorNo.SUCCESS_STATUS;
    }

    private static final int RUN_NUM = 10;

    private void deleteObjects(MsHttpRequest request, Delete delete) {
        String versionStatus = request.headers().get(BUCKET_VERSION_STATUS);
        boolean datasyncIsEnabled = isSwitchOn(request.headers().get(DATA_SYNC_SWITCH));
        request.headers().remove(BUCKET_VERSION_STATUS).remove(DATA_SYNC_SWITCH);
        final String requestId = Objects.isNull(request.getMember(REQUESTID)) ? getRequestId() : request.getMember(REQUESTID);
        List<ObjectsList> objectKey = new ArrayList<>(delete.getObjects().size());
        Set<String> repeated = new HashSet<>();
        delete.getObjects().forEach(object -> {
            if (null != object.getVersionId() || !repeated.contains(object.getKey())) {
                objectKey.add(object);
                if (null == object.getVersionId()) {
                    repeated.add(object.getKey());
                }
            }
        });
        Queue<Deleted> deletedObj = new ConcurrentLinkedQueue<>();
        Queue<Error> errors = new ConcurrentLinkedQueue<>();
        final byte[] xmlHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>".getBytes();
        final AtomicInteger times = new AtomicInteger(0);
        // setPeriodic就算有未捕获的错误也不会中止
        long timerId = vertx.setPeriodic(20000, (idP) -> {
            try {
                if (times.incrementAndGet() == 1 && Objects.isNull(request.getMember("timerId"))) {
                    addPublicHeaders(request, requestId).putHeader(CONTENT_TYPE, "application/xml")
                            .putHeader(TRANSFER_ENCODING, "chunked")
                            .write(Buffer.buffer(xmlHeader));
                }
                request.response().write("\n");
            } catch (Exception e) {
                logger.info("still run timer, {}", e.getMessage());
                vertx.cancelTimer(idP);
            }
        });
        Disposable[] disposables = new Disposable[3];
        UnicastProcessor<ObjectsList> processor = UnicastProcessor.create(Queues.<ObjectsList>unboundedMultiproducer().get());
        int runNum = 0;
        // 最多并发删除RUN_NUM个
        for (; runNum < RUN_NUM && runNum < objectKey.size(); runNum++) {
            processor.onNext(objectKey.get(runNum));
        }

        AtomicInteger atomicLong = new AtomicInteger(runNum);

        int finalRunNum = runNum;
        disposables[0] = processor.publishOn(DISK_SCHEDULER).doOnNext(KeyVersion -> {
            logger.debug("the object waiting to be deleted is  :" + KeyVersion);
            String uri = UrlEncoder.encode("/" + request.getBucketName() + "/" + KeyVersion.getKey(), "UTF-8");
            if (StringUtils.isNotBlank(KeyVersion.getVersionId())) {
                uri += "?versionId=" + KeyVersion.getVersionId();
            }

            HttpServerRequestImpl httpServerRequest = null;
            try {
                Constructor<HttpServerRequestImpl> constructor = HttpServerRequestImpl.class.getDeclaredConstructor(Http1xServerConnection.class, HttpRequest.class);
                constructor.setAccessible(true);
                httpServerRequest = constructor.newInstance(null, null);

                Field methodField = HttpServerRequestImpl.class.getDeclaredField("method");
                methodField.setAccessible(true);
                methodField.set(httpServerRequest, HttpMethod.DELETE);

                Field headersField = HttpServerRequestImpl.class.getDeclaredField("headers");
                headersField.setAccessible(true);
                headersField.set(httpServerRequest, new CaseInsensitiveHeaders());

                Field paramsField = HttpServerRequestImpl.class.getDeclaredField("params");
                paramsField.setAccessible(true);
                paramsField.set(httpServerRequest, RestfulVerticle.params(uri));

                Field uriField = HttpServerRequestImpl.class.getDeclaredField("uri");
                uriField.setAccessible(true);
                uriField.set(httpServerRequest, uri);
            } catch (Exception e) {
                logger.error("reflect HttpServerRequestImpl error. ", e);
            }
            MsHttpRequest msHttpRequest = new MsHttpRequest(httpServerRequest, true);
            msHttpRequest.setBucketName(request.getBucketName());
            msHttpRequest.setObjectName(request.getObjectName());
            msHttpRequest.setCodec(request.getCodec());
            msHttpRequest.setUserId(request.getUserId());
            msHttpRequest.setAccessKey(request.getAccessKey());
            msHttpRequest.setUri(uri);
            String trashDir = bucketTrash.get(request.getBucketName());
            if (StringUtils.isNotEmpty(trashDir)) {
                msHttpRequest.headers().set("trashDir", trashDir);
            }
            if (StringUtils.isNotEmpty(request.getHeader("recoverObject"))) {
                msHttpRequest.headers().set("recoverObject", request.getHeader("recoverObject"));
            }

            request.headers().forEach(entry -> Optional.ofNullable(entry).ifPresent(entry1 -> msHttpRequest.headers().set(entry1.getKey(), entry1.getValue())));

            StoragePool bucketPool = StoragePoolFactory.getStoragePool(StorageOperate.META, request.getBucketName());
            String bucketVnode = bucketPool.getBucketVnodeId(request.getBucketName());

            AtomicBoolean needFlushSyncRecord = new AtomicBoolean(isMultiAliveStarted && datasyncIsEnabled && !IS_ASYNC_CLUSTER);
            if (request.headers().contains(ClUSTER_NAME_HEADER)) {
                needFlushSyncRecord.compareAndSet(true, false);
            }
            UnSynchronizedRecord preRecord = needFlushSyncRecord.get() ? buildSyncRecord(msHttpRequest, versionStatus) : new UnSynchronizedRecord();
            preRecord.setMethod(HttpMethod.DELETE);
            preRecord.setObject(KeyVersion.getKey());
            // 批删保证生成差异记录的stamp和本地的删除标记的stamp一致
            String stamp = String.valueOf(System.currentTimeMillis());
            Optional.ofNullable(preRecord.headers).ifPresent(headers -> headers.put("stamp", stamp));
            msHttpRequest.headers().set("stamp", stamp);

            msHttpRequest.setObjectName(KeyVersion.getKey());
            if (null != KeyVersion.getVersionId()) {
                msHttpRequest.headers().add(VERSIONID, KeyVersion.getVersionId());
            } else {
                if (null != msHttpRequest.getHeader(VERSIONID)) {
                    msHttpRequest.headers().remove(VERSIONID);
                }
                String newVersionId = "NULL".equals(versionStatus) || VERSION_SUSPENDED.equals(versionStatus) ?
                        "null" : RandomStringUtils.randomAlphanumeric(32);
                msHttpRequest.headers().set(NEW_VERSION_ID, newVersionId).set(SYNC_STAMP, preRecord.syncStamp);
                Optional.ofNullable(preRecord.headers).ifPresent(headers -> headers.put(NEW_VERSION_ID, newVersionId));
            }
            preRecord.setUri(uri);
            String bucketVnodeId = bucketPool.getBucketVnodeId(msHttpRequest.getBucketName(), msHttpRequest.getObjectName());
            disposables[1] = bucketPool.mapToNodeInfo(bucketVnode)
                    .zipWith(pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(request.getBucketName(), ARCHIVE_SWITCH).defaultIfEmpty("off"))
                    .flatMap(tuple2 -> {
                        if (needFlushSyncRecord.get()) {
                            if ("on".equals(tuple2.getT2())) {
                                needFlushSyncRecord.set(false);
                                return Mono.just(true);
                            }
                            preRecord.rocksKey();
                            boolean isVersion = ("NULL".equals(versionStatus)) && (KeyVersion.getVersionId() == null);
                            String tempVersion = isVersion ? "null" : KeyVersion.getVersionId();
                            return bucketPool.mapToNodeInfo(bucketVnodeId)
                                    .flatMap(infoList -> ErasureClient.getObjectMetaVersionUnlimited(msHttpRequest.getBucketName(), msHttpRequest.getObjectName(), tempVersion, infoList, request))
                                    .flatMap(metaData -> {
                                        String syncStamp = preRecord.syncStamp;
                                        if (metaData.equals(ERROR_META)) {
                                            throw new MsException(ErrorNo.UNKNOWN_ERROR, "can't get object meta " + KeyVersion);
                                        }
                                        if (metaData.equals(NOT_FOUND_META) || metaData.deleteMark) {
                                            if (StringUtils.isNotEmpty(tempVersion)) {
                                                throw new MsException(NO_SUCH_OBJECT, "no such object, object name: " + KeyVersion);
                                            } else {
                                                return ECUtils.putSynchronizedRecord(bucketPool, preRecord.recordKey, Json.encode(preRecord), tuple2.getT1(), WRITE_ASYNC_RECORD, request);
                                            }
                                        } else {
                                            if (null != KeyVersion.getVersionId() || "NULL".equals(versionStatus)) {
                                                msHttpRequest.headers().set(SYNC_STAMP, metaData.syncStamp);
                                                preRecord.setSyncStamp(metaData.syncStamp);
                                                preRecord.headers.put(SYNC_STAMP, metaData.syncStamp);
                                            }

                                            if (StringUtils.isNotEmpty(trashDir) && StringUtils.isNotEmpty(msHttpRequest.getHeader("recoverObject"))) {
                                                msHttpRequest.headers().set(LAST_STAMP, syncStamp);
                                                preRecord.setSyncStamp(syncStamp);
                                            }

                                            return ECUtils.putSynchronizedRecord(bucketPool, preRecord.recordKey, Json.encode(preRecord), tuple2.getT1(), WRITE_ASYNC_RECORD, request);
                                        }
                                    });
                        }
                        return Mono.just(true);
                    })
                    .doOnNext(res -> {
                        if (!res) {
                            logger.error("write async preRecord error! {}", preRecord);
                            throw new MsException(ErrorNo.UNKNOWN_ERROR, "");
//                            responseError(request, ErrorNo.UNKNOWN_ERROR);
                        }
                    })
                    .flatMap(res -> deleteObj(request, msHttpRequest, ""))
                    .doOnNext(b -> {
                        if (!b) {
                            int code = new ResponseMsg(UNKNOWN_ERROR)
                                    .getErrCode();
                            Error errorInfo = ErrMsgBuilder.build(KeyVersion.getKey(), code);
                            if (KeyVersion.getVersionId() != null) {
                                errorInfo = ErrMsgBuilder.build(KeyVersion.getKey(), KeyVersion.getVersionId(), code);
                            }
                            errors.add(errorInfo);
                        }
                    })
                    .onErrorResume(MsException.class, e -> {
                        if (NO_SUCH_OBJECT == e.getErrCode()) {
                            return Mono.just(true);
                        }

                        Error errorInfo = ErrMsgBuilder.build(KeyVersion.getKey(), e.getErrCode());
                        if (null != KeyVersion.getVersionId()) {
                            errorInfo = ErrMsgBuilder.build(KeyVersion.getKey(), KeyVersion.getVersionId(), e.getErrCode());
                        }
                        errors.add(errorInfo);
                        logger.error("the object is failed deleted :  " + KeyVersion);
                        return Mono.just(false);
                    })
                    .onErrorResume(Exception.class, e -> {
                        logger.error("execute error", e);
                        int code = new ResponseMsg(UNKNOWN_ERROR)
                                .getErrCode();
                        Error errorInfo = ErrMsgBuilder.build(KeyVersion.getKey(), code);
                        if (KeyVersion.getVersionId() != null) {
                            errorInfo = ErrMsgBuilder.build(KeyVersion.getKey(), KeyVersion.getVersionId(), code);
                        }
                        errors.add(errorInfo);
                        return Mono.just(false);
                    })
                    .publishOn(DISK_SCHEDULER)
                    .doFinally(signalType -> msHttpRequest.shutdown())
                    .subscribe(b -> {
                        if (b) {
                            Deleted key = new Deleted().setKey(KeyVersion.getKey())
                                    .setVersionId(KeyVersion.getVersionId()).setDeleteMarker(null);

                            String versionId = msHttpRequest.headers().get(X_AMX_VERSION_ID);
                            key.setVersionId(versionId);
                            if (msHttpRequest.headers().contains(X_AMX_DELETE_MARKER)) {
                                key.setDeleteMarker(true);
                                key.setDeleteMarkerVersionId(versionId);
                            }

                            disposables[2] = bucketPool.mapToNodeInfo(bucketVnode)
                                    .subscribe(nodeList -> {
                                        if (needFlushSyncRecord.get()) {
                                            ECUtils.updateSyncRecord(preRecord.setCommited(true), nodeList, WRITE_ASYNC_RECORD, request)
                                                    .map(resInt -> resInt != 0)
                                                    .subscribe(res1 -> {
                                                        if (!res1) {
                                                            logger.error("commit sync preRecord error!" + preRecord);
                                                        }
                                                    }, e -> logger.error("commit sync preRecord error, " + preRecord, e));
                                        }
                                    });

                            deletedObj.add(key);
                        } else {
                            logger.info("------------------{}", msHttpRequest.getObjectName());
                        }

                        // 释放之前的流资源
                        msHttpRequest.shutdown();

                        int next = atomicLong.getAndIncrement();
                        if (next < objectKey.size()) {
                            processor.onNext(objectKey.get(next));
                        } else if (next == objectKey.size() + finalRunNum - 1) {
                            DeleteMultipleObjects deleteMultipleObjects = new DeleteMultipleObjects();
                            if ("true".equals(delete.getQuiet())) {
                                deleteMultipleObjects.setErrors(new LinkedList<>(errors));
                            } else {
                                deleteMultipleObjects
                                        .setErrors(new LinkedList<>(errors))
                                        .setDeleted(new LinkedList<>(deletedObj));
                            }

                            byte[] data = JaxbUtils.toByteArray(deleteMultipleObjects);
                            vertx.cancelTimer(timerId);
                            if (times.get() > 0) {
                                request.response().end(Buffer.buffer(data).slice(xmlHeader.length, data.length));
                            } else {
                                addPublicHeaders(request, requestId).putHeader(CONTENT_TYPE, "application/xml")
                                        .putHeader(CONTENT_LENGTH, String.valueOf(data.length))
                                        .end(Buffer.buffer(data));
                            }

                            //桶通知进行批量删除的通知
                            checkNotification(request).subscribe(event -> {
                                if (event[0].startsWith("ObjectRemoved:multiDelete")) {
                                    String[] result = event[0].split(":");
                                    String prefix = "";
                                    String suffix = "";
                                    if (result.length == 4) {
                                        prefix = result[2];
                                        suffix = result[3];
                                    } else if (result.length == 3) {
                                        prefix = result[2];
                                    }
                                    NotificationInfo info = new NotificationInfo();
                                    info.setInfo(prefix, suffix, event, delete.getObjects(), request);
                                    String str = msHttpRequest.getHeader(NEW_VERSION_ID);
                                    if (msHttpRequest.headers().contains(X_AMX_DELETE_MARKER)) {
                                        saveInfoToQueue(info, true);
                                    } else {
                                        saveInfoToQueue(info, false);
                                    }
                                }
                            });

                        }
                        delObjLogger.info("the object in {} has been dealt finish {}", msHttpRequest.getBucketName(), KeyVersion);
                        QuotaRecorder.addCheckBucket(msHttpRequest.getBucketName());
                    }, e -> dealException(request, e));
        }).doOnError(e -> {
            logger.error("send error", e);
            dealException(request, e);
        }).subscribe();
        request.addResponseCloseHandler(v -> {
            streamDispose(disposables);
            vertx.cancelTimer(timerId);
        });
        request.addResponseEndHandler(v -> vertx.cancelTimer(timerId));
    }

    /**
     * 批删接口删单个对象实际流程
     *
     * @param subRequest 批删使用，保存单个对象的删除所需的信息。没有绑定连接，因此不可进行任何通信。
     */
    private Mono<Boolean> deleteObj(MsHttpRequest request0, MsHttpRequest subRequest, String requestId) {
        MsHttpRequest request = subRequest == null ? request0 : subRequest;
        final String bucketName = request.getBucketName();
        final String objName = request.getObjectName();
        StoragePool bucketPool = StoragePoolFactory.getStoragePool(StorageOperate.META, bucketName);
        com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucketName, objName);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        String requestUserId = request.getUserId();
        final String backupUserId = request.getHeader("backup_userId");
        final Long[] currentWormStamp = new Long[1];
        String reqWormStamp = request.getHeader(WORM_STAMP);
        UnifiedMap<String, String> paramMap = RequestBuilder.parseRequest(request);
        paramMap.put(SOURCEIP, request0.remoteAddress().host());
        String versionId = StringUtils.isEmpty(paramMap.get(VERSIONID)) ? paramMap.get("versionid") : paramMap.get(VERSIONID);
        final String method = StringUtils.isEmpty(versionId) ? "DeleteObject" : "DeleteObjectVersion";
        SocketReqMsg msg = SocketReqMsgBuilder.buildDelEcObjMsg(bucketName, objName, requestId);
        //estask中使用
        msg.put("userId", requestUserId);
        List<String> needDeleteFile = new LinkedList<>();
        MetaData[] metaData = new MetaData[]{null};
        final int[] policy = new int[1];
        String[] trashDir = new String[1];
        String[] versionStatus = new String[]{"NULL"};
        final Boolean[] trashCopyResult = {false, false};
        final String[] versionIdArr = new String[1];
        final com.macrosan.utils.functional.Tuple2<Boolean, Inode>[] tuple2s = new com.macrosan.utils.functional.Tuple2[]{new com.macrosan.utils.functional.Tuple2<Boolean, Inode>()};
        final String[] currentSnapshotMark = new String[1];
        final String[] snapshotLink = new String[1];
        String[] updateDirStr = new String[1];
        boolean[] hasStartFS = {false};
        int[] startFsType = {0};
        Inode[] dirInodes = {null};
        boolean deleteESSource = !isNotDeleteEs || request.getHeader("deleteSource") == null || !"1".equals(request.getHeader("deleteSource"));
        return currentWormTimeMillis()
                .doOnNext(l -> currentWormStamp[0] = reqWormStamp == null ? l : Long.valueOf(reqWormStamp))
                .flatMap(l -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                        .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                        .doOnNext(this::siteCheck)
                        .doOnNext(bucketInfo -> versionIdArr[0] = StringUtils.isNotEmpty(versionId) ? versionId
                                : bucketInfo.containsKey(BUCKET_VERSION_STATUS) ? "" : "null")
                        .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, objName, versionIdArr[0],
                                method, request0.remoteAddress().host()).zipWith(Mono.just(bucketInfo)))
                        .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                        .map(Tuple2::getT2)
                        .doOnNext(bucketInfo -> {
                            // 获取当前桶的快照链接关系和当前快照标记
                            snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                            currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                        })
                        .flatMap(bucketInfo -> {
                            if (bucketInfo.containsKey("fsid")) {
                                hasStartFS[0] = true;
                                startFsType[0] = ACLUtils.setFsProtoType(bucketInfo);
                                return FSQuotaUtils.existQuotaInfoS3(bucketName, objName, System.currentTimeMillis(), requestUserId)
                                        .flatMap(t2 -> {
                                            if (t2.var1) {
                                                updateDirStr[0] = Json.encode(t2.var2);
                                            }
                                            return Mono.just(bucketInfo);
                                        });
                            }
                            return Mono.just(bucketInfo);
                        })
                        //桶的写权限校验
                        .flatMap(bucketInfo -> {
                            if ("on".equals(bucketInfo.getOrDefault("mda", "off"))) {
                                msg.put("mda", "on");
                                EsMetaTask.isExit();
                            } else {
                                msg.put("mda", "off");
                            }
                            msg.put("bucket_userId", bucketInfo.get(BUCKET_USER_ID));
                            if (bucketInfo.containsKey(BUCKET_VERSION_STATUS)) {
                                versionStatus[0] = bucketInfo.get(BUCKET_VERSION_STATUS);
                            }
                            trashDir[0] = bucketInfo.get("trashDir");
                            if (!PASSWORD.equals(request.getHeader(SYNC_AUTH)) && policy[0] == 0) {
                                if (hasStartFS[0]) {
                                    return S3ACL.checkFSACL(objName, bucketName, requestUserId, dirInodes, 12, false, startFsType[0], 0)
                                            .flatMap(b -> MsAclUtils.checkWriteAclReactive(bucketInfo, requestUserId, bucketName, msg));
                                } else {
                                    return MsAclUtils.checkWriteAclReactive(bucketInfo, requestUserId, bucketName, msg);
                                }
                            } else {
                                return Mono.empty();
                            }
                        })
                        .switchIfEmpty(pool.getReactive(REDIS_USERINFO_INDEX).hget(Optional.ofNullable(backupUserId)
                                .orElse(requestUserId), "name"))
                        .flatMap(Object -> MsObjVersionUtils.versionStatusReactive(bucketName))
                        .flatMap(status -> {
                            checkVersionStatus(request0, status);
                            msg.put("status", status);
                            return bucketPool.mapToNodeInfo(bucketVnode);
                        })
                        .flatMap(list -> {
                            if (StringUtils.isNotBlank(updateDirStr[0])) {
                                return getObjectMetaVersionFsQuotaRecover(bucketName, objName, StringUtils.isNotEmpty(versionId) ? versionId : "NULL".equals(msg.get("status")) ? "null" : "", list, request,
                                        currentSnapshotMark[0], snapshotLink[0], updateDirStr[0], true);
                            }
                            return getObjectMetaVersionUnlimited(bucketName, objName,
                                    StringUtils.isNotEmpty(versionId) ? versionId : "NULL".equals(msg.get("status")) ? "null" : "", list, request, currentSnapshotMark[0], snapshotLink[0]);
                        })
                        .flatMap(meta -> {
                            if (isMultiAliveStarted && request.headers().contains(ClUSTER_NAME_HEADER) && Objects.isNull(subRequest)) {
                                int clusterIndex = AssignClusterHandler.getInstance().checkClusterIndexHeader(request);
                                if (clusterIndex == LOCAL_CLUSTER_INDEX) {
                                    return Mono.just(meta);
                                }
                                return AssignClusterHandler.getInstance().sendRequest(request, null)
                                        .flatMap(i -> Mono.error(new RuntimeException(SKIP_SIGNAL)));
                            }
                            return Mono.just(meta);
                        })
                        .flatMap(meta -> {
                            if (Objects.nonNull(request0.getMember(STORE_MANAGEMENT_HEADER)) && Objects.isNull(subRequest)) {
                                return forwardRequest(request0, false, null).flatMap(b -> Mono.just(meta));
                            }
                            return Mono.just(meta);
                        })
                        .flatMap(meta -> {
                            if (meta.equals(ERROR_META)) {
                                throw new MsException(UNKNOWN_ERROR, "Get Object Meta Data fail");
                            }
                            if (meta.equals(NOT_FOUND_META) || meta.deleteMark || meta.isUnView(currentSnapshotMark[0])) {
                                if ("NULL".equals(msg.get("status")) || versionId != null) {
                                    throw new MsException(NO_SUCH_OBJECT, "no such object, object name: " + objName + ".");
                                } else {
                                    MetaData newMeta = new MetaData().setBucket(bucketName).setKey(objName).setPartUploadId(null)
                                            .setReferencedBucket(bucketName).setReferencedKey(objName);
                                    metaData[0] = newMeta;
                                    return Mono.just(metaData[0]);
                                }
                            } else {
                                String syncStamp = request.getHeader(SYNC_STAMP);
                                // 双活站点生成差异进行同步时，比较syncStamp，当对象syncStamp较小时，进行删除
                                if (StringUtils.isNotBlank(syncStamp) && StringUtils.isNotEmpty(versionId)) {
                                    if (request.headers().contains(IS_SYNCING)) {
                                        if (syncStamp.compareTo(meta.syncStamp) < 0) {
                                            logger.info("obj {} syncStamp is different,curr is {} ,sync is {}", objName, meta.syncStamp, syncStamp);
                                            throw new MsException(SYNC_DATA_MODIFIED, "");
                                        }
                                    } else {
                                        if (!syncStamp.equals(meta.syncStamp) && !"true".equals(request.getHeader("recoverObject"))) {
                                            logger.info("obj {} syncStamp is different,curr is {} ,sync is {}", objName, meta.syncStamp, syncStamp);
                                            throw new MsException(SYNC_DATA_MODIFIED, "");
                                        }
                                    }
                                }
                                if (meta.partUploadId == null) {
                                    if (meta.isCurrentSnapshotObject(currentSnapshotMark[0])) { // 只能删除当前快照下上传的数据块
                                        needDeleteFile.add(meta.getFileName());
                                    }
                                } else {
                                    for (PartInfo partInfo : meta.partInfos) {
                                        if (partInfo.isCurrentSnapshotObject(currentSnapshotMark[0])) { // 只能删除当前快照下上传的数据块
                                            needDeleteFile.add(partInfo.fileName);
                                        }
                                    }
                                }
                                metaData[0] = meta;
                                return Mono.just(metaData[0]);
                            }
                        })
                        .flatMap(meta -> {
                            if (hasStartFS[0]) {
                                S3ACL.judgeDeleteCifsACL(requestUserId, meta, 12, startFsType[0], dirInodes[0]);
                                if (StringUtils.isNotBlank(updateDirStr[0])) {
                                    List<String> updateCapKeys = Json.decodeValue(updateDirStr[0], new TypeReference<List<String>>() {
                                    });
                                    com.macrosan.utils.functional.Tuple2<Boolean, List<String>> tuple2 = new com.macrosan.utils.functional.Tuple2<>(true, updateCapKeys);
                                    return FSQuotaUtils.existQuotaInfoHasScanComplete0(bucketName, objName, Long.parseLong(meta.stamp), Collections.singletonList(0), Collections.singletonList(0), tuple2)
                                            .flatMap(t2 -> {
                                                if (t2.var1) {
                                                    updateDirStr[0] = Json.encode(t2.var2);
                                                }
                                                return Mono.just(meta);
                                            });
                                }
                            }
                            return Mono.just(meta);
                        })
                        .doOnNext(meta -> {
                            // 只有真删的时候进行worm保护并且不保护deleteMarker
                            if (!meta.deleteMarker && (versionId != null || (!"Enabled".equals(msg.get("status"))))) {
                                if (StringUtils.isNotEmpty(meta.sysMetaData)) {
                                    Map<String, String> map = Json.decodeValue(meta.sysMetaData, new TypeReference<Map<String, String>>() {
                                    });
                                    boolean isNoSyncObject = map.containsKey(NO_SYNCHRONIZATION_KEY) && NO_SYNCHRONIZATION_VALUE.equals(map.get(NO_SYNCHRONIZATION_KEY));
                                    if (isNoSyncObject && request.headers().contains(CLUSTER_ALIVE_HEADER)) {
                                        request.response().putHeader(IFF_TAG, "1");
                                    }

                                    if (request.headers().contains(IS_SYNCING) && isNoSyncObject) {
                                        request.headers().remove(IS_SYNCING);
                                    }
                                    checkWormRetention(request, getWormExpire(meta), currentWormStamp[0]);
                                }
                            }
                        })
                        .flatMap(meta -> {
                            if (subRequest != null && StringUtils.isNotEmpty(subRequest.headers().get("trashDir"))) {
                                trashDir[0] = subRequest.headers().get("trashDir");
                            }
                            if (request.headers().contains(IS_SYNCING)) {
                                trashDir[0] = null;
                            }
                            //桶回收站功能实现
                            if (trashDir[0] != null) {
                                //判断对象是否在回收站中
                                if (objName.startsWith(trashDir[0])) {
                                    if (!objName.equals(trashDir[0])) {
                                        //判断是否为彻底删除回收站中的对象,只有recoverObject为true时才彻底删除
                                        if ("true".equals(request.getHeader("recoverObject"))) {
                                            return recoverObject(request, trashDir[0])
                                                    .doOnNext(res -> {
                                                        trashCopyResult[0] = true;
                                                    }).flatMap(b -> Mono.just(meta));
                                        } else {
                                            return Mono.just(meta);
                                        }
                                    } else {
                                        return Mono.just(meta);
                                    }
                                } else {
                                    //移动到回收站
                                    return move(request, bucketName, trashDir[0] + objName, objName, versionId)
                                            .doOnNext(res -> {
                                                trashCopyResult[0] = true;
                                            }).flatMap(b -> Mono.just(meta));
                                }
                            } else {
                                return Mono.just(meta);
                            }
                        })
                        .flatMap(meta -> {
                            {
                                if (meta.inode > 0) {
                                    return Node.getInstance().deleteInode(meta.inode, bucketName, meta.key)
                                            .flatMap(i -> {
                                                Mono<Boolean> esMono = Mono.just(true);
                                                boolean res = ERROR_INODE.getLinkN() != i.getLinkN();
                                                if (res) {
                                                    if (ES_ON.equals(msg.get("mda"))) {
                                                        esMono = EsMetaTask.delEsMeta(i.clone().setObjName(meta.key), meta, !deleteESSource);
                                                    }
                                                }
                                                return esMono.flatMap(b -> InodeUtils.updateSpeciDirTime(dirInodes[0], meta.key, bucketName))
                                                        .map(f -> res);
                                            });
                                }
                                //带有版本号删除(真删)
                                if (versionId != null) {
                                    if (meta.deleteMarker) {
                                        String[] versionNum = new String[]{null};
                                        //删除多版本删除标记 -- 双写流程 -- 写新的vnode
                                        return VersionUtil.getVersionNum(bucketName, objName)
                                                .doOnNext(version -> versionNum[0] = version)
                                                .flatMap(b -> InodeUtils.updateLinkN(true, bucketName, meta, objName, tuple2s))
                                                .flatMap(b -> {
                                                    if (migrateVnode != null) {
                                                        return bucketPool.mapToNodeInfo(migrateVnode)
                                                                .flatMap(migrateNodeList -> setDelMark(new DelMarkParams(bucketName, objName, versionId, meta, migrateNodeList, versionStatus[0],
                                                                        versionNum[0]).migrate(true).request(request), currentSnapshotMark[0], snapshotLink[0]));
                                                    }
                                                    return Mono.just(1);
                                                })
                                                .doOnNext(b -> {
                                                    if (b == 0) {
                                                        throw new MsException(DEL_OBJ_ERR, "delete object from migrate internal error");
                                                    } else {
                                                        if (subRequest != null) {
                                                            subRequest.headers().set(X_AMX_VERSION_ID, versionId)
                                                                    .set(X_AMX_DELETE_MARKER, "true");
                                                        } else {
                                                            request.response().putHeader(X_AMX_VERSION_ID, versionId)
                                                                    .putHeader(X_AMX_DELETE_MARKER, "true");
                                                        }
                                                    }
                                                })
                                                // 写当前的vnode
                                                .flatMap(b -> bucketPool.mapToNodeInfo(bucketVnode))
                                                .flatMap(bucketVnodeList -> setDelMark(new DelMarkParams(bucketName, objName, versionId, meta, bucketVnodeList, versionStatus[0], versionNum[0]).request(request), currentSnapshotMark[0], snapshotLink[0]))
                                                .doOnNext(b -> {
                                                    if (b == 0) {
                                                        throw new MsException(DEL_OBJ_ERR, "delete object internal error");
                                                    } else {
                                                        if (subRequest != null) {
                                                            subRequest.headers().set(X_AMX_VERSION_ID, versionId)
                                                                    .set(X_AMX_DELETE_MARKER, "true");
                                                        } else {
                                                            request.response().putHeader(X_AMX_VERSION_ID, versionId)
                                                                    .putHeader(X_AMX_DELETE_MARKER, "true");
                                                        }
                                                    }
                                                })
                                                .flatMap(b -> tuple2s[0].var1 ? Node.getInstance().updateInodeLinkN(tuple2s[0].var2.getNodeId(), bucketName, 0) : Mono.just(true));

                                    } else {
                                        //指定版本真删
                                        // --- 双写流程
                                        String[] versionNum = new String[]{null};
                                        boolean[] syncDelete = new boolean[]{true};
                                        return VersionUtil.getVersionNum(bucketName, objName)
                                                .doOnNext(version -> versionNum[0] = version)
                                                .flatMap(b -> InodeUtils.updateLinkN(true, bucketName, meta, objName, tuple2s))
                                                .flatMap(b -> {
                                                    if (migrateVnode != null) {
                                                        return bucketPool.mapToNodeInfo(migrateVnode)
                                                                .flatMap(migrateNodeList -> setDelMark(new DelMarkParams(bucketName, objName, versionId, meta, migrateNodeList, versionStatus[0],
                                                                        versionNum[0]).migrate(true).request(request), currentSnapshotMark[0], snapshotLink[0]));
                                                    }
                                                    return Mono.just(1);
                                                })
                                                .doOnNext(b -> {
                                                    if (b == 0) {
                                                        throw new MsException(DEL_OBJ_ERR, "delete migrate " + migrateVnode + " object internal error");
                                                    }
                                                })
                                                .doOnNext(r -> syncDelete[0] = syncDelete[0] && r == 1)
                                                .flatMap(b -> bucketPool.mapToNodeInfo(bucketVnode))
                                                .flatMap(bucketNodeList -> setDelMark(new DelMarkParams(bucketName, objName, versionId, meta, bucketNodeList, versionStatus[0], versionNum[0]).request(request), currentSnapshotMark[0], snapshotLink[0]))
                                                .doOnNext(b -> {
                                                    if (b == 0) {
                                                        throw new MsException(DEL_OBJ_ERR, "delete object internal error");
                                                    }
                                                })
                                                .doOnNext(r -> syncDelete[0] = syncDelete[0] && r == 1)
                                                .flatMap(b -> tuple2s[0].var1 ? Node.getInstance().updateInodeLinkN(tuple2s[0].var2.getNodeId(), bucketName, 0) :
                                                        (syncDelete[0] ? delVersionObj(request0, needDeleteFile, msg, metaData, subRequest, currentSnapshotMark[0]) : Mono.just(true)))
                                                .flatMap(f -> {
                                                    if (!syncDelete[0]) {
                                                        if (!"off".equals(msg.get("mda"))) {
                                                            return EsMetaTask.putOrDelS3EsMeta(requestUserId, bucketName, objName, versionId, metaData[0].endIndex + 1, request, true, !deleteESSource);
                                                        }
                                                    }
                                                    return Mono.just(true);
                                                })
                                                .flatMap(b -> {
                                                    if (subRequest != null) {
                                                        subRequest.headers().set(X_AMX_VERSION_ID, versionId);
                                                    } else {
                                                        request.response().putHeader(X_AMX_VERSION_ID, versionId);
                                                    }
                                                    return Mono.just(true);
                                                });
                                    }
                                }
                                //不带版本号删除（假删）
                                else {
                                    //多版本开启
                                    if ("Enabled".equals(msg.get("status"))) {
                                        //添加多版本删除标记
                                        return addVersionDeleteMarker(request0, bucketVnode, migrateVnode, msg, meta, subRequest, currentSnapshotMark[0], snapshotLink[0]);
                                    }
                                    //多版本暂停
                                    else if ("Suspended".equals(msg.get("status"))) {

                                        List<String> nullNeedDeleteFile = new LinkedList<>();
                                        return bucketPool.mapToNodeInfo(bucketVnode)
                                                .flatMap(list -> getObjectMetaVersionUnlimited(bucketName, objName, "null", list, request, currentSnapshotMark[0], snapshotLink[0]))
                                                .doOnNext(nullMeta -> {
                                                    if (nullMeta.equals(ERROR_META)) {
                                                        throw new MsException(UNKNOWN_ERROR, "Get Object Meta Data fail");
                                                    }
                                                })
                                                .flatMap(nullMeta -> InodeUtils.updateLinkN(nullMeta, bucketName, nullMeta, objName, tuple2s))
                                                .flatMap(nullMeta -> addVersionDeleteMarker(request0, bucketVnode, migrateVnode, msg, meta, subRequest, currentSnapshotMark[0], snapshotLink[0])
                                                        .flatMap(b -> Mono.just(nullMeta)))
                                                .flatMap(nullMeta -> {
                                                    if (!"null".equals(meta.versionId)) {
                                                        return Mono.just(true);
                                                    }
                                                    //删除版本号为"null"的对象
                                                    if (nullMeta.isAvailable()) {
                                                        if (nullMeta.deleteMarker) {
                                                            return Mono.just(true);
                                                        }
                                                        if (nullMeta.partUploadId == null) {
                                                            if (nullMeta.isCurrentSnapshotObject(currentSnapshotMark[0])) {
                                                                nullNeedDeleteFile.add(nullMeta.getFileName());
                                                            }
                                                        } else {
                                                            for (PartInfo partInfo : nullMeta.partInfos) {
                                                                if (partInfo.isCurrentSnapshotObject(currentSnapshotMark[0])) {
                                                                    nullNeedDeleteFile.add(partInfo.fileName);
                                                                }
                                                            }
                                                        }
                                                        metaData[0] = nullMeta;
                                                        return Mono.just(tuple2s[0])
                                                                .flatMap(tuple2 -> tuple2.var1 ? Node.getInstance().updateInodeLinkN(tuple2s[0].var2.getNodeId(), bucketName, 0) : delVersionObj(request0,
                                                                        nullNeedDeleteFile, msg, metaData,
                                                                        subRequest, currentSnapshotMark[0]))
                                                                .flatMap(b -> {
                                                                    if (subRequest != null) {
                                                                        subRequest.headers().set(X_AMX_VERSION_ID, "null");
                                                                    } else {
                                                                        request.response().putHeader(X_AMX_VERSION_ID, "null");
                                                                    }
                                                                    return Mono.just(true);
                                                                });
                                                    } else {
                                                        return Mono.just(true);
                                                    }
                                                });
                                    }
                                    //未开启多版本
                                    else {
                                        String[] versionNum = new String[]{null};
                                        boolean[] syncDelete = new boolean[]{true};
                                        return VersionUtil.getVersionNum(bucketName, objName)
                                                .doOnNext(version -> versionNum[0] = version)
                                                .flatMap(b -> InodeUtils.updateLinkN(true, bucketName, meta, objName, tuple2s))
                                                .flatMap(b -> {
                                                    if (migrateVnode != null) {
                                                        return bucketPool.mapToNodeInfo(migrateVnode)
                                                                .flatMap(migrateVnodeList -> setDelMark(new DelMarkParams(bucketName, objName, "null", meta, migrateVnodeList, versionStatus[0],
                                                                        versionNum[0]).migrate(true).request(request), currentSnapshotMark[0], snapshotLink[0]));
                                                    }
                                                    return Mono.just(1);
                                                })
                                                .doOnNext(b -> {
                                                    if (b == 0) {
                                                        throw new MsException(DEL_OBJ_ERR, "delete migrate " + migrateVnode + " object internal error");
                                                    }
                                                })
                                                .doOnNext(r -> syncDelete[0] = syncDelete[0] && r == 1)
                                                .flatMap(b -> bucketPool.mapToNodeInfo(bucketVnode))
                                                .flatMap(bucketVnodeList -> {
                                                    if (StringUtils.isNotBlank(updateDirStr[0])) {
                                                        DelMarkParams delParams = new DelMarkParams(bucketName, objName, "null", meta, bucketVnodeList, versionStatus[0], versionNum[0]).request(request);
                                                        delParams.updateQuotaKeyStr(updateDirStr[0]);
                                                        return setDelMark(delParams, currentSnapshotMark[0], snapshotLink[0]);
                                                    }
                                                    return setDelMark(new DelMarkParams(bucketName, objName, "null", meta, bucketVnodeList, versionStatus[0], versionNum[0]).request(request), currentSnapshotMark[0], snapshotLink[0]);
                                                })
                                                .doOnNext(b -> {
                                                    if (b == 0) {
                                                        throw new MsException(DEL_OBJ_ERR, "delete object internal error");
                                                    }
                                                })
                                                .doOnNext(r -> syncDelete[0] = syncDelete[0] && r == 1)
                                                .doOnNext(b -> {
                                                    if (b > 0) {
                                                        //检测原对象是否删除成功
                                                        trashCopyResult[1] = true;
                                                    }
                                                })
                                                .flatMap(b -> (syncDelete[0] ? delVersionObj(request0, needDeleteFile, msg, metaData, subRequest, currentSnapshotMark[0]) : Mono.just(true)))
                                                .flatMap(f -> {
                                                    if (!syncDelete[0]) {
                                                        if (!"off".equals(msg.get("mda"))) {
                                                            return EsMetaTask.putOrDelS3EsMeta(requestUserId, bucketName, objName, versionId, metaData[0].endIndex + 1, request, true, !deleteESSource);
                                                        }
                                                    }
                                                    return Mono.just(true);
                                                })
                                                .flatMap(b -> {
                                                    if (subRequest != null) {
                                                        subRequest.headers().set(X_AMX_VERSION_ID, "null");
                                                    } else {
                                                        request.response().putHeader(X_AMX_VERSION_ID, "null");
                                                    }
                                                    return Mono.just(true);
                                                });
                                    }
                                }
                            }
                        })
                        .doOnError(res -> {
                            //判断复制到回收站中对象是否复制成功，若成功则删除
                            if (trashCopyResult[0] && !trashCopyResult[1]) {
                                if (objName.startsWith(trashDir[0])) {
                                    String newObjectName = objName.substring(trashDir[0].length());
                                    delTrashObj(newObjectName, bucketName, requestUserId, msg, currentSnapshotMark[0], snapshotLink[0]).subscribe(b -> {
                                        if (b) {
                                            delObjLogger.info("roll back {} {} success", bucketName, newObjectName);
                                        }
                                    }, e -> {
                                        logger.error("roll back fail. {}", e);
                                    });
                                } else {
                                    delTrashObj(trashDir[0] + objName, bucketName, requestUserId, msg, currentSnapshotMark[0], snapshotLink[0]).subscribe(b -> {
                                        if (b) {
                                            delObjLogger.info("roll back {} {} success", bucketName, trashDir[0] + objName);
                                        }
                                    }, e -> {
                                        logger.error("roll back fail. {}", e);
                                    });
                                }
                            }
                        }));
    }

    private static void checkVersionStatus(MsHttpRequest request, String status) {
        if ("NULL".equals(status) && request.headers().contains(NEW_VERSION_ID) && request.headers().contains(IS_SYNCING)
                && StringUtils.isNotEmpty(request.headers().get(NEW_VERSION_ID))) {
            throw new MsException(UNKNOWN_ERROR, "bucket version status is NULL but object has versionId," +
                    "object : " + request.getObjectName() + ", versionId: " + request.headers().get(NEW_VERSION_ID));
        }
    }

    /**
     * 指定版本下载对象
     *
     * @param request 下载请求
     * @return 返回结果状态
     */
    public int getObjectVersion(MsHttpRequest request) {
        return getObject(request);
    }

    /**
     * 下载对象
     *
     * @param request 下载请求
     * @return 返回结果状态
     */
    public int getObject(MsHttpRequest request) {
        final String bucketName = request.getBucketName();
        final String objName = request.getObjectName();
        final String[] versionId = new String[1];
        final int[] policy = new int[1];
        AtomicBoolean deleteSource = new AtomicBoolean(false);
        AtomicBoolean backSource = new AtomicBoolean(false);

        final String method = request.getParam(VERSIONID) == null ? "GetObject" : "GetObjectVersion";
        final String userName = request.getUserName() == null ? "" : request.getUserName();
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_DOWNLOAD_OBJECT, 0);
        msg.put("bucket", request.getBucketName())
                .put("userId", request.getUserId())
                .put("objName", objName);
        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        StoragePool[] dataPool = new StoragePool[]{null};
        boolean[] hasStartFS = {false};
        int[] startFsType = {0};
        pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .doOnNext(bucketInfo -> {
                    throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "No such bucket. bucket name :" + bucketName));
                    CheckUtils.hasStartFS(bucketInfo, hasStartFS);
                    startFsType[0] = ACLUtils.setFsProtoType(bucketInfo);
                    regionCheck(bucketInfo.get(REGION_NAME));
                    siteCheck(bucketInfo);
                    referCheck(bucketInfo, request);
                    msg.put("bucketUserId", bucketInfo.get(BUCKET_USER_ID));
                    if (StringUtils.isNotEmpty(request.getParam(VERSIONID))) {
                        versionId[0] = request.getParam(VERSIONID);
                    } else {
                        versionId[0] = bucketInfo.containsKey(BUCKET_VERSION_STATUS) ? "" : "null";
                    }
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    if (bucketInfo.containsKey(DELETE_SOURCE_SWITCH)) {
                        deleteSource.set(true);
                    }
                    if ("on".equals(bucketInfo.get(BACK_SOURCE))) {
                        backSource.set(true);
                    }
                })
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyGetCheckResult(request, bucketName,
                        objName, versionId[0], method).zipWith(Mono.just(bucketInfo))).doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo ->
                        bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(bucketName, objName))
                                .flatMap(list -> getObjectMetaVersionUnlimited(bucketName, objName, versionId[0], list, request, currentSnapshotMark[0], snapshotLink[0]))
                )
                .doOnNext(metaData -> checkMetaData(objName, metaData, request.getParam(VERSIONID), currentSnapshotMark[0]))
                .doOnNext(metaData -> {
                    String header = request.getHeader("check-syncstamp");
                    if (StringUtils.isNotBlank(header)) {
                        if (!metaData.syncStamp.equals(header)) {
                            throw new RuntimeException("not match");
                        }
                    }
                })
                .flatMap(metaData -> {
                    dataPool[0] = StoragePoolFactory.getStoragePool(metaData);


                    return dataPool[0].mapToNodeInfo(dataPool[0].getObjectVnodeId(metaData)).zipWith(Mono.just(metaData));
                })
                .flatMap(tuple2 -> {
                    return Mono.just(policy[0] == 0 && !request.headers().contains(IS_SYNCING))
                            .flatMap(needJudge -> {
                                if (needJudge) {
                                    MetaData metaData = tuple2.getT2();
                                    String userId = msg.get("userId");
                                    if (hasStartFS[0]) {
                                        S3ACL.judgeMetaFSACL(userId, metaData, 6, startFsType[0]);
                                        checkFsObjectReadAcl(msg, metaData);
                                        // 判断目录权限以及文件本身的权限
                                        return S3ACL.checkFSACL(objName, bucketName, userId, null, 6, false, startFsType[0], 0)
                                                .map(b -> tuple2);
                                    } else {
                                        checkObjectReadAcl(msg, metaData);
                                    }
                                }

                                return Mono.just(tuple2);
                            });
                })
                .flatMapMany(tuple2 -> {
                    MetaData metaData = tuple2.getT2();
                    Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                    });

                    // 对象锁定系统元数据
                    String expiration = sysMetaMap.remove(EXPIRATION);
                    if (!StringUtils.isEmpty(expiration)) {
                        sysMetaMap.put("x-amz-object-lock-retain-until-date", MsDateUtils.stampToISO8601(expiration));
                        sysMetaMap.put("x-amz-object-lock-mode", sysMetaMap.getOrDefault("mode", "COMPLIANCE"));
                    }

                    long total = metaData.endIndex - metaData.startIndex + 1;
                    long startIndex = 0;
                    long endIndex = metaData.endIndex;
                    //range处理
                    if (request.headers().contains(RANGE)) {
                        String range = request.headers().get(RANGE);
                        LongLongTuple t = dealRange(range, total);
                        startIndex = t.var1;
                        endIndex = t.var2;

//                        if (endIndex - startIndex <= 0) {
//                            total = 0;
//                        }
                        request.response()
                                .putHeader("Accept-Ranges", "bytes")
                                .putHeader("Content-Range", "bytes " + startIndex + '-' + endIndex + '/' + total)
                                .setStatusCode(206);
                    }
                    //添加请求头
                    addPublicHeaders(request, getRequestId())
                            .putHeader(CONTENT_LENGTH, String.valueOf(endIndex - startIndex < 0 ? 0 : endIndex - startIndex + 1))
                            .putHeader(ETAG, '"' + sysMetaMap.get(ETAG) + '"')
                            .putHeader(CONTENT_TYPE, sysMetaMap.get(CONTENT_TYPE))
                            .putHeader(LAST_MODIFY, sysMetaMap.get(LAST_MODIFY))
                            .putHeader(X_AMX_VERSION_ID, metaData.versionId);
                    CryptoUtils.addCryptoResponse(request, metaData.getCrypto());
                    String userMetaData = metaData.userMetaData;
                    Map<String, String> userMetaDataMap = Json.decodeValue(userMetaData, new TypeReference<Map<String, String>>() {
                    });
//                    sysMetaMap.remove(CONTENT_ENCODING);//SERVER-1420
                    userMetaDataMap.putAll(sysMetaMap);
                    userMetaDataMap.forEach((key, value) -> {
                        final String lowerKey = key.toLowerCase();
                        if (lowerKey.startsWith("x-amz-object-lock") || lowerKey.startsWith(USER_META) || SPECIAL_HEADER.contains(lowerKey.hashCode())) {
                            request.response().putHeader(UrlEncoder.encode(key, "UTF-8"),
                                    new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1));
                        }
                    });

                    dealWithSixRequestParameter(request);

                    if (total == 0) {
                        MonoProcessor<byte[]> res = MonoProcessor.create();
                        res.onNext(new byte[0]);
                        return res;
                    }

                    if (metaData.aggregationKey != null && metaData.crypto == null && metaData.aggSize > 0) {
                        long offset = metaData.offset + startIndex;
                        long size = endIndex - startIndex + 1;
                        return ReadObjClient.readObj(0, metaData.storage, bucketName, metaData.fileName, offset, size, metaData.aggSize, false)
                                .map(tuple21 -> tuple21.var2);
                    }

                    return ErasureClient.getObject(dataPool[0], metaData, startIndex, endIndex, tuple2.getT1(), streamController, request, null);
                })
                .subscribe(new DownLoadSubscriber(request, request.getContext(), streamController, deleteSource, backSource));
        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 获取指定版本的object的对象元数据
     *
     * @param request 请求参数
     * @return 对象的元数据信息，不包括对象内容
     */
    public int headObjectVersion(MsHttpRequest request) {
        return headObject(request);
    }

    /**
     * 获取指定字段的object的对象元数据
     *
     * @param request 请求参数
     * @return 对象的元数据信息，不包括对象内容
     */
    public int headObjectPartNumber(MsHttpRequest request) {
        return headObject(request);
    }

    /**
     * 获取指定字段和版本的object的对象元数据
     *
     * @param request 请求参数
     * @return 对象的元数据信息，不包括对象内容
     */
    public int headObjectPartNumberAndVersionId(MsHttpRequest request) {
        return headObject(request);
    }

    /**
     * 获取对象元数据接口
     *
     * @param request 用户请求
     * @return 返回结果
     */
    public int headObject(MsHttpRequest request) {
        String bucketName = request.getBucketName();
        String key = request.getObjectName();
        String userId = request.getUserId();
        String requestId = getRequestId();
        final String[] versionId = new String[1];
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);

        final int[] policy = new int[1];

        final String method = request.getParam(VERSIONID) == null ? "HeadObject" : "HeadObjectVersion";

        final String userName = request.getUserName() == null ? "" : request.getUserName();
        //向其他节点发送消息的socket消息对象
        SocketReqMsg socketReqMsg = new SocketReqMsg(MSG_TYPE_GET_OBJECTINFO, 0);
        socketReqMsg.put(new HashMap<String, String>() {{
            put("bucket", bucketName);
            put("objName", key);
            put("userId", userId);
        }});
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        HashMap<String, String> strategyMap = new HashMap<>();
        boolean[] hasStartFS = {false};
        int[] startFsType = {0};
        AtomicBoolean needCheck = new AtomicBoolean(true);
        final boolean[] allSiteCheck = {false};
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, key, method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> {
                    CheckUtils.hasStartFS(bucketInfo, hasStartFS);
                    startFsType[0] = ACLUtils.setFsProtoType(bucketInfo);
                    regionCheck(bucketInfo.get(REGION_NAME));
                    siteCheck(bucketInfo);
                    String datasync = bucketInfo.get(DATA_SYNC_SWITCH);
                    allSiteCheck[0] = isMultiAliveStarted && request.headers().contains("all-cluster") && "1".equals(request.getHeader("all-cluster")) && (SWITCH_ON.equals(datasync) || SWITCH_SUSPEND.equals(datasync));
                    socketReqMsg.put("bucketUserId", bucketInfo.get(BUCKET_USER_ID));
                    versionId[0] = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                            : bucketInfo.isEmpty() || !bucketInfo.containsKey(BUCKET_VERSION_STATUS) ? "null" : "";
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    return Mono.just(storagePool.getBucketVnodeId(bucketName, key));
                })
                .flatMap(bucketVnode -> storagePool.mapToNodeInfo(bucketVnode))
                .flatMap(list -> {
                    if (allSiteCheck[0]) {
                        return headObjectFormAllSite(bucketName, key, versionId[0], list, request, currentSnapshotMark[0], snapshotLink[0], needCheck);
                    } else {
                        return getObjectMetaVersionUnlimited(bucketName, key, versionId[0], list, request, currentSnapshotMark[0], snapshotLink[0]);
                    }
                })
                .doOnNext(meta -> {
                    // 处理后台校验的一些特殊头部字段
                    if ("1".equals(request.getHeader("check-syncstamp")) && StringUtils.isNotBlank(meta.stamp)) {
                        request.response().putHeader("check-stamp", meta.stamp);
                    }
                    if ("1".equals(request.getHeader("check-syncstamp")) && StringUtils.isNotBlank(meta.syncStamp)) {
                        request.response().putHeader("check-syncstamp", meta.syncStamp);
                    }
                })
                .flatMap(metaData -> {
                    String header = request.getHeader("check-recordKey");
                    if (!allSiteCheck[0] && StringUtils.isNotBlank(header)) {
                        return DataSynChecker.hasUnSyncedData(bucketName, Integer.parseInt(request.getHeader("check-recordIndex")), false, header, false, false)
                                .map(res -> {
                                    request.response().putHeader("check-recordKey", res ? "1" : "0");
                                    return metaData;
                                });
                    }
                    return Mono.just(metaData);
                })
                .doOnNext(meta -> {
                    if (needCheck.get()) checkMetaData(key, meta, request.getParam(VERSIONID), currentSnapshotMark[0]);
                })
                .zipWith(pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, "storage_strategy"))
                .flatMap(tuple2 -> {
                    return Mono.just(policy[0] == 0 && !request.headers().contains(IS_SYNCING) && !allSiteCheck[0])
                            .flatMap(needJudge -> {
                                MetaData metaData = tuple2.getT1();
                                if (needJudge) {
                                    if (hasStartFS[0]) {
                                        checkFsObjectReadAcl(socketReqMsg, metaData);
                                        S3ACL.judgeMetaFSACL(userId, metaData, 3, startFsType[0]);
                                        return S3ACL.checkFSACL(key, bucketName, userId, null, 3, false, startFsType[0], 0)
                                                .map(b -> tuple2);
                                    } else {
                                        checkObjectReadAcl(socketReqMsg, metaData);
                                    }
                                }

                                return Mono.just(tuple2);
                            });
                })
                .subscribe(tuple2 -> {
                    MetaData metaData = tuple2.getT1();
                    try {
                        Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                        });

                        // 对象锁定系统元数据
                        String expiration = sysMetaMap.remove(EXPIRATION);
                        if (!StringUtils.isEmpty(expiration)) {
                            sysMetaMap.put("x-amz-object-lock-retain-until-date", MsDateUtils.stampToISO8601(expiration));
                            sysMetaMap.put("x-amz-object-lock-mode", sysMetaMap.getOrDefault("mode", "COMPLIANCE"));
                        }

                        String userMetaData = metaData.userMetaData;
                        Map<String, String> userMetaDataMap = Json.decodeValue(userMetaData, new TypeReference<Map<String, String>>() {
                        });
                        userMetaDataMap.putAll(sysMetaMap);
                        userMetaDataMap.forEach((key_, value) -> {
                            final String lowerKey = key_.toLowerCase();
                            if (lowerKey.startsWith("x-amz-object-lock") || lowerKey.startsWith(USER_META) || SPECIAL_HEADER.contains(lowerKey.hashCode())) {
                                request.response().putHeader(UrlEncoder.encode(key_, "UTF-8"),
                                        new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1));
                            }
                        });
                        if (request.headers().contains(IS_SYNCING) && StringUtils.isNotEmpty(metaData.partUploadId)) {
                            request.response().putHeader("uploadId", metaData.partUploadId)
                                    .putHeader(SYNC_STORAGE, metaData.storage);
                        }
                        //如果content-length和Content-Length都没有，那么直接设置endIndex+1为响应content-length
                        String contentLength = Objects.nonNull(sysMetaMap.get(CONTENT_LENGTH)) ?
                                sysMetaMap.get(CONTENT_LENGTH) : Objects.nonNull(sysMetaMap.get(CONTENT_LENGTH.toLowerCase())) ?
                                sysMetaMap.get(CONTENT_LENGTH.toLowerCase()) : String.valueOf(metaData.getEndIndex() + 1);
                        HttpServerResponse response = addPublicHeaders(request, requestId)
                                .putHeader(X_AMX_VERSION_ID, metaData.versionId)
                                .putHeader("Last-Modified", sysMetaMap.get(LAST_MODIFY))
                                .putHeader(ETAG, allSiteCheck[0] ? sysMetaMap.get(ETAG) : '"' + sysMetaMap.get(ETAG) + '"')
                                .putHeader(X_AMX_STORAGE_STRATEGY, allSiteCheck[0] ? sysMetaMap.getOrDefault(X_AMX_STORAGE_STRATEGY, null) : getObjectStorageStrategy(tuple2.getT2(), metaData))
                                .putHeader(CONTENT_LENGTH, contentLength)
                                .setStatusCode(200);
                        CryptoUtils.addCryptoResponse(request, metaData.getCrypto());
                        if (isAppendableObject(metaData)) {
                            response.putHeader(NEXT_APPEND_POSITION, (metaData.getEndIndex() + 1) + "");
                            response.putHeader("appendable", "true");
                        }
                        addAllowHeader(response).end();
                    } catch (Exception e) {
                        logger.error(e);
                        dealException(request, e);
                    }
                }, e -> headException(request, e));
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }

    private Mono<MetaData> headObjectFormAllSite(String bucketName, String key, String versionId, List<Tuple3<String, String, String>> list, MsHttpRequest request, String currentSnapshotMark, String snapshotLink, AtomicBoolean needCheck) {
        MetaData[] metaData = new MetaData[1];
        return Flux.fromIterable(INDEX_NAME_MAP.keySet())
                .flatMap(index -> {
                    if (LOCAL_CLUSTER_INDEX.equals(index)) {
                        return getObjectMetaVersionUnlimited(bucketName, key, versionId, list, request, currentSnapshotMark, snapshotLink)
                                .doOnNext(meta -> metaData[0] = meta)
                                .flatMap(meta -> {
                                    if (meta.equals(ERROR_META)
                                            || meta.equals(NOT_FOUND_META)
                                            || meta.deleteMark
                                            || (StringUtils.isEmpty(versionId) && meta.deleteMarker)
                                            || meta.isUnView(currentSnapshotMark)
                                            || (StringUtils.isNotEmpty(versionId) && meta.deleteMarker)) {
                                        return Mono.just(new Tuple3<>(false, index, meta));
                                    }

                                    return pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, "storage_strategy")
                                            .flatMap(strategy -> {
                                                String strategyStr = getObjectStorageStrategy(strategy, meta);
                                                if (StringUtils.isNotEmpty(strategyStr)) {
                                                    Map<String, String> sysMetaMap = Json.decodeValue(meta.sysMetaData, new TypeReference<Map<String, String>>() {
                                                    });
                                                    sysMetaMap.put(X_AMX_STORAGE_STRATEGY, strategyStr);
                                                    meta.sysMetaData = Json.encode(sysMetaMap);
                                                    metaData[0] = meta;
                                                }
                                                return Mono.just(new Tuple3<>(true, index, metaData[0]));
                                            });
                                });
                    } else {
                        Map<String, String> headers = new HashMap<>();
                        request.headers().entries().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
                        headers.remove("all-cluster");
                        if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(index) && !EXTRA_AK_SK_MAP.containsKey(bucketName)) {
                            final Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
                            String serverAk = StringUtils.isNotBlank(bucketInfo.get("server_ak")) ? bucketInfo.get("server_ak") : "";
                            String serverSk = StringUtils.isNotBlank(bucketInfo.get("server_sk")) ? bucketInfo.get("server_sk") : "";
                            com.macrosan.utils.functional.Tuple2<String, String> tuple2 = new com.macrosan.utils.functional.Tuple2<>(serverAk, serverSk);
                            EXTRA_AK_SK_MAP.put(bucketName, tuple2);
                            logger.info("update ak {} sk {} to {}", serverAk, serverSk, bucketName);
                        }
                        return MossHttpClient.getInstance().sendRequest(index, bucketName, key, request.uri(), request.method(), headers, null, null)
                                .flatMap(tuple -> {
                                    logger.debug("{} ------------- {}", index, tuple);
                                    if (tuple.var1 == SUCCESS) {
                                        final MultiMap delegate = tuple.var3.getDelegate();
                                        final MetaData meta = new MetaData();
                                        if (!delegate.isEmpty()) {
                                            Map<String, String> headerMap = new HashMap<>();
                                            delegate.names().forEach(name -> headerMap.put(name, delegate.get(name)));
                                            meta.sysMetaData = Json.encode(headerMap);
                                        }
                                        return Mono.just(new Tuple3<>(true, index, meta));
                                    } else if (tuple.var1 == NOT_FOUND || tuple.var1 == NO_SUCH_BUCKET) {
                                        return Mono.just(new Tuple3<>(false, index, new MetaData()));
                                    }
                                    return Mono.error(new MsException(tuple.var1, "head object error at cluster " + INDEX_NAME_MAP.get(index)));
                                });
                    }
                })
                .collectList()
                .flatMap(results -> {
                    if (results.isEmpty()) {
                        return Mono.just(metaData[0]);
                    }

                    // 只取 SUCCESS 的 metaData
                    List<Tuple3<Boolean, Integer, MetaData>> successList = results.stream()
                            .filter(t -> t.var1)
                            .collect(Collectors.toList());

                    if (successList.isEmpty()) {
                        return Mono.just(metaData[0]);
                    }

                    // 拼接成功站点
                    String clusters = successList.stream()
                            .map(t -> INDEX_NAME_MAP.get(t.var2))
                            .collect(Collectors.joining(","));
                    request.response().putHeader("save-clusters", clusters);

                    // 返回任意一个成功的 MetaData（比如第一个）
                    MetaData meta = successList.get(0).var3;
                    needCheck.compareAndSet(true, false);
                    return Mono.just(meta);
                });

    }

    private Semaphore semaphore = new Semaphore(1000, true);
    private ConcurrentSkipListMap<Long, MsHttpRequest> listMap = new ConcurrentSkipListMap<>();
    private AtomicLong listID = new AtomicLong();
    private UnicastProcessor<MsHttpRequest> processor = UnicastProcessor.create(Queues.<MsHttpRequest>unboundedMultiproducer().get());

    private void semaphoreRelease() {
        semaphore.release();
        Map.Entry<Long, MsHttpRequest> entry = listMap.pollFirstEntry();
        if (entry != null) {
            processor.onNext(entry.getValue());
        }
    }

    public int listObjects(MsHttpRequest request) {
        if (!semaphore.tryAcquire()) {
            long id = listID.incrementAndGet();
            listMap.put(id, request);
            request.addResponseCloseHandler(v -> listMap.remove(id));
            return UNKNOWN_ERROR;
        }
        return listObjectsReal(request);
    }

    /**
     * 某Bucket内的对象列表
     *
     * @param request 请求
     * @return 对象列表
     */
    @SuppressWarnings("LanguageDetectionInspection")
    public int listObjectsReal(MsHttpRequest request) {
        String bucketName = request.getBucketName();
        final int[] policy = new int[1];
        final String method = "ListObjects";
        int maxKeyInt;
        final String[] objectNum = {""};
        final String[] objectBytes = {""};
        final String[] ensureV2 = {""};
        final String[] realValue = {""};
        try {
            maxKeyInt = getMaxKey(request);
        } catch (Exception e) {
            semaphoreRelease();
            dealException(request, e);
            return UNKNOWN_ERROR;
        }
        String prefix = request.getParam(PREFIX, "");
        String marker = request.getParam(MARKER, "");
        String delimiter = request.getParam(DELIMITER, "");

        String listType = request.getParam(LIST_TYPE, "");
        String startAfter = request.getParam(START_AFTER, "");
        String continuationToken = request.getParam(CONTINUATION_TOKEN, "");
        String fetchOwner0 = request.getParam(FETCH_OWNER);
        String fetchOwner1 = fetchOwner0 == null ? "false" : (fetchOwner0.equalsIgnoreCase("true") ? fetchOwner0 : "false");
        boolean fetchOwner = fetchOwner1.equalsIgnoreCase("false");
        if (StringUtils.isNotBlank(continuationToken)) {
            if (continuationToken.contains(" ")) {
                continuationToken = continuationToken.replace(" ", "+");
            }
        }

        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);

        ListBucketResult listBucketResult = new ListBucketResult().setDelimiter(delimiter)
                .setMarker(marker)
                .setName(bucketName)
                .setMaxKeys(maxKeyInt)
                .setPrefix(prefix);

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("bucket", bucketName)
                .put("maxKeys", String.valueOf(maxKeyInt))
                .put("prefix", prefix)
                .put("marker", marker)
                .put("delimiter", delimiter);

        if (request.params().contains(LIST_TYPE)) {
            if (request.getParam(LIST_TYPE).equals(LIST_TYPE_VER)) {
                listBucketResult.setMarker(null);
                listBucketResult.setStartAfter(startAfter);
                listBucketResult.setFetchOwner(fetchOwner);
                listBucketResult.setListType(listType);
                listBucketResult.setContinuationToken(continuationToken);
                msg.put("marker", startAfter);
                ensureV2[0] = LIST_TYPE_VER;
            } else {
                throw new MsException(INVALID_ARGUMENT, "listType must equal 2.");
            }
        } else {
            if (request.params().contains(START_AFTER) || request.params().contains(FETCH_OWNER) || request.params().contains(CONTINUATION_TOKEN)) {
                throw new MsException(INVALID_ARGUMENT, "the param is invalid.");
            }
        }

        Disposable[] disposables = new Disposable[3];

        AtomicBoolean isFs = new AtomicBoolean(false);
        int[] startFsType = {0};
        List<String> updateDirList = new LinkedList<>();
        disposables[0] = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName)))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> {
                    if (listType.equals(LIST_TYPE_VER) && !listBucketResult.getContinuationToken().isEmpty()) {
                        byte[] decoded = Base64.getDecoder().decode(listBucketResult.getContinuationToken());
                        byte[] nextKey = CryptoUtils.decrypt(CryptoAlgorithm.AES256.name(), DEFAULT_AES256_SECRET_KEY, decoded);
                        msg.put("continuationToken", new String(nextKey));
                        realValue[0] = new String(nextKey);
                        return Mono.just(bucketInfo);
                    }
                    return Mono.just(bucketInfo);
                })
                .flatMap(bucketInfo -> {
                    if (bucketInfo.containsKey("fsid")) {
                        isFs.set(true);
                        startFsType[0] = ACLUtils.setFsProtoType(bucketInfo);
                        return FSQuotaUtils.existQuotaInfoS3(bucketName, prefix, System.currentTimeMillis(), bucketInfo.getOrDefault("user_id", "0"))
                                .flatMap(t2 -> {
                                    if (t2.var1) {
                                        updateDirList.addAll(t2.var2);
                                    }
                                    return Mono.just(bucketInfo);
                                });
                    }
                    return Mono.just(bucketInfo);
                })
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyListCheckResult(request, bucketName, method, maxKeyInt).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> {
                    if (policy[0] == 0) {
                        if (isFs.get()) {
                            return S3ACL.checkFSACL(prefix, bucketName, request.getUserId(), null, 16, true, startFsType[0], 0)
                                    .flatMap(b -> checkReadAclReactive(bucketInfo, request.getUserId(), bucketName, msg))
                                    .switchIfEmpty(Mono.just(bucketInfo)).map(s -> bucketInfo);
                        } else {
                            return checkReadAclReactive(bucketInfo, request.getUserId(), bucketName, msg)
                                    .switchIfEmpty(Mono.just(bucketInfo)).map(s -> bucketInfo);
                        }
                    }
                    return Mono.just(bucketInfo);
                })
                .doOnNext(bucketInfo -> {
                    // 设置桶快照相关参数
                    Optional.ofNullable(bucketInfo.get(CURRENT_SNAPSHOT_MARK)).ifPresent(s -> msg.put("currentSnapshotMark", s));
                    Optional.ofNullable(bucketInfo.get(SNAPSHOT_LINK)).ifPresent(s -> msg.put("snapshotLink", s));
                })
                .flatMap(b -> {
                    if ("true".equalsIgnoreCase(request.getIsSwift())) {
                        return ErasureClient.reduceBucketInfo(bucketName)
                                .doOnNext(bucketInfo -> {
                                    if (bucketInfo.equals(ERROR_BUCKET_INFO)) {
                                        throw new MsException(UNKNOWN_ERROR, "get bucket storage info error!");
                                    }
                                    objectNum[0] = bucketInfo.getObjectNum();
                                    objectBytes[0] = bucketInfo.getBucketStorage();
                                }).map(a -> true);
                    } else {
                        return Mono.just(true);
                    }
                })
                .flatMap(b -> Mono.just(storagePool.getBucketVnodeList(bucketName)))
                .doFinally(c -> semaphoreRelease())
                .subscribe(infoList -> {
                    ListObjectMergeChannel channel = (ListObjectMergeChannel) new ListObjectMergeChannel(bucketName, listBucketResult, storagePool, infoList, request, msg.get("currentSnapshotMark")
                            , msg.get("snapshotLink"))
                            .withBeginPrefix(prefix, marker, delimiter);
                    if (isFs.get() && !updateDirList.isEmpty()) {
                        channel.setUpdateDirList(updateDirList);
                    }
                    channel.request(msg);
                    disposables[1] = channel.response().doOnNext(b -> {
                        if (!b) {
                            throw new MsException(UNKNOWN_ERROR, "list objects error!");
                        }
                        if (Objects.nonNull(request.getMember(STORE_MANAGEMENT_HEADER))) {
                            forwardRequestListObject(request, realValue[0]).subscribe(forwardBytes -> {
                                try {
                                    ListBucketResult forwardBucketResult = null;
                                    boolean isTruncated = false;
                                    if (forwardBytes.length > 0) {
                                        Object ob = JaxbUtils.toObject(new String(forwardBytes));
                                        if (ob instanceof ListBucketResult) {
                                            forwardBucketResult = (ListBucketResult) ob;
                                        } else {
                                            logger.error("store_management listObjects fail! res is {}", JSONObject.toJSONString(ob));
                                        }
                                    }
                                    if (Objects.nonNull(forwardBucketResult)) {
                                        List<Contents> contents = listBucketResult.getContents() == null ? new ArrayList<>() : listBucketResult.getContents();
                                        List<Contents> forwardContents = forwardBucketResult.getContents() == null ? new ArrayList<>() : forwardBucketResult.getContents();
                                        List<Prefix> prefixList = listBucketResult.getPrefixlist() == null ? new ArrayList<>() : listBucketResult.getPrefixlist();
                                        List<Prefix> forwardPrefixList = forwardBucketResult.getPrefixlist() == null ? new ArrayList<>() : forwardBucketResult.getPrefixlist();
                                        contents.addAll(forwardContents);
                                        contents = contents.stream().collect(Collectors.groupingBy(Contents::getKey))
                                                .values()
                                                .stream()
                                                .map(group -> group.size() > 1 ?
                                                        group.stream().filter(con -> con.getOwner().getId().equals(request.getUserId()))
                                                                .collect(Collectors.toList()).get(0) : group.get(0))
                                                .collect(Collectors.toList());
                                        prefixList.addAll(forwardPrefixList);
                                        Set<Prefix> prefixSet = new HashSet<>(prefixList);
                                        List<Contents> contentsRes = new ArrayList<>();
                                        List<Prefix> prefixesRes = new ArrayList<>();
                                        List<Object> objectList = Stream.concat(contents.stream(), prefixSet.stream())
                                                .sorted(Comparator.comparing(o -> o instanceof Contents ? ((Contents) o).getKey() : o instanceof Prefix ? ((Prefix) o).getPrefix() : ""))
                                                .limit(maxKeyInt)
                                                .peek(obj -> {
                                                    if (obj instanceof Contents) {
                                                        contentsRes.add((Contents) obj);
                                                    } else if (obj instanceof Prefix) {
                                                        prefixesRes.add((Prefix) obj);
                                                    }
                                                }).collect(Collectors.toList());
                                        String nextMarker = "";
                                        if (prefixSet.size() + contents.size() > maxKeyInt || listBucketResult.isTruncated() || forwardBucketResult.isTruncated()) {
                                            Object o = objectList.get(objectList.size() - 1);
                                            nextMarker = o instanceof Contents ? ((Contents) o).getKey() : o instanceof Prefix ? ((Prefix) o).getPrefix() : "";
                                            isTruncated = true;
                                        }
                                        listBucketResult.setContents(contentsRes)
                                                .setPrefixlist(prefixesRes)
                                                .setNextMarker(nextMarker)
                                                .setTruncated(isTruncated);
                                    }
                                    if (forwardBucketResult == null && listBucketResult.isTruncated()) {
                                        isTruncated = true;
                                    }
                                    if (ensureV2[0].equals(LIST_TYPE_VER) && isTruncated) {
                                        createToken(listBucketResult.getNextMarker()).map(listBucketResult::setNextContinuationToken).subscribe(s -> {
                                            byte[] bytes = JaxbUtils.toByteArray(listBucketResult);
                                            addPublicHeaders(request, getRequestId())
                                                    .putHeader(CONTENT_TYPE, "application/xml")
                                                    .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                                                    .write(Buffer.buffer(bytes));
                                            addAllowHeader(request.response()).end();
                                        });
                                    } else {
                                        byte[] bytes = JaxbUtils.toByteArray(listBucketResult);
                                        addPublicHeaders(request, getRequestId())
                                                .putHeader(CONTENT_TYPE, "application/xml")
                                                .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                                                .write(Buffer.buffer(bytes));
                                        addAllowHeader(request.response()).end();
                                    }
                                } catch (Exception e) {
                                    logger.error("", e);
                                    dealException(request, e);
                                }
                            });
                        } else {
                            if (listBucketResult.isTruncated() && listType.equals(LIST_TYPE_VER)) {
                                String nextMarker = listBucketResult.getNextMarker();
                                createToken(nextMarker).map(listBucketResult::setNextContinuationToken).subscribe(s -> {
                                    byte[] bytes = JaxbUtils.toByteArray(listBucketResult);
                                    addPublicHeaders(request, getRequestId())
                                            .putHeader(CONTENT_TYPE, "application/xml")
                                            .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                                            .write(Buffer.buffer(bytes));
                                    addAllowHeader(request.response()).end();
                                });
                            } else {
                                if ("true".equalsIgnoreCase(request.getIsSwift())) {
                                    List<ObjectsSwift> list = new ArrayList<>(listBucketResult.getContents().size());
                                    ListBucketSwift listBucketSwift = new ListBucketSwift();
                                    for (Contents content : listBucketResult.getContents()) {
                                        ObjectsSwift objectsSwift = new ObjectsSwift()
                                                .setName(content.getKey())
                                                .setBytes(content.getSize())
                                                .setLast_modified(content.getLastModified())
                                                .setHash(content.getEtag());
                                        list.add(objectsSwift);
                                    }
                                    listBucketSwift.setObjectList(list);
                                    String format = request.getParam("format", "");
                                    addPublicHeaders(request, getRequestId())
                                            .putHeader(X_CONTAINER_OBJECT_COUNT, objectNum[0])
                                            .putHeader(X_CONTAINER_BYTES_USED, objectBytes[0])
                                            .setStatusCode(list.size() == 0 ? DEL_SUCCESS : SUCCESS);
                                    if (StringUtils.equalsIgnoreCase("json", format)) {
                                        byte[] bytes = new JsonArray(list).toString().getBytes();
                                        request.response().putHeader(CONTENT_TYPE, "application/json")
                                                .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                                                .write(Buffer.buffer(bytes));
                                    } else if (StringUtils.equalsIgnoreCase("xml", format)) {
                                        byte[] bytes = JaxbUtils.toByteArray(listBucketSwift);
                                        request.response().putHeader(CONTENT_TYPE, "application/xml")
                                                .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                                                .write(Buffer.buffer(bytes));
                                    } else {
                                        StringBuilder objectName = new StringBuilder(list.size());
                                        for (ObjectsSwift objectsSwift : list) {
                                            objectName.append(objectsSwift.getName()).append("\n");
                                        }
                                        byte[] bytes = objectName.toString().getBytes();
                                        request.response().putHeader(CONTENT_TYPE, "application/xml")
                                                .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                                                .write(Buffer.buffer(bytes));
                                    }
                                    addAllowHeader(request.response()).end();
                                } else {
                                    byte[] bytes = JaxbUtils.toByteArray(listBucketResult);
                                    addPublicHeaders(request, getRequestId())
                                            .putHeader(CONTENT_TYPE, "application/xml")
                                            .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                                            .write(Buffer.buffer(bytes));
                                    addAllowHeader(request.response()).end();
                                }
                            }
                        }
                    }).doOnError(e -> {
                        logger.error("", e);
                        dealException(request, e);
                    }).subscribe();

                    request.addResponseCloseHandler(v -> {
                        for (Disposable d : disposables) {
                            if (null != d) {
                                d.dispose();
                            }
                        }
                    });
                }, e -> dealException(request, e));
        return ErrorNo.SUCCESS_STATUS;
    }

    private static Mono<String> createToken(String value) {
        byte[] encrypt = CryptoUtils.encrypt(CryptoAlgorithm.AES256.name(), DEFAULT_AES256_SECRET_KEY, value.getBytes());
        return Mono.just(Base64.getEncoder().encodeToString(encrypt));
    }

    /**
     * 获取多版本对象列表
     *
     * @param request 请求参数
     * @return 结果 1表示成功
     */
    public int listObjectsVersions(MsHttpRequest request) {
        final String bucketName = request.getBucketName();
        int maxKey = getMaxKey(request);
        String prefix = request.getParam(PREFIX, "");
        String delimiter = request.getParam(DELIMITER, "");
        String keyMarker = request.getParam(KEY_MARKER, "");
        String versionIdMarker = request.getParam(VERSION_ID_MARKER, "");
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        com.macrosan.utils.functional.Tuple2<String, String> nextMarkerTuple = new com.macrosan.utils.functional.Tuple2<>("", "");
        final int[] policy = new int[1];
        final String[] realValue = {""};
        final String method = "ListObjectsVersions";
        if (StringUtils.isEmpty(keyMarker) && StringUtils.isNotEmpty(versionIdMarker)) {
            throw new MsException(VERSION_ID_MARKER_ERROR, "A version-id marker cannot be specified without a key marker.");
        }
        ListVersionsResult listVersionsRes = new ListVersionsResult()
                .setDelimiter(delimiter)
                .setKeyMarker(keyMarker)
                .setName(bucketName)
                .setMaxKeys(maxKey)
                .setPrefix(prefix);
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("bucket", listVersionsRes.getName())
                .put("maxKeys", String.valueOf(listVersionsRes.getMaxKeys()))
                .put("prefix", listVersionsRes.getPrefix())
                .put("marker", listVersionsRes.getKeyMarker())
                .put("versionIdMarker", versionIdMarker)
                .put("delimiter", listVersionsRes.getDelimiter());
        Disposable[] disposables = new Disposable[3];
        disposables[0] = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName)))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyListCheckResult(request, bucketName, method, maxKey).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> {
                    if (policy[0] == 0) {
                        if (CheckUtils.hasStartFS(bucketInfo, null)) {
                            int startFsType = ACLUtils.setFsProtoType(bucketInfo);
                            return S3ACL.checkFSACL(prefix, bucketName, request.getUserId(), null, 16, true, startFsType, 0)
                                    .flatMap(b -> checkReadAclReactive(bucketInfo, request.getUserId(), bucketName, msg))
                                    .switchIfEmpty(Mono.just(bucketInfo)).map(s -> bucketInfo);
                        } else {
                            return checkReadAclReactive(bucketInfo, request.getUserId(), bucketName, msg)
                                    .switchIfEmpty(Mono.just(bucketInfo)).map(s -> bucketInfo);
                        }
                    }
                    return Mono.just(bucketInfo);
                })
                .doOnNext(bucketInfo -> {
                    // 设置桶快照相关参数
                    Optional.ofNullable(bucketInfo.get(CURRENT_SNAPSHOT_MARK)).ifPresent(s -> msg.put("currentSnapshotMark", s));
                    Optional.ofNullable(bucketInfo.get(SNAPSHOT_LINK)).ifPresent(s -> msg.put("snapshotLink", s));
                })
                .flatMap(bucketInfo -> Mono.just(storagePool.getBucketVnodeList(bucketName)))
                .subscribe(infoList -> {
                    ListVersionsMergeChannel channel = (ListVersionsMergeChannel) new ListVersionsMergeChannel(bucketName, listVersionsRes, storagePool, infoList, request, msg.get("snapshotLink"))
                            .withBeginPrefix(prefix, keyMarker, delimiter);
                    channel.request(msg);
                    disposables[1] = channel.response()
                            .doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(UNKNOWN_ERROR, "");
                                }
                                byte[] bytes = JaxbUtils.toByteArray(listVersionsRes);
                                addPublicHeaders(request, getRequestId())
                                        .putHeader(CONTENT_TYPE, "application/xml")
                                        .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                                        .write(Buffer.buffer(bytes));
                                addAllowHeader(request.response()).end();
                            })
                            .doOnError(e -> {
                                logger.error("", e);
                                MsException.dealException(request, e);
                            }).subscribe();
                    request.addResponseCloseHandler(v -> {
                        for (Disposable d : disposables) {
                            if (null != d) {
                                d.dispose();
                            }
                        }
                    });
                }, e -> dealException(request, e));
        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 校验maxKey
     *
     * @param request 参数值
     * @return maxKey
     */
    private int getMaxKey(MsHttpRequest request) {
        final String maxUploadStr;
        if (request.getParam(MAX_KEYS) == null || StringUtils.isBlank(request.getParam(MAX_KEYS))) {
            maxUploadStr = "1000";
        } else {
            maxUploadStr = request.getParam(MAX_KEYS, "1000");
        }
        if (!PatternConst.LIST_PARTS_PATTERN.matcher(maxUploadStr).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "max_keys param error.");
        }
        int maxUpload = Integer.parseInt(maxUploadStr);
        if (maxUpload > 1000 || maxUpload <= 0) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "max_keys param error, must be int in [1, 1000]");
        }
        return maxUpload;
    }

    /**
     * head请求的异常处理
     *
     * @param request 请求对象
     * @param e       异常类型
     */
    private void headException(MsHttpRequest request, Throwable e) {
        String requestId = getRequestId();
        if (e instanceof MsException) {
            MsException exception = (MsException) e;
            if (exception.getErrCode() == NO_SUCH_OBJECT || exception.getErrCode() == NO_SUCH_VERSION) {
                //如果head未找到,如果是纳管桶，那就去纳管桶中查找
                if (Objects.nonNull(request.getMember(STORE_MANAGEMENT_HEADER)) && !request.headers().contains("check-syncstamp")) {
                    StoreManagementServer.forwardRequest(request, true, null).subscribe(b -> {
                        if (!b) {
                            logger.error(e.getMessage() + " bucket: {} object: {} request id: {}",
                                    request.getBucketName(), request.getObjectName(), requestId);
                            addPublicHeaders(request, requestId).putHeader(X_AMZ_REQUEST_ID, requestId)
                                    .setStatusCode(404).end();
                        }
                    });
                    return;
                }
                // 异步复制流程里的head请求不打印error
                if (request.headers().contains(IS_SYNCING) || request.headers().contains("check-syncstamp")) {
                    addPublicHeaders(request, requestId).putHeader(X_AMZ_REQUEST_ID, requestId)
                            .setStatusCode(404).end();
                    logger.debug("is_syncing " + e.getMessage() + " bucket: {} object: {} request id: {}",
                            request.getBucketName(), request.getObjectName(), requestId);
                    return;
                }
                logger.debug(e.getMessage() + " bucket: {} object: {} request id: {}",
                        request.getBucketName(), request.getObjectName(), requestId);
                addPublicHeaders(request, requestId).putHeader(X_AMZ_REQUEST_ID, requestId)
                        .setStatusCode(404).end();
                return;
            } else if (exception.getErrCode() == METHOD_NOT_ALLOWED) {
                if (request.headers().contains("check-syncstamp")) {
                    addPublicHeaders(request, requestId).putHeader(X_AMZ_REQUEST_ID, requestId)
                            .setStatusCode(404).end();
                    logger.debug("is_syncing " + e.getMessage() + " bucket: {} object: {} request id: {}",
                            request.getBucketName(), request.getObjectName(), requestId);
                    return;
                }
            }
        }
        dealException(request, e);
    }

    private Mono<NetSocket> getTarget(SocketReqMsg msg, String vnodeId) {
        return getTargetIpReactive(msg, vnodeId)
                .flatMap(resMsg -> singleToMono(sender.getStreamSocket(resMsg.get(HEART_ETH1), resMsg.get(HEART_ETH2))
                        .doOnSuccess(socket -> socket.write(JsonUtils.toString(msg, SocketReqMsg.class) + '\n').pause())));
    }

    /**
     * 检查对象名字是否符合要求
     *
     * @param name 对象名字
     */
    private void checkObjectName(String name) {
        if (!OBJECT_NAME_PATTERN.matcher(name).matches()) {
            throw new MsException(ErrorNo.NAME_INPUT_ERR, "name is invalid");
        }

        if (name.getBytes(StandardCharsets.UTF_8).length > 1024) {
            throw new MsException(ErrorNo.NAME_INPUT_ERR, "name is invalid");
        }
    }

    /**
     * 将目标字符串填充上空字符，使总长度为1024
     * 注：不直接使用format是因为format性能较低
     *
     * @param msg 要填充的字符串
     * @return 填充之后的字符串
     */
    private String getFormattedStr(String msg) {
        StringBuilder builder = new StringBuilder(format);
        builder.replace(0, msg.length(), msg);
        return builder.toString();
    }

    /**
     * 处理上传时的Http Body的片段
     *
     * @param targetSocket 目标socket
     * @param migSocket    迁移时的目标socket
     * @return Http Body片段的回调函数
     */
    private Handler<Buffer> httpBodyChunkHandler(NetSocket targetSocket, NetSocket migSocket) {
        return httpBodyChunk -> {
            targetSocket.getDelegate().write(httpBodyChunk);
            migSocket.getDelegate().write(httpBodyChunk);
        };
    }

    /**
     * 上传之前的桶配额和账户配额检查
     *
     * @param bucketInfo        桶信息
     * @param accountQuotaFlag  账户容量配额标志
     * @param accountObjNumFlag 账户对象数配额标志
     */
    private void quotaCheck(Map<String, String> bucketInfo, String accountQuotaFlag, String accountObjNumFlag) {
        if ("1".equals(bucketInfo.get(QUOTA_FLAG))) {
            throw new MsException(NO_ENOUGH_SPACE, "No Enough Space Because of the bucket quota.");
        } else if ("1".equals(bucketInfo.get(OBJNUM_FLAG))) {
            throw new MsException(NO_ENOUGH_OBJECTS, "The bucket hard-max-objects was exceeded.");
        } else if ("2".equals(accountQuotaFlag)) {
            throw new MsException(NO_ENOUGH_SPACE, "No Enough Space Because of the account quota.");
        } else if ("2".equals(accountObjNumFlag)) {
            throw new MsException(NO_ENOUGH_OBJECTS, "The account hard-max-objects was exceeded.");
        }
    }

    private void capacityQuotaCheck(Map<String, String> bucketInfo, String accountQuotaFlag) {
        if ("1".equals(bucketInfo.get(QUOTA_FLAG))) {
            throw new MsException(NO_ENOUGH_SPACE, "No Enough Space Because of the bucket quota.");
        } else if ("2".equals(accountQuotaFlag)) {
            throw new MsException(NO_ENOUGH_SPACE, "No Enough Space Because of the account quota.");
        }
    }

    private void objectsQuotaCheck(Map<String, String> bucketInfo, String accountObjNumFlag) {
        if ("1".equals(bucketInfo.get(OBJNUM_FLAG))) {
            throw new MsException(NO_ENOUGH_OBJECTS, "The bucket hard-max-objects was exceeded.");
        } else if ("2".equals(accountObjNumFlag)) {
            throw new MsException(NO_ENOUGH_OBJECTS, "The account hard-max-objects was exceeded.");
        }
    }

    private void versionNumCheck(String versionNum) {
        if (!versionNum.equals("-1")) {
            if (!PatternConst.LIST_PARTS_PATTERN.matcher(versionNum).matches()) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "versionNum param error.");
            }
            int maxNum = Integer.parseInt(versionNum);
            if (maxNum > 256 || maxNum == 0) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "versionNum param error, must be int in [1, 256]");
            }
        }
    }

    /**
     * 错误处理
     * <p>
     * 这里的错误处理暂时没法集中到控制器层，因为Lettuce捕获了异常，导致异常无法继续向外抛出
     *
     * @param requestId request id
     * @param request   请求
     * @return 出错处理的回调函数
     */
    private Consumer<? super Throwable> exceptionHandler(String requestId, MsHttpRequest request) {
        return exception -> dealException(requestId, request, exception, false);
    }

    /**
     * 下载对象的subscriber
     */
    private static final class DownLoadSubscriber implements CoreSubscriber<byte[]> {
        //请求对象
        private final MsHttpRequest request;
        //上下文
        private final Context context;
        private Subscription s;
        private final UnicastProcessor<Long> streamController;
        private boolean dataStreamStart = false;
        private AtomicBoolean deleteSource;
        private AtomicBoolean backSource;

        private DownLoadSubscriber(MsHttpRequest request, Context context, UnicastProcessor<Long> streamController, AtomicBoolean deleteSource, AtomicBoolean backSource) {
            this.request = request;
            this.context = context;
            this.streamController = streamController;
            this.deleteSource = deleteSource;
            this.backSource = backSource;
            request.exceptionHandler(this::onError);
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
            Optional.ofNullable(request).ifPresent(request -> {
                request.addResponseCloseHandler(v -> s.cancel());
                request.exceptionHandler(e -> s.cancel());
            });
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onError(Throwable t) {
            if (request.getMember(STORE_MANAGEMENT_HEADER) != null && t instanceof MsException) {
                MsException e = (MsException) t;
                if (e.getErrCode() == NO_SUCH_OBJECT || e.getErrCode() == NO_SUCH_VERSION) {
                    s.cancel();
                    if (!backSource.get()) {
                        StoreManagementServer.forwardRequest(request, true, null).subscribe(b -> {
                            if (!b) {
                                dealException(request, t);
                            }
                        });
                        return;
                    }
                    if (deleteSource.get()) {
                        StoreManagementServer.forwardRequest(request, true, null).subscribe(b -> {
                            if (!b) {
                                dealException(request, t);
                            }
                        });
                        return;
                    }
                    String objectKey = request.getObjectName() + (StringUtils.isEmpty(request.getParam("versionId")) ? "" : request.getParam("versionId"));
                    if (request.headers().contains(RANGE)) {
                        StoreManagementServer.forwardRequest(request, true, null).subscribe(b -> {
                            if (!b) {
                                dealException(request, t);
                            } else {
                                request.headers().remove(RANGE);
                                if (requestSet.contains(objectKey)) {
                                    return;
                                }
                                requestSet.add(objectKey);
                                StoreManagementServer.forwardPutRequest(request, true, null).subscribe(r -> {
                                    requestSet.remove(objectKey);
                                });
                            }
                        });
                    } else {
                        if (requestSet.contains(objectKey)) {
                            StoreManagementServer.forwardRequest(request, true, null).subscribe(b -> {
                                if (!b) {
                                    dealException(request, t);
                                }
                            });
                        }
                        requestSet.add(objectKey);
                        StoreManagementServer.forwardGetRequest(request, true, null).subscribe(b -> {
                            requestSet.remove(objectKey);
                            if (!b) {
                                dealException(request, t);
                            }
                        });
                    }
                    return;
                }
            }
            if (!dataStreamStart) {
                if ("not match".equals(t.getMessage())) {
                    request.response()
                            .putHeader("check-syncstamp", "not match")
                            .setStatusCode(200)
                            .end();
                    return;
                }
                dealException(request, t);
            } else {
                request.response().ended();
                request.connection().close();
            }
            s.cancel();
        }

        @Override
        public void onNext(byte[] bytes) {
            try {
                context.runOnContext(v -> {
                    try {
                        request.response().write(Buffer.buffer(bytes), unit -> {
                            streamController.onNext(-1L);
                            dataStreamStart = true;
                        });
                    } catch (Exception e) {
                        logger.error(e);
                    }
                });
            } catch (Exception e) {
                logger.error("exception in write : ", e);
                s.cancel();
            }
        }

        @Override
        public void onComplete() {
            context.runOnContext(v -> {
                try {
                    request.response().end();
                } catch (Exception e) {
                    logger.error(e);
                }
            });
        }
    }

    public static Handler<Buffer> httpBodyLimitHandler(Consumer<byte[]> bytesConsumer, MsHttpRequest request, MessageDigest digest) {
        return buf -> {
            if (digest != null) {
                digest.update(buf.getBytes());
            }
            bytesConsumer.accept(buf.getBytes());
        };
    }

    private void rateLimitHandler(KeepAliveRequest request) {
        Buffer buf = request.getHttpBodyChunk();
        int position = request.getPosition();
        if (request.getWaitedTime() >= request.getTotalWaitTime()) {
            //已到等待时间发送余下所有字节
            request.getSink().next(DefaultPayload.create(buf.getBuffer(position, buf.length()).getByteBuf()));
            request.getRequest().resume();
        } else {
            //未到等待时间发送一个字节保持连接，并计算下次发送时间
            request.getSink().next(DefaultPayload.create(buf.getBuffer(position, position + 1).getByteBuf()));

            long delay;
            delay = Math.min(request.getTotalWaitTime() - request.getWaitedTime(), KEEP_SOCKET_PERIOD);
            vertx.setTimer(delay, e -> rateLimitHandler(request.add(delay)));
        }
    }


    /**
     * @param request, response
     * @return io.vertx.core.http.HttpServerResponse
     * @description 处理getObject的6个自定义request parameter
     * content-language ；expires；response-cache-control；
     * response-content-disposition；response-content-encoding
     * response-expires 格式要求 ：such as 'Tue, 3 Jun 2008 11:05:30 GMT'
     * @author makaikai
     * @date 2019/10/22
     */
    private static HttpServerResponse dealWithSixRequestParameter(MsHttpRequest request) {
        HttpServerResponse response = request.response();

        String language = request.getParam("response-content-language");
        if (language != null && language.length() > 0) {
            response.putHeader(CONTENT_LANGUAGE, language);
        }

        String control = request.getParam("response-cache-control");
        if (control != null && control.length() > 0) {
            response.putHeader(CACHE_CONTROL, control);
        }

        String disposition = request.getParam("response-content-disposition");
        if (disposition != null && disposition.length() > 0) {
            response.putHeader(CONTENT_DISPOSITION, disposition);
        }

        //临时URL下载大文件使用的响应头
        disposition = request.getParam(CONTENT_DISPOSITION);
        if (disposition != null && disposition.length() > 0) {
            response.putHeader(CONTENT_DISPOSITION, disposition);
        }

        String encoding = request.getParam("response-content-encoding");
        if (encoding != null && encoding.length() > 0) {
            response.putHeader(CONTENT_ENCODING, encoding);
        }

        String type = request.getParam("response-content-type");
        if (type != null && type.length() > 0) {
            response.putHeader(CONTENT_TYPE, type);
        }

        String expires = request.getParam("response-expires");
        if (expires != null && expires.length() > 0) {
            response.putHeader(EXPIRES, expires);
        }

        return response;
    }


    /**
     * 将桶中的对象移动到桶的回收站中
     *
     * @param request         请求
     * @param bucketName      桶名
     * @param newObjectName   垃圾目录名
     * @param oldObjectName   对象名
     * @param sourceVersionId 对象的版本ID
     * @return 执行结果
     */
    private Mono<Boolean> move(MsHttpRequest request, String bucketName, String newObjectName, String oldObjectName, String sourceVersionId) {

        final String wormExpire = request.getHeader(X_AMZ_WORM_DATE_HEADER);
        final String wormMode = request.getHeader(X_AMZ_WORM_MODE_HEADER);


        final String[] userId = {request.getUserId()};
        final String requestId = getRequestId();

        //进行请求头中参数校验
        checkObjectName(newObjectName);
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucketName, newObjectName);
        final String targetBucketVnode = bucketVnodeIdTuple.var1;
        final String targetBucketMigrateVnode = bucketVnodeIdTuple.var2;

        String stamp;
        if (StringUtils.isNotEmpty(request.getHeader("stamp"))) {
            stamp = request.getHeader("stamp");
        } else {
            stamp = String.valueOf(System.currentTimeMillis());
        }
        String lastModify = MsDateUtils.stampToGMT(Long.parseLong(stamp));
        final Long[] currentWormStamp = new Long[1]; // 当前worm时钟时间
        String wormStamp = request.getHeader(WORM_STAMP);

        final SocketReqMsg msg = buildRefactorCopyMessage(request, bucketName, oldObjectName);
        final Map<String, String> sysMetaMap = new HashMap<>();

        String[] sysHeader = {CONTENT_DISPOSITION, CONTENT_LANGUAGE, CACHE_CONTROL, CONTENT_TYPE, CONTENT_ENCODING, EXPIRES};

        MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();

        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        final boolean[] isInnerObject = {request.headers().contains(CLUSTER_ALIVE_HEADER)};
        AtomicBoolean needCopyWrite = new AtomicBoolean(false);

        return currentWormTimeMillis()
                .doOnNext(l -> currentWormStamp[0] = wormStamp == null ? l : Long.valueOf(wormStamp))
                .flatMap(obj -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))
                .doOnNext(bucketInfo -> setObjectRetention(bucketInfo, sysMetaMap, currentWormStamp[0], wormExpire, wormMode))
                .flatMap(bucketInfo -> {
                    msg.put("mda", bucketInfo.getOrDefault("mda", "off"));
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    return Mono.empty();
                })
                .switchIfEmpty(Mono.just(bucketPool.getBucketVnodeId(bucketName, oldObjectName)))
                .flatMap(vnode -> bucketPool.mapToNodeInfo(String.valueOf(vnode)))
                .flatMap(list -> getObjectMetaVersionUnlimited(bucketName, oldObjectName, sourceVersionId, list, null, currentSnapshotMark[0], snapshotLink[0]))
                .doOnNext(metadata -> {
                    checkMetaData(oldObjectName, metadata, sourceVersionId, currentSnapshotMark[0]);
                    JsonObject sysMeta = new JsonObject(metadata.sysMetaData);

                    // 包含此key-value标记的对象为系统内部生成的对象，不进行多站点间的同步
                    isInnerObject[0] = isInnerObject[0]
                            && sysMeta.containsKey(NO_SYNCHRONIZATION_KEY)
                            && NO_SYNCHRONIZATION_VALUE.equals(sysMeta.getString(NO_SYNCHRONIZATION_KEY));

                    // 对象的worm属性不复制
                    sysMeta.fieldNames()
                            .stream()
                            .filter(key -> !EXPIRATION.equals(key))
                            .forEach(key -> sysMetaMap.put(key, String.valueOf(sysMeta.getValue(key))));
                })
                .zipWith(BucketSyncSwitchCache.isSyncSwitchOffMono(bucketName))
                .flatMap(tuple2S -> {
                    MetaData sourceMeta = tuple2S.getT1();
                    Boolean isSyncSwitchOff = tuple2S.getT2();
                    StoragePool dataPool = StoragePoolFactory.getStoragePool(sourceMeta);
                    String[] targetObjSuffix = new String[]{ROCKS_FILE_META_PREFIX + dataPool.getObjectVnodeId(bucketName, newObjectName).var2};
                    // 聚合文件直接走真copy流程
                    needCopyWrite.set(StringUtils.isNotEmpty(sourceMeta.aggregationKey));
                    StoragePool targetPool;
                    if (needCopyWrite.get()) {
                        StorageOperate operate = new StorageOperate(DATA, newObjectName, sourceMeta.endIndex + 1);
                        targetPool = StoragePoolFactory.getStoragePool(operate, bucketName);
                    } else {
                        targetPool = dataPool;
                    }
                    MetaData metaData = createMeta(targetPool, bucketName, newObjectName, requestId).setUserMetaData(sourceMeta.userMetaData);
                    metaData.setSyncStamp(StringUtils.isBlank(request.getHeader(SYNC_STAMP)) ? VersionUtil.getVersionNum(isSyncSwitchOff) : request.getHeader(SYNC_STAMP));
                    metaData.setShardingStamp(VersionUtil.getVersionNum());
                    metaData.setSnapshotMark(currentSnapshotMark[0]);
                    return MsObjVersionUtils.getObjVersionIdReactive(bucketName)
                            .zipWith(VersionUtil.getVersionNum(bucketName, newObjectName))
                            .doOnNext(tuple2 -> {
                                String versionId = tuple2.getT1();
                                String versionNum = tuple2.getT2();
                                targetObjSuffix[0] = targetObjSuffix[0] + versionId + requestId;
                                String stampKey = Utils.getMetaDataKey(targetBucketVnode, bucketName, newObjectName, versionId, stamp, metaData.snapshotMark);
                                String fileMetaKey = Utils.getVersionMetaDataKey(targetBucketVnode, bucketName, newObjectName, versionId, metaData.snapshotMark);
                                msg.put("fileMetaKey", fileMetaKey);
                                msg.put("metaKey", stampKey);
                                if (StringUtils.isEmpty(userId[0])) {
                                    userId[0] = new JsonObject(metaData.sysMetaData).getString("owner");
                                }
                                sysMetaMap.put("owner", userId[0]);
                                sysMetaMap.put(LAST_MODIFY, lastModify);
                                if ("REPLACE".equals(request.getHeader(X_AMZ_METADATA_DIRECTIVE))) {
                                    sysMetaMap.keySet().removeIf(key -> SPECIAL_HEADER.contains(key.toLowerCase().hashCode()));
                                }
                                setSysMeta(sysHeader, sysMetaMap, request);
                                metaData.setVersionId(versionId).setVersionNum(versionNum).setStamp(stamp)
                                        .setFileName(sourceMeta.fileName).setEndIndex(sourceMeta.endIndex)
                                        .setObjectAcl(sourceMeta.getObjectAcl()).setReferencedBucket(sourceMeta.referencedBucket)
                                        .setReferencedKey(sourceMeta.referencedKey)
                                        .setReferencedVersionId(sourceMeta.referencedVersionId);

                                if (sourceMeta.getPartInfos() != null) {
                                    String uploadId = StringUtils.isNotBlank(request.getHeader(SYNC_COPY_PART_UPLOADID)) ?
                                            request.getHeader(SYNC_COPY_PART_UPLOADID) :
                                            RandomStringUtils.randomAlphabetic(32);
                                    PartInfo[] metaPartInfos = sourceMeta.getPartInfos();
                                    boolean isAppendObject = isAppendableObject(sourceMeta);
                                    for (PartInfo info : metaPartInfos) {
                                        info.setBucket(bucketName);
                                        info.setObject(newObjectName);
                                        if (!isAppendObject) {
                                            info.setUploadId(uploadId);
                                        }
                                        info.setDeduplicateKey(null);
                                        info.setSnapshotMark(currentSnapshotMark[0]);
                                    }
                                    if (isAppendObject) {
                                        metaData.setPartUploadId(sourceMeta.getPartUploadId());
                                    } else {
                                        metaData.setPartUploadId(uploadId);
                                    }
                                    metaData.setPartInfos(metaPartInfos);
                                }

                                if (!"REPLACE".equals(request.getHeader(X_AMZ_METADATA_DIRECTIVE))) {
                                    metaData.setUserMetaData(sourceMeta.userMetaData);
                                }
                                metaData.setSysMetaData(JsonUtils.toString(sysMetaMap, HashMap.class));
                                if (sourceMeta.referencedKey.equals(newObjectName) && sourceMeta.referencedBucket.equals(bucketName)
                                        && sourceMeta.referencedVersionId.equals(versionId)) {
                                    targetObjSuffix[0] = "#" + requestId;
                                }
                            }).flatMap(b -> {
                                if (needCopyWrite.get()) {
                                    return ErasureClient.copyCryptoData(dataPool, targetPool, sourceMeta, metaData, request, recoverDataProcessor, requestId, msg.get("fileMetaKey"));
                                }
                                String objVnode = dataPool.getObjectVnodeId(metaData);
                                return dataPool.mapToNodeInfo(objVnode)
                                        .flatMap(list -> updateFileMeta(metaData, targetObjSuffix[0], recoverDataProcessor, null, null));
                            })
                            .doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(UNKNOWN_ERROR, "move object internal error");
                                }
                            })
                            .flatMap(b -> bucketPool.mapToNodeInfo(targetBucketVnode))
                            .flatMap(bucketList -> {
                                        if (metaData.getPartInfos() != null) {
                                            PartInfo[] metaPartInfos = metaData.getPartInfos();
                                            for (PartInfo info : metaPartInfos) {
                                                info.setFileName(info.fileName.split(ROCKS_FILE_META_PREFIX)[0] + targetObjSuffix[0]);
                                            }
                                            metaData.setPartInfos(metaPartInfos);
                                        } else if (!needCopyWrite.get()) {
                                            metaData.setFileName(metaData.fileName.split(ROCKS_FILE_META_PREFIX)[0] + targetObjSuffix[0]);
                                        }

                                        EsMeta esMeta = new EsMeta()
                                                .setUserId(userId[0])
                                                .setSysMetaData(JsonUtils.toString(sysMetaMap, HashMap.class))
                                                .setUserMetaData(metaData.userMetaData)
                                                .setBucketName(bucketName)
                                                .setObjName(newObjectName)
                                                .setVersionId(metaData.versionId)
                                                .setStamp(stamp)
                                                .setObjSize(String.valueOf(metaData.endIndex + 1))
                                                .setInode(metaData.inode);
                                        return Mono.just(targetBucketMigrateVnode != null)
                                                .flatMap(b -> {
                                                    if (b) {
                                                        if (targetBucketMigrateVnode.equals(targetBucketVnode)) {
                                                            return Mono.just(true);
                                                        }
                                                        return bucketPool.mapToNodeInfo(targetBucketMigrateVnode)
                                                                .flatMap(nodeList -> putMetaData(Utils.getMetaDataKey(targetBucketMigrateVnode, bucketName,
                                                                        newObjectName, metaData.versionId, metaData.stamp, metaData.snapshotMark), metaData, nodeList, recoverDataProcessor, null,
                                                                        msg.get("mda"), esMeta, snapshotLink[0]));
                                                    }
                                                    return Mono.just(true);
                                                })
                                                .doOnNext(b -> {
                                                    if (!b) {
                                                        throw new MsException(UNKNOWN_ERROR, "put object meta to new mapping error!");
                                                    }
                                                }).flatMap(r -> putMetaData(msg.get("metaKey"), metaData, bucketList, recoverDataProcessor, null, msg.get("mda"), esMeta, snapshotLink[0])
                                                        .doOnNext(b -> {
                                                            if (!b) {
                                                                Set<String> overWriteFileNames = new HashSet<>();
                                                                if (metaData.partUploadId != null) {
                                                                    for (PartInfo info : metaData.partInfos) {
                                                                        overWriteFileNames.add(info.fileName);
                                                                    }
                                                                } else if (StringUtils.isNotEmpty(metaData.aggregationKey)) {
                                                                    overWriteFileNames.add(metaData.aggregationKey);
                                                                } else {
                                                                    overWriteFileNames.add(metaData.fileName);
                                                                }
                                                                String[] overWriteFileName = overWriteFileNames.toArray(new String[0]);
                                                                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(dataPool.getVnodePrefix());
                                                                SocketReqMsg overWriteMsg = new SocketReqMsg("", 0)
                                                                        .put("bucket", metaData.referencedBucket)
                                                                        .put("storage", dataPool.getVnodePrefix())
                                                                        .put("object", metaData.referencedKey)
                                                                        .put("overWrite0", Json.encode(overWriteFileName))
                                                                        .put("poolQueueTag", poolQueueTag);
                                                                ObjectPublisher.basicPublish(bucketList.get(0).var1, overWriteMsg, OVER_WRITE);
                                                                throw new MsException(UNKNOWN_ERROR, "put meta data error!");
                                                            }
                                                            Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(true));
                                                        })).flatMap(b -> {
                                                    if ("on".equals(msg.get("mda"))) {
                                                        MonoProcessor<Boolean> esRes = MonoProcessor.create();
                                                        Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> EsMetaTask.putEsMeta(esMeta, false)).subscribe(esRes::onNext);
                                                        return esRes;
                                                    } else {
                                                        return Mono.just(b);
                                                    }
                                                });
                                    }
                            )
                            .flatMap(b -> {
                                try {
                                    QuotaRecorder.addCheckBucket(bucketName);
                                } catch (Exception e) {
                                    logger.error(e);
                                }
                                return Mono.just(b);
                            });
                });

    }

    /**
     * 获取object的存储策略
     */
    public String getObjectStorageStrategy(String defaultStrategy, MetaData metaData) {
        if (metaData.storage.startsWith("cache")) {
            return defaultStrategy;
        } else {
            Set<Map.Entry<String, StorageStrategy>> entries = POOL_STRATEGY_MAP.entrySet();
            for (Map.Entry<String, StorageStrategy> entry : entries) {
                String dataStr = entry.getValue().redisMap.get("data");
                if (StringUtils.isNotEmpty(dataStr)) {
                    String[] dataArr = JSON.parseObject(dataStr,
                            new com.alibaba.fastjson.TypeReference<String[]>() {
                            });
                    for (String data : dataArr) {
                        if (metaData.storage.equals(data)) {
                            return entry.getKey();
                        }
                    }
                }
            }
        }
        return null;
    }


    /**
     * 将回收站中的指定对象恢复到原来的位置
     *
     * @return
     */
    private Mono<Boolean> recoverObject(MsHttpRequest request, String dir) {

        String bucket = request.getBucketName();
        String objectName = request.getObjectName();
        //将对象的路径改成原来的路径

        if (StringUtils.isBlank(dir)) {
            return Mono.just(false);
        }
        if (request.headers().contains(LAST_STAMP)) {
            String lastStamp = request.headers().get(LAST_STAMP);
            request.headers().set(SYNC_STAMP, lastStamp);
        }
        String newObjectName = getObjectStr(objectName, dir);
        UnifiedMap<String, String> paramMap = RequestBuilder.parseRequest(request);
        String versionId = StringUtils.isEmpty(paramMap.get(VERSIONID)) ? paramMap.get("versionid") : paramMap.get(VERSIONID);
        return move(request, bucket, newObjectName, objectName, versionId);


    }

    private static String getObjectStr(String objectName, String trashDir) {
        if (objectName.startsWith(trashDir)) {
            return objectName.substring(trashDir.length());
        }
        return null;
    }

    /**
     * 当删除流程出现错误导致无法正常删除时，调用此函数对已复制的对象进行删除
     *
     * @param objectName 对象名
     * @param bucketName 桶名
     * @return
     */

    private Mono<Boolean> delTrashObj(String objectName, String bucketName, String userId, SocketReqMsg
            socketReqMsg, String currentSnapshotMark, String snapshotLink) {
        HttpServerRequestImpl httpServerRequest = null;

        MsHttpRequest request = new MsHttpRequest(httpServerRequest);
        request.setBucketName(bucketName);
        request.setObjectName(objectName);
        request.setUserId(userId);


        StoragePool bucketPool = StoragePoolFactory.getStoragePool(StorageOperate.META, bucketName);
        com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucketName, objectName);
        List<String> needDeleteFile = new LinkedList<>();
        String requestId = RandomStringUtils.randomAlphanumeric(32);
        SocketReqMsg msg = SocketReqMsgBuilder.buildDelEcObjMsg(bucketName, objectName, requestId);
        msg.put("mda", socketReqMsg.get("mda"));
        msg.put("status", socketReqMsg.get("status"));
        final String bucketVnode = bucketVnodeIdTuple.var1;
        final String migrateVnode = bucketVnodeIdTuple.var2;
        MetaData[] metaData = new MetaData[]{null};
        String[] versionNum = new String[]{null};
        boolean[] syncDelete = new boolean[]{true};
        boolean deleteESSource = !isNotDeleteEs || request.getHeader("deleteSource") == null || !"1".equals(request.getHeader("deleteSource"));
        return VersionUtil.getVersionNum(bucketName, objectName)
                .doOnNext(version -> versionNum[0] = version)
                .flatMap(res -> bucketPool.mapToNodeInfo(bucketVnode))
                .flatMap(list -> getObjectMetaVersionUnlimited(bucketName, objectName, "null", list, null, currentSnapshotMark, snapshotLink))
                .flatMap(meta -> {
                    if (meta.equals(ERROR_META)) {
                        throw new MsException(UNKNOWN_ERROR, "Get Object Meta Data fail");
                    }

                    if (meta.equals(NOT_FOUND_META) || meta.deleteMark) {
                        if ("NULL".equals(msg.get("status"))) {
                            throw new MsException(NO_SUCH_OBJECT, "no such object, object name: " + objectName + ".");
                        } else {
                            MetaData newMeta = new MetaData().setBucket(bucketName).setKey(objectName).setPartUploadId(null)
                                    .setReferencedBucket(bucketName).setReferencedKey(objectName);
                            metaData[0] = newMeta;
                            return Mono.just(metaData[0]);
                        }
                    } else {
                        if (meta.partUploadId == null) {
                            needDeleteFile.add(meta.getFileName());
                        } else {
                            for (PartInfo partInfo : meta.partInfos) {
                                needDeleteFile.add(partInfo.fileName);
                            }
                        }
                        metaData[0] = meta;
                        return Mono.just(metaData[0]);
                    }
                })
                .flatMap(meta -> {
                    metaData[0] = meta;

                    if (meta.partUploadId == null) {
                        needDeleteFile.add(meta.getFileName());
                    } else {
                        for (PartInfo partInfo : meta.partInfos) {
                            needDeleteFile.add(partInfo.fileName);
                        }
                    }
                    return Mono.just(metaData[0]);
                })
                .flatMap(meta -> {
                    if (migrateVnode != null) {
                        return bucketPool.mapToNodeInfo(migrateVnode)
                                .flatMap(migrateNodeList -> setDelMark(new DelMarkParams(bucketName, objectName, "null", meta, migrateNodeList, msg.get("status"), versionNum[0]).migrate(true),
                                        currentSnapshotMark, snapshotLink));
                    }
                    return Mono.just(1);
                })
                .doOnNext(b -> {
                    if (b == 0) {
                        throw new MsException(DEL_OBJ_ERR, "delete migrate " + migrateVnode + " object internal error");
                    }
                })
                .doOnNext(r -> syncDelete[0] = syncDelete[0] && r == 1)
                .flatMap(b -> bucketPool.mapToNodeInfo(bucketVnode))
                .flatMap(bucketVnodeList -> setDelMark(new DelMarkParams(bucketName, objectName, "null", metaData[0], bucketVnodeList, msg.get("status"), versionNum[0]), currentSnapshotMark,
                        snapshotLink))
                .doOnNext(b -> {
                    if (b == 0) {
                        throw new MsException(DEL_OBJ_ERR, "delete object internal error");
                    }
                })
                .doOnNext(r -> syncDelete[0] = syncDelete[0] && r == 1)
                .flatMap(b -> syncDelete[0] ? delVersionObj(null, needDeleteFile, msg, metaData, request, currentSnapshotMark) : Mono.just(true))
                .flatMap(f -> {
                    if (!syncDelete[0]) {
                        if (!"off".equals(msg.get("mda"))) {
                            return EsMetaTask.putOrDelS3EsMeta(userId, bucketName, objectName, metaData[0].versionId, metaData[0].endIndex + 1, request, true, !deleteESSource);
                        }
                    }
                    return Mono.just(true);
                });
    }

    /**
     * 数据追加写接口
     *
     * @param request
     */

    public int appendObject(MsHttpRequest request) {

        if (isMultiAliveStarted) {
            throw new MsException(ErrorNo.METHOD_NOT_ALLOWED, "The specified method is not allowed against this resource.");
        }
        MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();

        checkAppendHeader(request);

        request.setExpectMultipart(true);

        //标识是否为第一次追加数据
        final boolean[] firstAppend = {true};

        //追加数据的起始位置
        long position = Long.parseLong(request.getParam(POSITION));

        int[] policy = {0};
        final boolean[] dedup = {false};

        final String bucketName = request.getBucketName();
        final String objName = request.getObjectName();
        final String requestId = getRequestId();
        final String user = request.getUserId();
        final String backupUserId = request.getHeader("backup_userId");
        checkObjectName(objName);
        final JsonObject userMeta = getUserMeta(request);

        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucketName, objName);
        final String bucketVnode = bucketVnodeIdTuple.var1;
        final String migrateVnode = bucketVnodeIdTuple.var2;

        final long objectSize = Long.parseLong(request.getHeader(CONTENT_LENGTH));
        StorageOperate dataOperate = new StorageOperate(DATA, objName, Long.MAX_VALUE);
        final StoragePool[] dataPool = {StoragePoolFactory.getStoragePool(dataOperate, bucketName)};
        final StoragePool defaultDataPool = dataPool[0];

        final String[] versionId = {"null"};
        final String[] latestVersionId = {"null"};
        final MetaData[] metaData = {null};
        final JsonObject[] sysMetaMap = {null};
        SocketReqMsg msg = buildUploadMsg(request, bucketName, objName, requestId);

        long currMills = System.currentTimeMillis();
        String stamp = StringUtils.isBlank(request.getHeader("stamp")) ? String.valueOf(currMills) : request.getHeader("stamp");
        String lastModify = MsDateUtils.stampToGMT(Long.parseLong(stamp));

        PartInfo partInfo = new PartInfo()
                .setBucket(bucketName)
                .setObject(objName)
                .setLastModified(MsDateUtils.stampToISO8601(currMills))
                .setPartSize(objectSize);

        long[] currentWormStamp = {0};
        JsonObject aclJson = new JsonObject();

        String contentMD5 = request.getHeader(CONTENT_MD5);
        String wormStamp = request.getHeader(WORM_STAMP);
        final Map<String, String>[] bucketMap = new Map[]{null};

        final String[] accountQuota = {"0"};
        final String[] objNumQuota = {"0"};
        boolean[] isFS = new boolean[]{false};
        int[] startFsType = {0};
        boolean[] isNewMeta = new boolean[]{false};
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        Inode[] dirInodes = {null};
        Disposable disposable = Mono.just(true)
                .doOnNext(b -> CryptoUtils.containsCryptoAlgorithm(request.getHeader(X_AMZ_SERVER_SIDE_ENCRYPTION)))
                .flatMap(b -> wormStamp != null ? Mono.just(Long.valueOf(wormStamp)) : currentWormTimeMillis())
                .doOnNext(wormTime -> currentWormStamp[0] = wormTime)
                .flatMap(b -> pool.getReactive(REDIS_USERINFO_INDEX).hget(user, USER_DATABASE_ID_NAME))
                .flatMap(username -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(username))
                .flatMap(b -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "No such bucket")))
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, objName, "PutObject").zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .flatMap(tuple2 -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(tuple2.getT2().get("user_name")).zipWith(Mono.just(tuple2.getT2())))
                .flatMap(tuple2 -> {
                    accountQuota[0] = tuple2.getT1().get(USER_DATABASE_ACCOUNT_QUOTA_FLAG);
                    objNumQuota[0] = tuple2.getT1().get(USER_DATABASE_ACCOUNT_OBJNUM_FLAG);
                    regionCheck(tuple2.getT2().get(REGION_NAME));
                    siteCheck(tuple2.getT2());
                    currentSnapshotMark[0] = tuple2.getT2().get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = tuple2.getT2().get(SNAPSHOT_LINK);
                    if (objectSize > 0) {
                        if (!request.headers().contains(IS_SYNCING)) {
                            capacityQuotaCheck(tuple2.getT2(), accountQuota[0]);
                        }
                    }
                    bucketMap[0] = tuple2.getT2();
                    if (tuple2.getT2().containsKey("fsid")) {
                        isFS[0] = true;
                        startFsType[0] = ACLUtils.setFsProtoType(tuple2.getT2());
                    }
                    if (!PASSWORD.equals(request.getHeader(SYNC_AUTH)) && policy[0] == 0) {
                        return MsAclUtils.checkWriteAclReactive(tuple2.getT2(), request.getUserId(), bucketName, msg);
                    }
                    return Mono.empty();
                })
                .switchIfEmpty(pool.getReactive(REDIS_USERINFO_INDEX).hget(Optional.ofNullable(backupUserId).orElse(user), "name"))
                .map(userName -> {
                    msg.put("name", (String) userName);
                    return bucketMap[0];
                })
                .doOnNext(bucketInfo -> {
                    String versionStatus = bucketInfo.getOrDefault(BUCKET_VERSION_STATUS, "NULL");
                    String versionId1 = "NULL".equals(versionStatus) || VERSION_SUSPENDED.equals(versionStatus) ?
                            "null" : RandomStringUtils.randomAlphanumeric(32);
                    versionId[0] = StringUtils.isNoneBlank(request.getHeader(VERSIONID)) ? request.getHeader(VERSIONID) : versionId1;
                    latestVersionId[0] = "NULL".equals(versionStatus) ? latestVersionId[0] : "";
                    msg.put("status", versionStatus);
                })
                .flatMap(bucketInfo -> bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(bucketName, objName))
                        // 获取该对象最新版本元数据
                        .flatMap(list -> getObjectMetaVersionUnlimited(bucketName, objName, latestVersionId[0], list, request, currentSnapshotMark[0], snapshotLink[0])).zipWith(Mono.just(bucketInfo))
                )
                .flatMap(tuple2 -> {
                    MetaData meta = tuple2.getT1();
                    if (meta.equals(NOT_FOUND_META) || meta.deleteMark || meta.deleteMarker || meta.isUnView(currentSnapshotMark[0])) {
                        return MsObjVersionUtils.checkObjVersionsLimit(bucketName, objName, request).map(b -> tuple2);
                    }
                    return Mono.just(tuple2);
                })
                .flatMap(tuple2 -> {
                    MetaData meta = tuple2.getT1();
                    if (meta.equals(ERROR_META)) {
                        return Mono.error(new MsException(UNKNOWN_ERROR, "AppendObject Get bucket: " + bucketName + " Object object:" + objName + " Meta Data fail"));
                    }
                    if (meta.equals(NOT_FOUND_META) || meta.deleteMark || meta.deleteMarker || meta.isUnView(currentSnapshotMark[0])) {
                        if (!request.headers().contains(IS_SYNCING)) {
                            objectsQuotaCheck(tuple2.getT2(), objNumQuota[0]);
                        }
                        if (position != 0) {
                            return Mono.error(new MsException(POSITION_NOT_EQUAL_TO_LENGTH, "AppendObject bucket: " + bucketName + " objName:" + objName + " Position: " + position + " is not equal " +
                                    "to file length: " + 0 + "."));
                        }
                        if (isFS[0]) {
                            isNewMeta[0] = true;
                        }
                        String crypto = request.getHeader(X_AMZ_SERVER_SIDE_ENCRYPTION);
                        metaData[0] = createMeta(dataPool[0], bucketName, objName, requestId)
                                .setUserMetaData(userMeta.encode())
                                .setEndIndex(objectSize - 1)
                                .setStamp(stamp)
                                .setSmallFile(objectSize < MAX_SMALL_SIZE)
                                .setVersionId(versionId[0])
                                .setReferencedVersionId(versionId[0])
                                .setPartUploadId("appendObject")
                                .setPartInfos(new PartInfo[]{partInfo})
                                .setCrypto(StringUtils.isBlank(crypto) ? tuple2.getT2().get("crypto") : crypto)
                                .setSnapshotMark(currentSnapshotMark[0]);

                        String fileName = Utils.getPartFileName(dataPool[0], bucketName, objName, "append:1", "1", requestId);
                        partInfo.setPartNum("1").setUploadId("append:1").setFileName(fileName);
                        partInfo.setStorage(metaData[0].getStorage());
                        partInfo.setVersionId(metaData[0].versionId);
                        partInfo.setSnapshotMark(metaData[0].getSnapshotMark());
                        sysMetaMap[0] = getSysMetaMap(request)
                                .put("owner", user)
                                .put(LAST_MODIFY, lastModify)
                                .put("displayName", msg.get("name"));

                        if (StringUtils.isNotEmpty(backupUserId)) {
                            msg.put("backup_userId", backupUserId);
                            msg.put("backup_time", request.getHeader("backup_time"));
                            sysMetaMap[0].put("owner", backupUserId);
                        }
//                        logger.info("first:{}", partInfo.getFileName());
                        return createObjAcl(request, aclJson).zipWith(Mono.just(tuple2.getT2()));
                    } else {
                        metaData[0] = meta;
                        //元数据中用partUploadId="appendObject"来表示为可追加的对象
                        if (!isAppendableObject(meta)) {
                            return Mono.error(new MsException(OBJECT_NOT_APPENDABLE, "The object is not appendable."));
                        }

                        if (StringUtils.isNotBlank(request.getHeader(VERSIONID)) && !StringUtils.equals(request.getHeader(VERSIONID), meta.getVersionId())) {
                            return Mono.error(new MsException(OBJECT_NOT_APPENDABLE, "It is not the latest versionId."));
                        }

                        long wormExpire = getWormExpire(meta);
                        checkWormRetention(request, wormExpire, currentWormStamp[0]);
                        if (wormExpire != 0) {
                            return Mono.error(new MsException(OBJECT_NOT_APPENDABLE, "The object is not appendable."));
                        }

                        if (position != meta.getEndIndex() + 1) {
                            return Mono.error(new MsException(POSITION_NOT_EQUAL_TO_LENGTH,
                                    "Bucket" + bucketName + "key " + objName + " Position:" + position + " is not equal to file length: " + meta.getEndIndex() + 1 + ".."));
                        }

                        firstAppend[0] = false;
                        dataPool[0] = StoragePoolFactory.getStoragePool(meta);

                        int partNum = meta.getPartInfos().length + 1;
                        String uploadId = "append:" + partNum;
                        String fileName = Utils.getPartFileName(dataPool[0], meta.getBucket(), meta.getKey(), uploadId, partNum + "", requestId);
                        partInfo.setPartNum(partNum + "")
                                .setStorage(meta.getStorage())
                                .setUploadId(uploadId)
                                .setFileName(fileName)
                                .setVersionId(meta.versionId);
                        // append接口中，使partinfo中的快照标记始终和meta中保持一致
                        partInfo.setSnapshotMark(meta.getSnapshotMark());
                        partInfo.setInitSnapshotMark(meta.getSnapshotMark());

//                        PartInfo[] newPartInfos;
//                        if (meta.getPartInfos().length == 1 && meta.getPartInfos()[0].partSize == 0) {
//                            newPartInfos = new PartInfo[]{partInfo};
//                        } else {
//                            newPartInfos = appendPartInfo(meta.getPartInfos(), partInfo);
//                        }
                        PartInfo[] newPartInfos = appendPartInfo(meta.getPartInfos(), partInfo);

                        meta.setPartInfos(newPartInfos).setEndIndex(meta.getEndIndex() + objectSize);

                        metaData[0] = meta;
                        // 追加写对象 fileName置为null
                        metaData[0].fileName = null;

                        String sysMetaData = meta.getSysMetaData();
                        sysMetaMap[0] = new JsonObject(sysMetaData);
                        long oldLength = Long.parseLong(sysMetaMap[0].getString(CONTENT_LENGTH));
                        if (oldLength + objectSize > MAX_UPLOAD_TOTAL_SIZE) {
                            throw new MsException(OBJECT_SIZE_OVERFLOW, "object size too large.");
                        }
                        if (meta.getPartInfos().length > MAX_UPLOAD_PART_NUM) {
                            throw new MsException(OBJECT_NOT_APPENDABLE, "append object part amount too large.");
                        }
                        sysMetaMap[0].put(CONTENT_LENGTH, (oldLength + objectSize) + "")
                                .put(LAST_MODIFY, lastModify);
//                        logger.info("append:{}", partInfo.getFileName());
                        return Mono.just(true).zipWith(Mono.just(tuple2.getT2()));
                    }
                })
                .map(Tuple2::getT2)
                .doOnNext(bucketInfo -> {
                    request.addMember("address", bucketInfo.getOrDefault("address", ""));
                    msg.put("mda", "on".equals(bucketInfo.get("mda")) ? "on" : "off");
                    if (firstAppend[0]) {
                        metaData[0].setObjectAcl(aclJson.encode());
                        String wormExpire = request.getHeader(X_AMZ_WORM_DATE_HEADER);
                        String wormMode = request.getHeader(X_AMZ_WORM_MODE_HEADER);
                        String syncWormExpire = request.getHeader(SYNC_WORM_EXPIRE);
                        if (StringUtils.isEmpty(syncWormExpire) && !request.headers().contains(IS_SYNCING)) {
                            setObjectRetention(bucketInfo, sysMetaMap[0], currentWormStamp[0], wormExpire, wormMode);
                        } else if (StringUtils.isNotEmpty(syncWormExpire)) {
                            sysMetaMap[0].put(EXPIRATION, syncWormExpire);
                        }
                    }
                })
                .flatMap(list -> dataPool[0].mapToNodeInfo(dataPool[0].getObjectVnodeId(partInfo.getFileName())))
                .flatMap(list -> PartClient.partUpload(dataPool[0], partInfo.getFileName(), list, request, partInfo, recoverDataProcessor, metaData[0].getCrypto(), false))
                .doOnNext(md5 -> {
                    if (StringUtils.isBlank(md5)) {
                        throw new MsException(UNKNOWN_ERROR, "put file fail");
                    }
                    if (contentMD5 != null) {
                        checkMd5(contentMD5, md5);
                    }
                })
                .flatMap(md5 -> StoragePoolFactory.getDeduplicateByBucketName(bucketName).zipWith(Mono.just(md5)))
                .doOnNext(tuple2 -> dedup[0] = tuple2.getT1())
                .flatMap(tuple2 -> {
                    String md5 = tuple2.getT2();
                    partInfo.setEtag(md5);
                    MessageDigest digest = Md5DigestPool.acquire();
                    for (PartInfo info : metaData[0].getPartInfos()) {
                        digest.update(BaseEncoding.base16().decode(info.getEtag().toUpperCase()));
                    }
                    sysMetaMap[0].put(ETAG, Hex.encodeHexString(digest.digest()) + "-" + metaData[0].getPartInfos().length);
                    Md5DigestPool.release(digest);
                    if (objectSize == 0) {
                        return Mono.just(true);
                    }

                    if (dedup[0]) {
                        StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);
                        String dedupVnode = dedupPool.getBucketVnodeId(md5);
                        String suffix = Utils.getDeduplicatMetaKey(dedupVnode, md5, partInfo.storage, requestId);

                        return dedupPool.mapToNodeInfo(dedupVnode)
                                .flatMap(nodeList -> putPartDeduplicate(dedupPool, suffix,
                                        nodeList, request, recoverDataProcessor, partInfo))
                                .flatMap(b -> {
                                    if (!b) {
                                        return Mono.error(new MsException(UNKNOWN_ERROR, "put deduplicate meta failed"));
                                    }
                                    partInfo.deduplicateKey = suffix;
                                    return Mono.just(b);
                                });
                    }
                    return Mono.just(true);
                })
                .flatMap(b -> VersionUtil.getVersionNum(bucketName, objName))
                .flatMap(versionNum -> {
                    metaData[0].setVersionNum(versionNum);
                    metaData[0].setSyncStamp(StringUtils.isBlank(request.getHeader(SYNC_STAMP)) ? versionNum : request.getHeader(SYNC_STAMP));
                    metaData[0].setShardingStamp(VersionUtil.getVersionNum());
                    metaData[0].setSysMetaData(sysMetaMap[0].encode());
                    metaData[0].setStamp(stamp);
                    partInfo.setVersionNum(versionNum);
                    partInfo.setSyncStamp(metaData[0].getSyncStamp());
                    return bucketPool.mapToNodeInfo(bucketVnode);
                })
                .flatMap(tuple -> {
                    if (isFS[0] && isNewMeta[0]) {
                        return Mono.error(new MsException(OBJECT_NOT_APPENDABLE, "The object is not appendable."));
                    } else {
                        return Mono.just(tuple);
                    }
                })
                .flatMap(bucketList -> {
                    EsMeta esMeta = new EsMeta()
                            .setUserId(user)
                            .setSysMetaData(sysMetaMap[0].encode())
                            .setUserMetaData(userMeta.encode())
                            .setBucketName(bucketName)
                            .setObjName(objName)
                            .setVersionId(metaData[0].getVersionId())
                            .setStamp(metaData[0].getStamp())
                            .setObjSize(String.valueOf(metaData[0].getEndIndex() + 1))
                            .setInode(metaData[0].inode);
                    Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(false));

                    return Mono.just(migrateVnode != null)
                            .flatMap(b -> {
                                if (b) {
                                    if (migrateVnode.equals(bucketVnode)) {
                                        return Mono.just(true);
                                    }
                                    return bucketPool.mapToNodeInfo(migrateVnode)
                                            .flatMap(nodeList -> putMetaData(Utils.getMetaDataKey(migrateVnode, bucketName,
                                                    objName, metaData[0].getVersionId(), metaData[0].stamp, metaData[0].snapshotMark), metaData[0], nodeList, recoverDataProcessor, request, msg.get(
                                                    "mda"), esMeta, null));
                                }
                                return Mono.just(true);
                            })
                            .doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(UNKNOWN_ERROR, "put object meta to new mapping error!");
                                }
                            })
                            .flatMap(b -> putMetaData(Utils.getMetaDataKey(bucketVnode, bucketName,
                                    objName, metaData[0].versionId, metaData[0].stamp, metaData[0].snapshotMark), metaData[0], bucketList, recoverDataProcessor, request, msg.get("mda"), esMeta, null))
                            .doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(UNKNOWN_ERROR, "put meta data error!");
                                }
                                Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(true));
                            })
                            .flatMap(b -> {
                                if (!"off".equals(msg.get("mda"))) {
                                    MonoProcessor<Boolean> esRes = MonoProcessor.create();
                                    Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> EsMetaTask.putEsMeta(esMeta, false)).subscribe(esRes::onNext);
                                    return esRes;
                                } else {
                                    return Mono.just(b);
                                }
                            });
                })
                .onErrorResume(e -> {
                    if (e instanceof MsException) {
                        MsException er = (MsException) e;
                        if (er.getErrCode() == OBJECT_NOT_APPENDABLE) {
                            if (isFS[0]) {
                                msg.put("mda", "on".equals(bucketMap[0].get("mda")) ? "on" : "off");
                                Inode[] inodes = new Inode[]{ERROR_INODE};
                                dataPool[0] = defaultDataPool;
                                Mono<Boolean> putMono = Mono.just(false);
                                if (isNewMeta[0]) {
                                    ReqInfo reqHeader = new ReqInfo();
                                    reqHeader.bucket = bucketName;
                                    reqHeader.bucketInfo = bucketMap[0];

                                    RpcCallHeader callHeader = new RpcCallHeader(null);
                                    try {
                                        AuthUnix auth = new AuthUnix();
                                        auth.flavor = 1;

                                        int[] uidAndGid = ACLUtils.getUidAndGid(request.getUserId());
                                        auth.setUid(uidAndGid[0]);
                                        auth.setGid(uidAndGid[1]);
                                        Set<Integer> gids = ACLUtils.s3IDToGids.get(request.getUserId());
                                        if (gids == null) {
                                            auth.setGidN(1);
                                            auth.setGids(new int[]{uidAndGid[1]});
                                        } else {
                                            auth.setGidN(gids.size());
                                            auth.setGids(gids.stream().mapToInt(Integer::intValue).toArray());
                                        }
                                        callHeader.auth = auth;
                                    } catch (Exception mapAuthErr) {
                                        logger.error("map auth error ", mapAuthErr);
                                    }

                                    //上传可追加文件，此时判断父目录wx权限
                                    int judgeFlag = ACLUtils.setDirOrFile(false);
                                    putMono = S3ACL.checkFSACL(objName, bucketName, request.getUserId(), dirInodes, 7, false, startFsType[0], judgeFlag)
                                            .flatMap(b -> InodeUtils.create(reqHeader, DEFAULT_FILE_MODE | S_IFREG, FILE_ATTRIBUTE_ARCHIVE, objName, objName, -1, dataPool[0].getVnodePrefix(), null, null, callHeader))
                                            .flatMap(inode -> {
                                                if (isError(inode)) {
                                                    return Mono.error(new MsException(UNKNOWN_ERROR, "put meta data error!"));
                                                }
                                                metaData[0].versionId = inode.getVersionId();
                                                inodes[0] = inode;
                                                return Mono.just(true);
                                            });
                                } else {
                                    if (metaData[0].inode > 0 && "inode".equals(metaData[0].partUploadId)) {
                                        putMono = Mono.just(true).flatMap(b -> Node.getInstance().getInode(request.getBucketName(), metaData[0].inode))
                                                .flatMap(inode -> {
                                                    return FSQuotaUtils.addQuotaDirInfo(inode, Long.parseLong(stamp), true);
                                                })
                                                .flatMap(inode -> {
                                                    if (isError(inode)) {
                                                        return Mono.error(new MsException(UNKNOWN_ERROR, "AppendObject Get bucket: " + bucketName + " Object object:" + objName + " Meta Data fail"));
                                                    }
                                                    if (inode.getLinkN() == CAP_QUOTA_EXCCED_INODE.getLinkN()) {
                                                        return Mono.error(new MsException(NO_ENOUGH_SPACE, "No Enough Space Because of the fs quota."));
                                                    }
                                                    if (StringUtils.isNotBlank(request.getHeader(VERSIONID)) && !StringUtils.equals(request.getHeader(VERSIONID), inode.getVersionId())) {
                                                        return Mono.error(new MsException(OBJECT_NOT_APPENDABLE, "It is not the latest versionId."));
                                                    }
                                                    long wormExpire = getWormExpire(metaData[0]);
                                                    checkWormRetention(request, wormExpire, currentWormStamp[0]);
                                                    if (wormExpire != 0) {
                                                        return Mono.error(new MsException(OBJECT_NOT_APPENDABLE, "The object is not appendable."));
                                                    }
                                                    if (position != inode.getSize()) {
                                                        return Mono.error(new MsException(POSITION_NOT_EQUAL_TO_LENGTH,
                                                                "Bucket" + bucketName + "key " + objName + " Position:" + position + " is not equal to file length: " + metaData[0].getEndIndex() + 1 + ".."));
                                                    }

                                                    //追加写文件，此时判断文件本身是否有w权限
                                                    return S3ACL.checkInodeAppendACL(inode, bucketName, request.getUserId(), 7, startFsType[0])
                                                            .map(b -> inode);
                                                })
                                                .flatMap(inode -> {
                                                    String sysMetaData = metaData[0].getSysMetaData();
                                                    sysMetaMap[0] = new JsonObject(sysMetaData);
                                                    long oldLength = Long.parseLong(sysMetaMap[0].getString(CONTENT_LENGTH));
                                                    if (inode.getSize() + objectSize > MAX_UPLOAD_TOTAL_SIZE) {
                                                        return Mono.error(new MsException(OBJECT_SIZE_OVERFLOW, "object size too large."));
                                                    }
                                                    int partNum = 0;
                                                    for (Inode.InodeData inodeData : inode.getInodeData()) {
                                                        partNum += (inodeData.chunkNum == 0 ? 1 : inodeData.chunkNum);
                                                    }
                                                    partNum += 1;
                                                    if (partNum > MAX_UPLOAD_PART_NUM) {
                                                        return Mono.error(new MsException(OBJECT_NOT_APPENDABLE, "append object part amount too large."));
                                                    }
                                                    sysMetaMap[0].put(CONTENT_LENGTH, (oldLength + objectSize) + "")
                                                            .put(LAST_MODIFY, lastModify);
                                                    String uploadId = "append:" + partNum;
                                                    String fileName = Utils.getPartFileName(dataPool[0], bucketName, metaData[0].getKey(), uploadId, partNum + "", requestId);
                                                    partInfo.setPartNum(partNum + "")
                                                            .setStorage(metaData[0].getStorage())
                                                            .setUploadId(uploadId)
                                                            .setFileName(fileName)
                                                            .setVersionId(metaData[0].versionId);
                                                    return Mono.just(true).flatMap(list -> dataPool[0].mapToNodeInfo(dataPool[0].getObjectVnodeId(partInfo.getFileName())))
                                                            .flatMap(list -> PartClient.partUpload(dataPool[0], partInfo.getFileName(), list, request, partInfo, recoverDataProcessor,
                                                                    metaData[0].getCrypto(), false))
                                                            .flatMap(md5 -> {
                                                                if (StringUtils.isBlank(md5)) {
                                                                    return Mono.error(new MsException(UNKNOWN_ERROR, "put file fail"));
                                                                }
                                                                if (contentMD5 != null) {
                                                                    checkMd5(contentMD5, md5);
                                                                }
                                                                partInfo.setEtag(md5);
                                                                metaData[0].setEndIndex(inode.getSize() + partInfo.getPartSize() - 1);
                                                                inodes[0] = inode;
                                                                Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(false));
                                                                return Mono.just(true);
                                                            });
                                                });
                                    }
                                }
                                return putMono
                                        .flatMap(f -> {
                                            if (f && !InodeUtils.isError(inodes[0])) {
                                                Inode.InodeData inodeData = new Inode.InodeData()
                                                        .setSize(partInfo.getPartSize())
                                                        .setStorage(dataPool[0].getVnodePrefix())
                                                        .setOffset(0L)
                                                        .setEtag(partInfo.getEtag())
                                                        .setFileName(partInfo.fileName);
                                                return Node.getInstance().updateInodeData(bucketName, inodes[0].getNodeId(), inodes[0].getSize(), inodeData, "", "", 1,
                                                        inodes[0].getXAttrMap().get(QUOTA_KEY))
                                                        .flatMap(i -> {
                                                            if (isError(i)) {
                                                                return Mono.error(new MsException(UNKNOWN_ERROR, "put meta data error!"));
                                                            }
                                                            recoverDataProcessor.onNext(true);
                                                            EsMeta esMeta = new EsMeta();
                                                            esMeta.setUserId(user)
                                                                    .setSysMetaData(sysMetaMap[0].encode())
                                                                    .setUserMetaData(userMeta.encode())
                                                                    .setBucketName(bucketName)
                                                                    .setObjName(objName)
                                                                    .setVersionId(metaData[0].getVersionId())
                                                                    .setStamp(String.valueOf(inodes[0].getMtime() * 1000 + inodes[0].getMtimensec() / 1_000_000))
                                                                    .setObjSize(String.valueOf(metaData[0].getEndIndex() + 1))
                                                                    .setInode(metaData[0].inode);
                                                            return InodeUtils.updateSpeciDirTime(dirInodes[0], objName, bucketName)
                                                                    .flatMap(ino -> {
                                                                        Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(true));
                                                                        if (!"off".equals(msg.get("mda"))) {
                                                                            MonoProcessor<Boolean> esRes = MonoProcessor.create();
                                                                            Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> EsMetaTask.overWriteEsMeta(esMeta, inodes[0], false)).subscribe(esRes::onNext);
                                                                            return esRes;
                                                                        } else {
                                                                            return Mono.just(true);
                                                                        }
                                                                    });
                                                        });
                                            }
                                            return Mono.error(e);
                                        });
                            }

                        }

                    }
                    return Mono.error(e);
                })
                .subscribe(b -> {
                    try {
                        QuotaRecorder.addCheckBucket(bucketName);
                        addPublicHeaders(request, requestId)
                                .putHeader(CONTENT_LENGTH, "0")
                                .putHeader(META_ETAG, '"' + partInfo.getEtag() + '"')
                                .putHeader(X_AMX_VERSION_ID, metaData[0].versionId)
                                .putHeader(NEXT_APPEND_POSITION, (metaData[0].getEndIndex() + 1) + "");
                        CryptoUtils.addCryptoResponse(request, metaData[0].getCrypto());
                        addAllowHeader(request.response()).end();
                    } catch (Exception e) {
                        logger.error("", e);
                    }
                }, e -> {
                    if (e instanceof NFSException) {
                        NFSException nfsException = (NFSException) e;
                        if (nfsException.getErrCode() == NfsErrorNo.NFS3ERR_DQUOT) {
                            e = new MsException(NO_ENOUGH_OBJECTS, "No objects enough because of the fs quota.");
                        }
                    }
                    dealException(requestId, request, e, false);
                });

        request.addResponseCloseHandler(v -> disposable.dispose());

        return ErrorNo.SUCCESS_STATUS;
    }

}
