package com.macrosan.filesystem.quota;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.message.jsonmsg.FSQuotaConfig;
import com.macrosan.message.jsonmsg.FSQuotaScanResultVo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple3;
import io.lettuce.core.SetArgs;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.REDIS_FS_QUOTA_INFO_INDEX;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_SET_INTERVAL;

@Log4j2
public class FSQuotaScannerTask {

    protected static RedisConnPool redisConnPool = RedisConnPool.getInstance();
    public FSQuotaConfig config;
    public String quotaTaskKey;

    public FSQuotaScannerTask(FSQuotaConfig config) {
        this.config = config;
        this.quotaTaskKey = FSQuotaUtils.getQuotaTaskKey(config.bucket, config.nodeId, config.quotaType);
    }

    public static final String QUOTA_CACHE_ADD_OPT = "quota_cache_add";
    public static final String QUOTA_CACHE_MODIFY_OPT = "quota_cache_modify";

    public static final Set<String> SCAN_LOCK = new ConcurrentHashSet<>();

    public void startScan() {
        //防止扫描过久，恢复任务重复扫描
        if (!getScanLock(this.quotaTaskKey)) {
            FSQuotaRealService.removeQuotaScannerTask(this);
            return;
        }
        FSQuotaRealService.FS_QUOTA_EXECUTOR.schedule(() -> {
            syncQuotaCache(config.bucket, config, QUOTA_CACHE_ADD_OPT, config.getDirName(), config.nodeId).subscribe();
        });
        UnicastProcessor<String> scanProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        AtomicLong totalCount = new AtomicLong(0);
        AtomicLong totalCap = new AtomicLong(0);
        AtomicLong dirCount = new AtomicLong(0);
        long start = System.currentTimeMillis();
        scanProcessor
                .subscribe(marker -> {
                    StoragePool pool = StoragePoolFactory.getMetaStoragePool(config.getBucket());
                    String bucketVnode = pool.getBucketVnodeId(config.getBucket());
                    List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();
                    List<SocketReqMsg> msg = nodeList.stream()
                            .map(t -> new SocketReqMsg("", 0)
                                    .put("config", Json.encode(config))
                                    .put("marker", marker)
                                    .put("vnode", bucketVnode)
                                    .put("lun", t.var2))
                            .collect(Collectors.toList());
                    ClientTemplate.ResponseInfo<FSQuotaScanResultVo> responseInfo =
                            ClientTemplate.oneResponse(msg, QUOTA_SCAN, FSQuotaScanResultVo.class, nodeList);
                    String[] tmpMarker = new String[1];
                    AtomicLong tmpCount = new AtomicLong(0);
                    AtomicLong tmpDirCount = new AtomicLong(0);
                    AtomicLong tmpCap = new AtomicLong(0);
                    responseInfo.responses.subscribe(res -> {
                                if (SUCCESS.equals(res.var2)) {
                                    long count = res.var3.getFileCount();
                                    long capacity = res.var3.getFileTotalSize();
                                    long dirTmpCount = res.var3.dirCount;
                                    String newMarker = res.var3.getMarker();
                                    if (StringUtils.isBlank(tmpMarker[0])) {
                                        tmpMarker[0] = newMarker;
                                        tmpCount.set(count);
                                        tmpCap.set(capacity);
                                        tmpDirCount.set(dirTmpCount);
                                    } else {
                                        if (count > tmpCount.get()) {
                                            tmpMarker[0] = newMarker;
                                            tmpCount.set(count);
                                            tmpCap.set(capacity);
                                            tmpDirCount.set(dirTmpCount);
                                        }
                                    }
                                }
                            }, e -> {
                                log.error("scan error", e);
                                FSQuotaUtils.tryUnLock(this.quotaTaskKey);
                                FSQuotaRealService.removeQuotaScannerTask(this);
                                deleteScanLock(this.quotaTaskKey);
                            }, () -> {
                                if (responseInfo.successNum >= pool.getK()) {
                                    if (StringUtils.isNotBlank(tmpMarker[0])) {
                                        totalCount.addAndGet(tmpCount.get());
                                        totalCap.addAndGet(tmpCap.get());
                                        dirCount.addAndGet(tmpDirCount.get());
                                        scanProcessor.onNext(tmpMarker[0]);
                                    } else {
                                        long endStamp = System.currentTimeMillis();
                                        scanProcessor.onComplete();
                                        String quotaTypeKey = FSQuotaUtils.getQuotaTypeKey(this.config);
                                        Long exists = redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).exists(quotaTypeKey);
                                        if (exists != null && exists > 0) {
                                            log.info("scan complete type:{},uid:{},gid:{}, bucket:{},dirName:{} total count: {},dirCount:{},fileCount:{} total capacity: {},cost:{}", config.quotaType, config.getUid(), config.getGid(), config.bucket, config.dirName, totalCount.get(), dirCount.get(), totalCount.get() - dirCount.get(), totalCap.get(), endStamp - start);
                                            FSQuotaRealService.updateQuotaInfo(config.bucket, config.nodeId, totalCap.get(), totalCount.get(), this);
                                        }
                                        FSQuotaRealService.removeQuotaScannerTask(this);
                                        deleteScanLock(this.quotaTaskKey);
                                    }
                                } else {
                                    scanProcessor.onNext(marker);
                                }
                            }
                    );
                });
        scanProcessor.onNext("");
    }

    public boolean getScanLock(String quotaTaskKey) {
        SetArgs setArgs = new SetArgs().nx().ex(QUOTA_SET_INTERVAL);
        return !SCAN_LOCK.contains(quotaTaskKey) && "OK".equals(redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).set(quotaTaskKey, "1", setArgs));
    }

    public void deleteScanLock(String quotaTaskKey) {
        removeScanLocalLock(quotaTaskKey);
        redisConnPool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).del(quotaTaskKey);
    }

    public static Mono<Boolean> syncQuotaCache(String bucketName, FSQuotaConfig config, String opt, String dirName, long dirNodeId) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<Tuple3<String, String, String>> nodeList = NodeCache.getAllNodeIp();
        List<SocketReqMsg> msg = nodeList.stream()
                .map(t -> {
                    SocketReqMsg msg1 = new SocketReqMsg("", 0)
                            .put("bucket", bucketName)
                            .put("opt", opt)
                            .put("dirNodeId", String.valueOf(dirNodeId))
                            .put("dirName", dirName);
                    if (config != null) {
                        msg1.put("config", Json.encode(config));
                    }
                    return msg1;
                })
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<String> responseInfo =
                ClientTemplate.oneResponse(msg, SYNC_QUOTA_CACHE, String.class, nodeList);
        responseInfo.responses.subscribe(
                r -> {
                },
                e -> {
                    res.onNext(false);
                },
                () -> {
                    res.onNext(true);
                }
        );

        return res;
    }

    public static void addScanLocalLock(String quotaTaskKey) {
        SCAN_LOCK.add(quotaTaskKey);
    }

    public static void removeScanLocalLock(String quotaTaskKey) {
        SCAN_LOCK.remove(quotaTaskKey);
    }
}
