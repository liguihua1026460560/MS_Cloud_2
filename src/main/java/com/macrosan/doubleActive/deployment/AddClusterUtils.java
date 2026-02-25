package com.macrosan.doubleActive.deployment;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ServerConstants;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.doubleActive.MainNodeSelector;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ListMultipartUploadsResult;
import com.macrosan.message.xmlmsg.section.Upload;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.functional.Tuple3;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.deployment.AddClusterHandler.*;
import static com.macrosan.ec.Utils.ZERO_STR;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_SYNC_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.DEAL_HIS_CHECKPOINT;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.IF_PUT_DONE;
import static com.macrosan.httpserver.MossHttpClient.*;

@Log4j2
public class AddClusterUtils {
    static void preDealObj(int clusterIndex, String bucketVnode, MetaData metaData) {
        String rocksKey = Utils.getVersionMetaDataKey(bucketVnode, metaData.bucket, metaData.key, metaData.versionId);
        deploysMap.get(clusterIndex).dealingObjMap.computeIfAbsent(metaData.bucket + "_" + bucketVnode, v -> new ConcurrentSkipListMap<>()).put(rocksKey, metaData);
    }

    static void finishDealObj(int clusterIndex, String bucketVnode, MetaData metaData) {
        String rocksKey = Utils.getVersionMetaDataKey(bucketVnode, metaData.bucket, metaData.key, metaData.versionId);
        Map<String, MetaData> dealingObjMap = deploysMap.get(clusterIndex).dealingObjMap.get(metaData.bucket + "_" + bucketVnode);
        dealingObjMap.remove(rocksKey);
        //该桶下没有待处理obj且objList也结束了，认为该桶的历史数据record已全部落盘。
        if (dealingObjMap.isEmpty() && !COM_SYNCING_BUCKET.get(clusterIndex).contains(metaData.bucket)) {
            Map<Integer, Integer> clusterStatusMap = COM_BUCKETS_STATUS_MAP.get(metaData.bucket);
            synchronized (clusterStatusMap) {
                clusterStatusMap.put(clusterIndex, 1);
                Mono.just(true).publishOn(SCAN_SCHEDULER)
                        .subscribe(s -> RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                                .hset(metaData.bucket, obj_his_sync_finished, Json.encode(clusterStatusMap)));
            }
        }
    }

    static void preDealPartList(int clusterIndex, String bucketName, String bucketVnode, Upload upload) {
        String initPartKey = InitPartInfo.getPartKey("", bucketName, upload.getKey(), upload.getUploadId());
        deploysMap.get(clusterIndex).dealingPartMap.computeIfAbsent(bucketName + "_" + bucketVnode, v -> new ConcurrentSkipListMap<>()).put(initPartKey, upload.getKey());
    }

    /**
     * part_multi_list中的一条Upload已经都处理完成（part_init、part_upload相关的record均已落盘）时调用。
     */
    static void finishDealPartList(int clusterIndex, String bucketName, String bucketVnode, Upload upload) {
        Map<String, String> dealingPartMap = deploysMap.get(clusterIndex).dealingPartMap.get(bucketName + "_" + bucketVnode);
        String initPartKey = InitPartInfo.getPartKey("", bucketName, upload.getKey(), upload.getUploadId());
        dealingPartMap.remove(initPartKey);
        //该桶下没有待处理obj且objList也结束了，认为该桶的历史数据record已全部落盘。
        if (dealingPartMap.isEmpty() && !PART_SYNCING_BUCKET.get(clusterIndex).contains(bucketName)) {
            Map<Integer, Integer> clusterStatusMap = PART_BUCKETS_STATUS_MAP.get(bucketName);
            synchronized (clusterStatusMap) {
                clusterStatusMap.put(clusterIndex, 1);
                Mono.just(true).publishOn(SCAN_SCHEDULER)
                        .subscribe(s -> RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                                .hset(bucketName, part_his_sync_finished, Json.encode(clusterStatusMap)));
            }
        }
    }

    static void preDealRecord(int clusterIndex, String bucketName, UnSynchronizedRecord record, boolean metaScan) {
        Map<String, Map<String, String>> dealingRecordMap;
        if (metaScan) {
            dealingRecordMap = deploysMap.get(clusterIndex).dealingOldRecordMap;
        } else {
            dealingRecordMap = deploysMap.get(clusterIndex).dealingRecordMap;
        }

        dealingRecordMap.computeIfAbsent(bucketName, v -> new ConcurrentSkipListMap<>()).put(record.rocksKey(), "rocksKey");
    }

    static void finishDealRecord(int clusterIndex, String bucketName, UnSynchronizedRecord record, boolean metaScan) {
        long dataSize = Long.parseLong(record.headers.getOrDefault(CONTENT_LENGTH, "0"));

        Map<String, Map<String, String>> dealingRecordMap;
        String redisStr;
        Map<Integer, Set<String>> syncingBucketMap;
        Map<String, Map<Integer, Integer>> bucketStatusMap;
        if (metaScan) {
            dealingRecordMap = deploysMap.get(clusterIndex).dealingOldRecordMap;
            redisStr = old_record_his_sync_finished;
            syncingBucketMap = OLD_REC_SYNCING_BUCKET;
            bucketStatusMap = OLD_REC_BUCKETS_STATUS_MAP;
        } else {
            dealingRecordMap = deploysMap.get(clusterIndex).dealingRecordMap;
            redisStr = record_his_sync_finished;
            syncingBucketMap = REC_SYNCING_BUCKET;
            bucketStatusMap = REC_BUCKETS_STATUS_MAP;
        }

        Map<String, String> dealingRecMap = dealingRecordMap.get(bucketName);
        dealingRecMap.remove(record.rocksKey());
        if (dealingRecMap.isEmpty() && !syncingBucketMap.get(clusterIndex).contains(bucketName)) {
            Map<Integer, Integer> clusterStatusMap = bucketStatusMap.get(bucketName);
            synchronized (clusterStatusMap) {
                clusterStatusMap.put(clusterIndex, 1);
                Mono.just(true).publishOn(SCAN_SCHEDULER)
                        .subscribe(s -> RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                                .hset(bucketName, redisStr, Json.encode(clusterStatusMap)));
            }
        }
    }

    public static void putRecordToMq(UnSynchronizedRecord record) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(record.bucket);
//        String strategyName = "storage_" + pool.getVnodePrefix();
//        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolQueueTag)) {
//            String strategyName = "storage_" + pool.getVnodePrefix();
//            poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("value", Json.encode(record))
                .put("poolQueueTag", poolQueueTag)
                .put("force", "1");
        ObjectPublisher.basicPublish(ServerConfig.getInstance().getHeartIp1(), msg, ERROR_PUT_SYNC_RECORD);
    }

    /**
     * 存放userId和临时AuthString。
     */
    static Map<String, String> tempAuthMap = new ConcurrentHashMap<>();

    /**
     * 生成poolingRequest使用的临时auth string。是为了在计算签名时提供AK。
     */
    public static String getTempAuthStr(String userId) {
        if (tempAuthMap.containsKey(userId)) {
            return tempAuthMap.get(userId);
        } else {
            String ak = RedisConnPool.getInstance().getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_AK1);
            if (StringUtils.isEmpty(ak) || "null".equals(ak)) {
                ak = RedisConnPool.getInstance().getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_AK2);
            }
            String format = String.format("AWS %s:000", ak);
            tempAuthMap.put(userId, format);
            return format;

        }
    }

    public static UnSynchronizedRecord buildSyncRecord(int sendClusterIndex, HttpMethod method, MetaData metaData) {
        return buildSyncRecord(sendClusterIndex, method, metaData, false);
    }

    public static UnSynchronizedRecord buildSyncRecord(int sendClusterIndex, HttpMethod method, MetaData metaData, boolean newDelRocksKey) {
        UnSynchronizedRecord record = new UnSynchronizedRecord();
        String pathStr;
        Map<String, String> headers = new HashMap<>();
        record.setHeaders(headers);
        headers.put(CLUSTER_ALIVE_HEADER, "ip");
        headers.put(SYNC_STAMP, metaData.getSyncStamp());
        headers.put("stamp", metaData.stamp);
        headers.put(VERSIONID, metaData.versionId);
        Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
        });
        headers.put(AUTHORIZATION, getTempAuthStr(sysMap.get("owner")));

        headers.put(EXPECT, EXPECT_100_CONTINUE);
        pathStr = UrlEncoder.encode(File.separator + metaData.bucket + File.separator + metaData.key, "UTF-8");

        record.setMethod(method);

        if (metaData.partUploadId != null) {
            record.headers.put(NEW_VERSION_ID, metaData.versionId);
        }

        long dataSize = metaData.endIndex - metaData.startIndex + 1;
        headers.put(CONTENT_LENGTH, HttpMethod.DELETE.equals(method) ? "0" : String.valueOf(dataSize));

        record.setBucket(metaData.bucket)
                .setObject(metaData.key)
                .setVersionNum(metaData.getVersionNum())
                .setSyncStamp(metaData.getSyncStamp())
                .setUri(pathStr)
                .setVersionId(metaData.versionId)
                .setSuccessIndex(LOCAL_CLUSTER_INDEX)
                .setIndex(sendClusterIndex)
                .setCommited(true);

        if (HttpMethod.DELETE.equals(method) && !newDelRocksKey) {
            // 删除相关的差异记录的recordKey将使用源对象中的syncStamp拼接。为了后台校验能找到差异记录。SERVER-1107
            if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(sendClusterIndex)) {
                record.rocksKeyAsync(sendClusterIndex);
            } else {
                record.rocksKey();
            }
        } else {
            if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(sendClusterIndex)) {
                record.hisRocksKeyAsync(sendClusterIndex, UnSynchronizedRecord.versionNum2syncStamp(record.syncStamp));
            } else {
                record.hisRocksKey(UnSynchronizedRecord.versionNum2syncStamp(record.syncStamp));
            }
        }

        return record;
    }

    private static UnSynchronizedRecord buildPartInitSyncRecord(int sendClusterIndex, String bucketName, Upload upload) {
        UnSynchronizedRecord record = new UnSynchronizedRecord();
        String pathStr;
        Map<String, String> headers = new HashMap<>();
        record.setHeaders(headers);
        headers.put(CLUSTER_ALIVE_HEADER, "ip");
        headers.put(AUTHORIZATION, getTempAuthStr(upload.getOwner().getId()));

        headers.put(EXPECT, EXPECT_100_CONTINUE);
        pathStr = UrlEncoder.encode(File.separator + bucketName + File.separator + upload.getKey(), "UTF-8") + "?uploads";

        record.setMethod(HttpMethod.POST);

        return record
                .setBucket(bucketName)
                .setObject(upload.getKey())
                .setVersionNum(VersionUtil.getVersionNum(false))
                .setSyncStamp(VersionUtil.getVersionNum(false))
                .setUri(pathStr)
                .setSuccessIndex(LOCAL_CLUSTER_INDEX)
                .setIndex(sendClusterIndex);
    }

    static UnSynchronizedRecord buildPartSyncRecord(int sendClusterIndex, String bucketName, Upload upload, PartInfo partInfo) {
        UnSynchronizedRecord record = new UnSynchronizedRecord();
        String pathStr;
        Map<String, String> headers = new HashMap<>();
        record.setHeaders(headers);
        headers.put(CLUSTER_ALIVE_HEADER, "ip");
        headers.put(AUTHORIZATION, getTempAuthStr(upload.getOwner().getId()));
        headers.put(UPLOAD_ID, upload.getUploadId());

        headers.put(EXPECT, EXPECT_100_CONTINUE);
        if (partInfo == null) {
            pathStr = UrlEncoder.encode(File.separator + bucketName + File.separator + upload.getKey(), "UTF-8") + "?uploads";
            record.setMethod(HttpMethod.POST);
            record.setSyncStamp(VersionUtil.getVersionNum(false));
        } else {
            long dataSize = partInfo.partSize;
            headers.put(CONTENT_LENGTH, String.valueOf(dataSize));
            pathStr = UrlEncoder.encode(File.separator + bucketName + File.separator + upload.getKey(), "UTF-8") + "?partNumber=" + partInfo.partNum + "&uploadId=" + upload.getUploadId();
            record.setMethod(HttpMethod.PUT);
            record.setSyncStamp(partInfo.syncStamp);
            headers.put(GET_SINGLE_PART, upload.getUploadId() + "," + partInfo.partNum);
        }

        record.setBucket(bucketName)
                .setObject(upload.getKey())
                .setVersionNum(VersionUtil.getVersionNum(false))
                .setUri(pathStr)
                .setSuccessIndex(LOCAL_CLUSTER_INDEX)
                .setIndex(sendClusterIndex);
        if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(sendClusterIndex)) {
            record.rocksKeyAsync(sendClusterIndex);
        } else {
            record.rocksKey();
        }
        return record;
    }

    /**
     * 扫描完成后执行，更改桶同步状态为已同步。
     * 注意执行到此并非表示历史数据同步的record已写入完成，只是表示list或part_multi_list这一步结束了。
     *
     * @param bucket       桶名
     * @param clusterIndex 待同步的站点
     * @param redisStr     表7桶下的redis字段，obj_his_sync_finished或part_his_sync_finished
     */
    static synchronized void endList(String bucket, int clusterIndex, String redisStr) {
        Map<Integer, Set<String>> listingMap;
        switch (redisStr) {
            case obj_his_sync_finished:
                listingMap = COM_SYNCING_BUCKET;
                break;
            case part_his_sync_finished:
                listingMap = PART_SYNCING_BUCKET;
                break;
            case record_his_sync_finished:
                listingMap = REC_SYNCING_BUCKET;
                break;
            case old_record_his_sync_finished:
                listingMap = OLD_REC_SYNCING_BUCKET;
                break;
            default:
                listingMap = new HashMap<>();
        }
        listingMap.get(clusterIndex).remove(bucket);
        if (COM_SYNCING_BUCKET.get(clusterIndex).contains(bucket) || PART_SYNCING_BUCKET.get(clusterIndex).contains(bucket)
                || REC_SYNCING_BUCKET.get(clusterIndex).contains(bucket) || OLD_REC_SYNCING_BUCKET.get(clusterIndex).contains(bucket)) {
            return;
        }

        syncingBucketNum.decrementAndGet();
        log.info("addcluster endList: {}, {}, {}", bucket, clusterIndex, syncingBucketNum);
    }

    static boolean allClusterFinished() {
        // 历史同步同步状态和待历史同步的站点数量不匹配，表示有站点的历史同步还未初始化过。
        if (INDEX_HIS_SYNC_MAP.size() != CLUSTERS_AMOUNT - 1) {
            return false;
        }

        for (Map.Entry<Integer, Integer> entry : INDEX_HIS_SYNC_MAP.entrySet()) {
            Integer index = entry.getKey();
            Integer status = entry.getValue();
            if (index == -1) {
                continue;
            }
            if (status != 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * 去每个非async站点获取该index的历史同步状态是否为待同步。拿到httpResponse前不会停止。
     *
     * @param index          在初始化历史数据同步的站点
     * @param escapeIndexSet 同时添加多个双活站点时，也不需要互相发check历史数据状态请求
     */
    static Mono<Boolean> checkHisSyncStatus(int index, Set<Integer> escapeIndexSet) {
        Set<Integer> set = new HashSet<>(INDEX_IPS_ENTIRE_MAP.keySet());
        // async站点和待同步的站点不发送check历史数据状态请求
        set.removeAll(ASYNC_INDEX_IPS_ENTIRE_MAP.keySet());
        set.removeAll(escapeIndexSet);
        if (set.size() == 0) {
            return Mono.just(true);
        }

        AtomicInteger count = new AtomicInteger();
        MonoProcessor<Boolean> res = MonoProcessor.create();

        for (Integer i : set) {
            AtomicInteger tryNum = new AtomicInteger();
            UnicastProcessor<Integer> retryProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
            retryProcessor.publishOn(SCAN_SCHEDULER).subscribe(ipIndex -> {
                // 防止切换扫描节点时无法结束进程
                if (!MainNodeSelector.checkIfSyncNode()) {
                    retryProcessor.onComplete();
                    res.onError(new RuntimeException("Stop History sync"));
                }
                if (tryNum.incrementAndGet() > 10) {
                    retryProcessor.onComplete();
                    res.onError(new RuntimeException("Stop History sync"));
                }

                String[] ips = INDEX_IPS_ENTIRE_MAP.get(i);
                String ip = ips[ipIndex];
                int nextIpIndex = (ipIndex + 1) % ips.length;
                HttpClientRequest request = getClient().request(HttpMethod.GET, DA_PORT, ip, "?indexHisSync");
                request.putHeader(SYNC_AUTH, PASSWORD)
                        .putHeader(index_his_sync, String.valueOf(index))
                        .setTimeout(10_000)
                        .exceptionHandler(e -> {
                            log.error("checkHisSyncStatus request error, ", e);
                            request.reset();
                            retryProcessor.onNext(nextIpIndex);
                        })
                        .handler(resp -> {
                            if (resp.statusCode() == ServerConstants.SUCCESS) {
                                log.info("checkHisStyncStatus of new cluster {} from {}: {}", index, i, resp.getHeader(index_his_sync));
                                if (resp.getHeader(index_his_sync).compareTo("0") >= 0) {
                                    if (count.incrementAndGet() == set.size()) {
                                        retryProcessor.onComplete();
                                        res.onNext(true);
                                    }
                                } else {
                                    Mono.delay(Duration.ofSeconds(5)).subscribe(s -> retryProcessor.onNext(nextIpIndex));
                                }
                            } else {
                                Mono.delay(Duration.ofSeconds(5)).subscribe(s -> retryProcessor.onNext(nextIpIndex));
                            }
                        })
                        .end();
            });
            retryProcessor.onNext(ThreadLocalRandom.current().nextInt(INDEX_IPS_ENTIRE_MAP.get(i).length));
        }
        return res;
    }

    /**
     * 获取索引为0的站点的历史同步是否为待同步。
     *
     * @return
     */
    static Mono<Boolean> checkMasterHisSyncStatus(int index) {
        Set<Integer> set = new HashSet<>(INDEX_IPS_ENTIRE_MAP.keySet());
        set.removeAll(ASYNC_INDEX_IPS_ENTIRE_MAP.keySet());
        set.remove(0);
        return checkHisSyncStatus(index, set);
    }


    /**
     * 主节点发起，同一站点的节点都去创建checkPoint。服务端收到该socket请求开始init就返回成功，是否创建完成由客户端（主节点）发起后续流程检查
     */
    public static Mono<Boolean> createCheckPointAllNodes(Integer index) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        Disposable[] disposables = new Disposable[2];
        UnicastProcessor<Integer> retryProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        retryProcessor.publishOn(SCAN_SCHEDULER).subscribe(i -> {
            disposables[0] = ScanStream.scan(RedisConnPool.getInstance().getReactive(REDIS_NODEINFO_INDEX))
                    .flatMap(node -> RedisConnPool.getInstance().getReactive(REDIS_NODEINFO_INDEX).hget(node, SysConstants.HEART_IP))
                    .collectList()
                    .map(list -> {
                        List<Tuple3<String, String, String>> nodeList = new ArrayList<>(list.size());
                        for (String ip : list) {
                            Tuple3<String, String, String> tuple3 = new Tuple3<>(ip, "", "");
                            nodeList.add(tuple3);
                        }
                        return nodeList;
                    })
                    .subscribe(list -> {
                        List<SocketReqMsg> msgs = list.stream()
                                .map(t -> new SocketReqMsg("", 0)
                                        .put("index", String.valueOf(index)))
                                .collect(Collectors.toList());
                        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DEAL_HIS_CHECKPOINT, String.class, list);
                        disposables[1] = responseInfo.responses.subscribe(s -> {
                        }, e -> log.error("", e), () -> {
                            if (responseInfo.successNum == list.size()) {
                                log.info("Send create checkpoint to all nodes complete. ");
                                res.onNext(true);
                                retryProcessor.onComplete();
                            } else {
                                Mono.delay(Duration.ofSeconds(5)).subscribe(s -> retryProcessor.onNext(1));
                                DoubleActiveUtil.streamDispose(disposables);
                            }
                        });
                    });

        });
        retryProcessor.onNext(1);
        return res;
    }

    /**
     * map，记录每个节点下对相关index的历史数据同步的checkpoint是否建立，格式为[index_uuid, status]
     */
    public static final String node_his_sync = "node_his_sync";

    /**
     * 主节点检查同一个站点的其他节点有没有完成checkPoint建立
     */
    static Mono<Boolean> checkAllNodesCheckPoint(int index) {
        UnicastProcessor<Integer> retryProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ScanArgs args = new ScanArgs().match(index + "_*");

        retryProcessor.publishOn(SCAN_SCHEDULER)
                .flatMap(i -> RedisConnPool.getInstance().getReactive(REDIS_SYSINFO_INDEX).hscan(node_his_sync, args))
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(cursor -> {
                    Map<String, String> map = cursor.getMap();
                    if (map.size() == LOCAL_NODE_AMOUNT) {
                        boolean b = true;
                        for (Map.Entry<String, String> entry : map.entrySet()) {
                            String v = entry.getValue();
                            if (!"1".equals(v)) {
                                b = false;
                                break;
                            }
                        }
                        if (b) {
                            log.info("All nodes create checkPoint done. ");
                            res.onNext(true);
                            retryProcessor.onComplete();
                            return;
                        }
                    }
                    Mono.delay(Duration.ofSeconds(5)).subscribe(s -> retryProcessor.onNext(1));
                }).subscribe();
        retryProcessor.onNext(1);
        return res;
    }

    static AtomicInteger u = new AtomicInteger();

    public static ListMultipartUploadsResult getUploadList(List<ListMultipartUploadsResult> listMultipartUploadsResults, int maxUploadsInt) {
        ListMultipartUploadsResult listMultipartUploadsResult = new ListMultipartUploadsResult();
        List<Upload> list = new ArrayList<>();
        for (ListMultipartUploadsResult multipartUploadsResult : listMultipartUploadsResults) {
            if (!multipartUploadsResult.getUploads().isEmpty()) {
                list.addAll(multipartUploadsResult.getUploads());
            }
        }
        quickSort(list, 0, list.size() - 1);
        if (list.size() < maxUploadsInt) {
            listMultipartUploadsResult.setTruncated(false);
            listMultipartUploadsResult.setUploads(list);
        } else {
            // 对 listMultipartUploadsResult 进行快速排序
            listMultipartUploadsResult.setTruncated(true);
            Upload upload = list.get(maxUploadsInt - 1);
            listMultipartUploadsResult.setNextKeyMarker(upload.getKey());
            listMultipartUploadsResult.setNextUploadIdMarker(upload.getUploadId());
            listMultipartUploadsResult.setUploads(list.subList(0, maxUploadsInt));
        }
        return listMultipartUploadsResult;
    }

    public static void quickSort(List<Upload> uploads, int low, int high) {
        if (low < high) {
            // 获取分区索引
            int partitionIndex = partition(uploads, low, high);

            // 对左右子数组递归排序
            quickSort(uploads, low, partitionIndex - 1);  // 排序左子数组
            quickSort(uploads, partitionIndex + 1, high); // 排序右子数组
        }
    }

    private static int partition(List<Upload> uploads, int low, int high) {
        // 选择最后一个元素作为基准值
        Upload pivot = uploads.get(high);
        int i = (low - 1); // i表示较小元素的索引

        for (int j = low; j < high; j++) {
            // 如果当前元素小于或等于基准值（按key比较）
            if (getCompareKey(uploads.get(j)).compareTo(getCompareKey(pivot)) <= 0) {
                i++;

                // 交换uploads[i]和uploads[j]
                Upload temp = uploads.get(i);
                uploads.set(i, uploads.get(j));
                uploads.set(j, temp);
            }
        }

        // 交换uploads[i+1]和基准值uploads[high]
        Upload temp = uploads.get(i + 1);
        uploads.set(i + 1, uploads.get(high));
        uploads.set(high, temp);

        return i + 1; // 返回分区索引
    }

    private static String getCompareKey(Upload upload) {
        return upload.getKey() + ZERO_STR + upload.getUploadId();
    }

    public static class SyncRequest<T> {
        public T payload;
        public MonoProcessor<Boolean> res;
    }

    public static Mono<Integer> startCheckPut(String bucket, Map<String, String> map) {
        // 保证与开启桶开关的时间有一秒以上，防止checkAllNodesPutDone中的getVersionNum小于没有写差异记录的请求的versionNum
        // 等60s是防止出现：开桶开关前上传的对象请求，它的处理流程还未到记录下versionNum。
        if (map.containsKey(bucket)) {
            return Mono.just(1);
        }
        return Mono.just(1).delayElement(Duration.ofSeconds(60));
    }

    /**
     * 检查所有节点是否还有正在上传的对象或分段的版本号比初次开始历史扫描的lastversionNum更小，防止出现扫描完成时该次上传仍未结束造成的丢数据
     */
    public static Mono<Boolean> checkAllNodesPutDone(String bucket, Map<String, String> syncVerMap) {
        String versionNum = syncVerMap.get(bucket);
        if (StringUtils.isBlank(versionNum)) {
            versionNum = VersionUtil.getVersionNum();
            syncVerMap.put(bucket, versionNum);
        }

        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<String> ipList = NodeCache.getCache().keySet().stream().sorted().map(NodeCache::getIP).collect(Collectors.toList());
        List<Tuple3<String, String, String>> nodeList = new ArrayList<>(ipList.size());
        for (String ip : ipList) {
            Tuple3<String, String, String> tuple3 = new Tuple3<>(ip, "", "");
            nodeList.add(tuple3);
        }
        String finalVersionNum = versionNum;
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(t -> new SocketReqMsg("", 0)
                        .put("versionNum", finalVersionNum)
                        .put("bucket", bucket)
                )
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, IF_PUT_DONE, String.class, nodeList);
        Map<Boolean, AtomicInteger> map = new ConcurrentHashMap<>();
        responseInfo.responses
                .timeout(Duration.ofSeconds(30))
                .subscribe(s -> {
                    if (s.var2 == ErasureServer.PayloadMetaType.SUCCESS) {
                        boolean b = Boolean.parseBoolean(s.var3);
                        map.computeIfAbsent(b, k -> new AtomicInteger()).incrementAndGet();
                    }
                }, e -> {
                    log.error("", e);
                }, () -> {
                    if (map.get(false) == null && map.get(true).get() >= nodeList.size() / 2 + 1) {
                        log.info("checkAllNodesPutDone server success, {}, {}", bucket, responseInfo.res);
                        res.onNext(true);
                    } else {
                        log.info("checkAllNodesPutDone server fail, {}, {}", bucket, responseInfo.res);
                        res.onNext(false);
                    }
                });
        return res;

    }

    public static boolean addClusterIsRunning() {
        Map<String, String> indexHisSync = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hgetall(index_his_sync);
        if (indexHisSync == null) {
            return false;
        }
        for (String status : indexHisSync.values()) {
            if ("0".equals(status)) {
                log.info("add cluster process is running. ");
                return true;
            }
        }
        return false;
    }
}
