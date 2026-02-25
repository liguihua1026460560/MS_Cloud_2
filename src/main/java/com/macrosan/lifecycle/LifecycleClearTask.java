package com.macrosan.lifecycle;

import com.alibaba.fastjson.JSONObject;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.LOCAL_IP_ADDRESS;
import static com.macrosan.constants.SysConstants.ROCKS_DEDUPLICATE_KEY;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.lifecycle.mq.LifecycleChannels.publishCommand;


public class LifecycleClearTask {

    public static final Logger logger = LogManager.getLogger(LifecycleCommandConsumer.class.getName());
    private static final String MQ_KEY_PREFIX = "life_move_";
    static MSRocksDB mqDB = MSRocksDB.getRocksDB(Utils.getMqRocksKey());

    public static void init() {
        lifecycleClearTask();
    }

    private static void lifecycleClearTask() {
        UnicastProcessor<Tuple2<byte[], byte[]>> res = UnicastProcessor.create();
        Flux.just(1).publishOn(DISK_SCHEDULER)
                .flatMap(b -> iterator(res))
                .flatMap(LifecycleClearTask::runTask)
                .doFinally(b -> {
                    res.cancel();
                    DISK_SCHEDULER.schedule(LifecycleClearTask::lifecycleClearTask, 10L, TimeUnit.SECONDS);
                })
                .subscribe();
    }

    private static Flux<Tuple2<byte[], byte[]>> iterator(UnicastProcessor<Tuple2<byte[], byte[]>> res) {
        next(MQ_KEY_PREFIX, res);
        return res;
    }

    public static void next(String prefix, UnicastProcessor<Tuple2<byte[], byte[]>> res) {
        if (res.isDisposed()) {
            return;
        }

        boolean end = false;
        int count = 0;

        if (res.size() < 100) {
            try (MSRocksIterator iterator = mqDB.newIterator()) {
                iterator.seek(prefix.getBytes());
                while (iterator.isValid() && count < 1000) {
                    String key = new String(iterator.key());
                    if (key.startsWith(prefix)) {
                        res.onNext(new Tuple2<>(iterator.key(), iterator.value()));
                        count++;
                    } else {
                        end = true;
                        break;
                    }
                    iterator.next();
                }

                if (count < 1000) {
                    end = true;
                }
            }

        }

        if (!end) {
            DISK_SCHEDULER.schedule(() -> next(prefix, res), 1L, TimeUnit.SECONDS);
        } else {
            res.onComplete();
        }
    }

    private static Mono<Boolean> runTask(Tuple2<byte[], byte[]> tuple2) {
        byte[] taskKey = tuple2.var1;
        byte[] taskValue = tuple2.var2();
        String value = new String(taskValue);

        JSONObject jsonObject = JSONObject.parseObject(value);
        String bucket = jsonObject.getString("bucket");
        String object = jsonObject.getString("object");
        String versionId = jsonObject.getString("versionId");
        String storage = jsonObject.getString("storage");
        String fileNameStr = jsonObject.getString("fileName");
        String storageStrategy = jsonObject.getString("storageStrategy");
        final String snapshotMark=jsonObject.getString("snapshotMark");
        String fileName = fileNameStr.substring(1, fileNameStr.length() - 1);
        String[] fileNameArr = fileName.split(", ");
        boolean dedup = fileName.contains(ROCKS_DEDUPLICATE_KEY);
      //是否是重删记录
//        boolean dedup = fileNameArr[0].startsWith(ROCKS_DEDUPLICATE_KEY);
        Set<String> allFile = new HashSet<>();
        for (String files : fileNameArr){
                if(!files.startsWith(ROCKS_DEDUPLICATE_KEY)){
                    allFile.add(files);
                }
        }

        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucket);
        final Tuple2<String, String> bucketVnodeIdTuple = StoragePoolFactory.getMetaStoragePool(bucket).getBucketVnodeIdTuple(bucket, object);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnodeId = bucketVnodeIdTuple.var2;
        return metaStoragePool.mapToNodeInfo(bucketVnode)
                .flatMap(bucketNodeList -> ErasureClient.getLifecycleMetaVersion(bucket, object, versionId, bucketNodeList, null, snapshotMark))
                .timeout(Duration.ofSeconds(30))
                .zipWith((StringUtils.isNotBlank(migrateVnodeId) ? obtainMigrationResults(metaStoragePool, migrateVnodeId, bucket, object, versionId, snapshotMark)
                .flatMap(meta -> {
                    if (meta.equals(MetaData.ERROR_META)) {
                        return Mono.just(new Tuple2<>(true, false));
                    }
                    boolean flag = true;
                    if (meta.equals(MetaData.NOT_FOUND_META) || meta.deleteMark || meta.deleteMarker) {
                        flag = false;
                    } else {
                        if (meta.partUploadId != null) {
                            for (PartInfo partInfo : meta.partInfos) {
                                flag = fileName.contains(partInfo.fileName);
                                if(dedup && StringUtils.isNotEmpty(partInfo.deduplicateKey)){
                                    flag = fileName.contains(partInfo.deduplicateKey);
                                }
                                if (flag) {
                                    break;
                                }
                            }
                        } else {
                            flag = fileName.equals(meta.fileName);
                            if (dedup && StringUtils.isNotEmpty(meta.duplicateKey)) {
                                flag = fileName.equals(meta.duplicateKey);
                            }
                        }
                    }
                    return Mono.just(new Tuple2<>(false, flag));
                }) : Mono.just(new Tuple2<>(false, false))))
                .flatMap(tuple21 -> {
                    MetaData metaData = tuple21.getT1();
                    Tuple2<Boolean, Boolean> migrationResults = tuple21.getT2();
                    if (metaData.equals(MetaData.ERROR_META) || migrationResults.var1) {
                        return Mono.just(false);
                    }
                    boolean flag = true;
                    if (metaData.equals(MetaData.NOT_FOUND_META) || metaData.deleteMark || metaData.deleteMarker) {
                        flag = false;
                    } else {
                        if (metaData.partUploadId != null) {
                            for (PartInfo partInfo : metaData.partInfos) {
                                flag = fileName.contains(partInfo.fileName);
                                if(dedup && StringUtils.isNotEmpty(partInfo.deduplicateKey)){
                                    flag = fileName.contains(partInfo.deduplicateKey);
                                }
                                if (flag) {
                                    break;
                                }
                            }
                        } else {
                            flag = fileName.equals(metaData.fileName);
                            if (dedup && StringUtils.isNotEmpty(metaData.duplicateKey)) {
                                flag = fileName.equals(metaData.duplicateKey);
                            }
                        }
                    }
                    if (!flag && !migrationResults.var2) {
                        if(dedup){
                            StoragePool sourcePool = StoragePoolFactory.getMetaStoragePool(bucket);
                            return ErasureClient.deleteDedupObjectFile(sourcePool, fileNameArr, null, true)
                                    .flatMap(b -> {
                                      if(b && !allFile.isEmpty()){
                                          StoragePool sourceObjPool = StoragePoolFactory.getStoragePool(storage, bucket);
                                          return ErasureClient.lifecycleDeleteFile(sourceObjPool, allFile.toArray(new String[0]), null);
                                      }
                                      return Mono.just(b);
                                    })
                                    .doOnNext(b -> {
                                        if (b) {
                                            deleteMq(taskKey);
                                        }
                                    });
                        }
                        StoragePool sourcePool = StoragePoolFactory.getStoragePool(storage, bucket);
                        return ErasureClient.lifecycleDeleteFile(sourcePool, fileNameArr, null)  // 生命周期删除限流
                                .doOnNext(b -> {
                                    if (b) {
                                        deleteMq(taskKey);
                                    }
                                });
                    } else {
                        JsonObject json = new JsonObject();
                        json.put("bucket", bucket);
                        json.put("object", object);
                        json.put("targetStorageStrategy", storageStrategy);
                        json.put("versionId", versionId);
                        Optional.ofNullable(snapshotMark).ifPresent(v->json.put("snapshotMark",snapshotMark));
                        try {
                            publishCommand(json, LOCAL_IP_ADDRESS);
                            logger.debug("restart produceObject {}!!!", object);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        deleteMq(taskKey);
                        return Mono.just(false);
                    }
                })
                .doOnError(logger::error);
    }

    private static Mono<MetaData> obtainMigrationResults(StoragePool pool, String migrateVnodeId, String bucket, String object, String versionId,String snapshotMark) {
        return pool.mapToNodeInfo(migrateVnodeId)
                .flatMap(nodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(bucket, object, versionId, nodeList, null,snapshotMark,null))
                .flatMap(tuple2 -> Mono.just(tuple2.var1))
                .timeout(Duration.ofSeconds(30));
    }

    private static void deleteMq(byte[] key) {
        try {
            mqDB.delete(key);
        } catch (Exception e) {
            logger.error("", e);
        }
    }

}
