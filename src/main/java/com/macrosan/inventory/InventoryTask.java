package com.macrosan.inventory;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.inventory.datasource.DataSource;
import com.macrosan.inventory.datasource.InventoryManifest;
import com.macrosan.inventory.transmitter.Destination;
import com.macrosan.inventory.transmitter.InventoryTransmitter;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import com.macrosan.utils.quota.QuotaRecorder;
import io.vertx.core.Handler;
import io.vertx.reactivex.core.http.HttpClientResponse;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.function.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.inventory.InventoryService.INVENTORY_SCHEDULER;
import static com.macrosan.inventory.transmitter.InventoryTransmitter.MAX_PART_SIZE;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;

@Log4j2
public class InventoryTask implements Runnable {
    private static final RedisConnPool REDIS_CONN_POOL = RedisConnPool.getInstance();

    /**
     * InventoryAPI数据源,负责控制扫描器从rocksdb中读取对象元数据的速度
     */
    private final DataSource dataSource;

    /**
     * 用于将数据源传输至目标桶
     */
    private final InventoryTransmitter transmitter;

    /**
     * Inventory Manifest文件
     */
    private InventoryManifest manifest;

    /**
     * 返回任务执行结果
     */
    private final MonoProcessor<Boolean> executeResult = MonoProcessor.create();

    /**
     * 任务的Id
     */
    private final String inventoryId;

    /**
     * 当前任务启动的方式
     */
    public String type;

    /**
     * 返回任务的直接结果
     */
    public Mono<Boolean> res() {
        return executeResult;
    }

    InventoryTask(DataSource dataSource, InventoryManifest manifest, InventoryTransmitter transmitter, String inventoryId, String sourceBucket, String targetBucket, String targetObject) {
        this.dataSource = dataSource;
        this.manifest = manifest;
        this.transmitter = transmitter;
        this.inventoryId = inventoryId;
        this.sourceBucket = sourceBucket;
        this.targetBucket = targetBucket;
        this.targetObject = targetObject;
    }

    /**
     * 任务是否已经被初始化
     */
    private final AtomicBoolean isInit = new AtomicBoolean(false);

    private Disposable disposable = null;

    private int requestPort = 80;

    /**
     * 任务起始入口
     */
    private void start() {
        log.info("The bucket {} inventoryId {} task is started! rollback is {}.", sourceBucket, inventoryId, isRollback);
        AtomicBoolean isFirst = new AtomicBoolean(true);
        UnicastProcessor<String> partController = UnicastProcessor.create();
        String port = Optional.ofNullable(REDIS_CONN_POOL.getCommand(REDIS_SYSINFO_INDEX).get("http_port")).orElse("80");
        requestPort = Integer.parseInt(port);
        disposable = partController.doOnError(e -> {
            log.error("partController encounter exception. " + e.getMessage());
            dataSource.release();
            executeResult.onNext(false);
        }).doOnComplete(this::completePart).subscribe(l -> {

            if (isRollback && runningState.name().equals(ExecuteState.INVENTORY_FILE_ENDED.name())) {
                log.info("the bucket {} inventoryId {} all inventory files are generated.", sourceBucket, inventoryId);
                partController.onComplete();
                return;
            }

            Destination[] destinations = new Destination[]{null};
            StoragePool[] storagePool = new StoragePool[]{null};
            Disposable[] disposables = new Disposable[]{null};

            MonoProcessor<Boolean> next = MonoProcessor.create();

            if (!isInit.get()) {
                dataSource.start().subscribe(next::onNext);
                isInit.set(true);
            } else {
                next.onNext(true);
            }

            Map<String, String> headers = new HashMap<>();
            headers.put("x-amz-acl", PERMISSION_BUCKET_OWNER_FULL_CONTROL);
            headers.put(NO_SYNCHRONIZATION_KEY, NO_SYNCHRONIZATION_VALUE);
            headers.put(CLUSTER_ALIVE_HEADER, "ip");
            headers.put(CONTENT_TYPE, "text/csv");
            disposables[0] = next.flatMap(b -> {
                        if (!b) {
                            partController.onError(new UnsupportedOperationException("data source start failed."));
                        }
                        return buildDestination("_" + partNumber.getAndIncrement(), l);
                    })
                    .publishOn(INVENTORY_SCHEDULER)
                    .doOnNext(tuple2 -> headers.put(VERSIONID, tuple2.getT1().getVersionId()))
                    .doOnNext(tuple2 -> destinations[0] = tuple2.getT1())
                    .doOnNext(tuple2 -> storagePool[0] = tuple2.getT2())
                    // 生成临时数据块
                    .flatMap(tuple2 -> transmitter.transport(tuple2.getT1(), tuple2.getT2()))
                    .doOnNext(b -> {
                        if (b) {
                            // 每生成一个文件关闭rocksdb iterator,防止iterator被长时间使用导致磁盘分区容量无法释放
                            dataSource.scanner().release();
                        }
                    })
                    // 将临时数据块写入对目标桶中
                    .flatMap(b -> {
                        if (b) {
                            Handler<HttpClientResponse> handler = response -> {
                                if (response.statusCode() != SUCCESS && response.statusCode() != INTERNAL_SERVER_ERROR) {
                                    log.error("Invalid inventory request." + response.statusMessage());
                                    Mono.just("1").publishOn(ErasureServer.DISK_SCHEDULER)
                                            .subscribe(r -> deleteArchive());
                                }
                            };
                            return findAccountAkByBucket(sourceBucket)
                                    .publishOn(INVENTORY_SCHEDULER)
                                    .flatMap(ak -> InventoryTransmitter.readAndWriteObject(ak, "127.0.0.1", requestPort, storagePool[0], destinations[0].getBucket(), destinations[0].getObject(), destinations[0].getFileName(), transmitter.getContentLength(), headers, handler));
                        } else {
                            log.error("transport inventory stream to destination error!");
                            return Mono.just(false);
                        }
                    })
                    .publishOn(ErasureServer.DISK_SCHEDULER)
                    // 删除临时数据块
                    .flatMap(b -> {
                        String objectVnodeId = storagePool[0].getObjectVnodeId(destinations[0].getFileName());
                        return storagePool[0].mapToNodeInfo(objectVnodeId)
                                .flatMap(nodeList -> ErasureClient.deleteObjectFile(storagePool[0], destinations[0].getFileName(), nodeList).var1)
                                .doOnNext(del -> {
                                    if (!del) {
                                        log.error("the bucket {} inventory {} remove inventory part {} failed.", sourceBucket, inventoryId, partNumber.get() - 1);
                                    } else {
                                        log.debug("the bucket {} inventory {} remove inventory part {} successfully.", sourceBucket, inventoryId, partNumber.get() - 1);
                                    }
                                }).map(del -> b);
                    })
                    .publishOn(INVENTORY_SCHEDULER)
                    .doOnNext(b -> {
                        if (!b) {
                            log.error("the bucket {} write inventory {} part {} info error!", sourceBucket, inventoryId, partNumber.get() - 1);
                            throw new InventoryRuntimeException("Inventory task running error!");
                        }
                        // 记录生成的清单文件的基本信息
                        InventoryManifest.InventoryFile inventoryFile = new InventoryManifest.InventoryFile(destinations[0].getObject(), transmitter.getContentLength(), transmitter.getMd5());
                        manifest.getFiles().add(inventoryFile);

                        log.info("The bucket {} inventoryId {} produce file {} is successfully.", sourceBucket, inventoryId, inventoryFile);
                        QuotaRecorder.addCheckBucket(targetBucket);
                        if (dataSource.hasRemaining()) {
                            // 将当前任务存档
                            archive();
                            // 释放数据源后重启连接数据源
                            dataSource.scanner().start(new String(dataSource.cursor(null)));
                            // 重置传输的数据流
                            transmitter.reset();
                            partController.onNext(RandomStringUtils.randomAlphanumeric(32));
                        } else {
                            transmitter.reset();
                            if (!partController.hasCompleted()) {
                                partController.onComplete();
                            }
                            updateArchive(ExecuteState.INVENTORY_FILE_ENDED);
                        }

                        Optional.ofNullable(disposables[0]).ifPresent(Disposable::dispose);

                    }).subscribe(r -> {
                    }, e -> {
                        log.error("partController encounter exception. ", e);
                        dataSource.release();
                        executeResult.onNext(false);
                    });

            dataSource.fetch(0L);
        });

        partController.onNext(RandomStringUtils.randomAlphanumeric(32));
    }

    private static Mono<String> findAccountAkByBucket(String bucket) {
        return REDIS_CONN_POOL.getReactive(SysConstants.REDIS_BUCKETINFO_INDEX)
                .hget(bucket, "user_id")
                .flatMap(userId -> REDIS_CONN_POOL.getReactive(SysConstants.REDIS_USERINFO_INDEX).hget(userId, "access_key1")
                        .flatMap(ak1 -> {
                            if (StringUtils.isEmpty(ak1)) {
                                return REDIS_CONN_POOL.getReactive(SysConstants.REDIS_USERINFO_INDEX).hget(userId, "access_key2")
                                        .defaultIfEmpty("");
                            }
                            return Mono.just(ak1);
                        }));
    }

    /**
     * 终止当前任务的执行
     */
    public void shutdown() {
        try {
            // 终止传输任务
            this.transmitter.dispose();
            // 释放数据源
            this.dataSource.release();
            Optional.ofNullable(disposable).ifPresent(Disposable::dispose);
        } catch (Exception e) {
            log.error("Shutdown the bucket {} inventroy {} task error! {}", sourceBucket, inventoryId, e);
        }
    }

    private final String sourceBucket;
    private final String targetBucket;
    private String targetObject;
    private final AtomicInteger partNumber = new AtomicInteger(1);
    private boolean isRollback = false;

    /**
     * 构建目标对象文件信息
     *
     * @param partNumStr 任务的第几段对象
     * @return 目标
     */
    private Mono<Tuple2<Destination, StoragePool>> buildDestination(String partNumStr, String randomStr) {
        String targetObjectPrefix = targetObject.substring(0, targetObject.lastIndexOf("."));
        String suffix = targetObject.substring(targetObject.lastIndexOf("."));
        String objectName = targetObjectPrefix + partNumStr + suffix;
        final StoragePool pool = StoragePoolFactory.getStoragePool(new StorageOperate(DATA, objectName, MAX_PART_SIZE), targetBucket);
        String fileName = Utils.getObjFileName(pool, targetBucket, objectName, randomStr);
        return MsObjVersionUtils.getObjVersionIdReactive(targetBucket)
                .flatMap(versionId -> Mono.just(new Destination(targetBucket, objectName, versionId, String.valueOf(System.currentTimeMillis()), fileName)))
                .zipWith(Mono.just(pool));
    }

    /**
     * 清单文件生成完毕
     */
    public void completePart() {
        // 关闭清单数据流数据源
        dataSource.release();
        writeManifest()
                .publishOn(INVENTORY_SCHEDULER)
                .subscribe(b -> {
                    transmitter.dispose();
                    if (b) {
                        updateArchive(InventoryTask.ExecuteState.INVENTORY_TASK_ENDED);
                        QuotaRecorder.addCheckBucket(targetBucket);
                        executeResult.onNext(true);
                        log.info("write the bucket {} inventory {} manifest successfully!", sourceBucket, inventoryId);
                    } else {
                        log.error("write the the bucket {} inventory {} manifest failed.", sourceBucket, inventoryId);
                        executeResult.onNext(false);
                    }
                }, e -> {
                    log.error("write the the bucket {} inventory {} manifest failed. {}", sourceBucket, inventoryId, e);
                    executeResult.onNext(false);
                });
    }

    /**
     * 生成当前清单列表的说明文件
     */
    public Mono<Boolean> writeManifest() {
        // 将transmitter原来的数据源切换成manifest
        try {
            transmitter.replaceSource(manifest);
            String manifestName = manifest.getFileName();
            final StoragePool pool = StoragePoolFactory.getStoragePool(new StorageOperate(DATA, manifestName, MAX_PART_SIZE), targetBucket);
            String fileName = Utils.getObjFileName(pool, targetBucket, manifestName, RandomStringUtils.randomAlphabetic(32));
            Map<String, String> headers = new HashMap<>();
            headers.put("x-amz-acl", PERMISSION_BUCKET_OWNER_FULL_CONTROL);
            headers.put(NO_SYNCHRONIZATION_KEY, NO_SYNCHRONIZATION_VALUE);
            headers.put(CLUSTER_ALIVE_HEADER, "ip");
            return manifest.start()
                    .doOnNext(b -> manifest.fetch(1L))
                    .flatMap(b -> MsObjVersionUtils.getObjVersionIdReactive(targetBucket))
                    .publishOn(INVENTORY_SCHEDULER)
                    .doOnNext(versionId -> headers.put(VERSIONID, versionId))
                    .flatMap(versionId -> Mono.just(new Destination(targetBucket, manifestName, versionId, String.valueOf(System.currentTimeMillis()), fileName)))
                    .flatMap(destination -> transmitter.transport(destination, pool))
                    .flatMap(b -> {
                        if (b) {
                            return findAccountAkByBucket(sourceBucket)
                                    .publishOn(INVENTORY_SCHEDULER)
                                    .flatMap(ak -> InventoryTransmitter.readAndWriteObject(ak, "127.0.0.1", requestPort, pool, targetBucket, manifestName, fileName, transmitter.getContentLength(), headers, null));
                        } else {
                            log.error("transport manifest stream to destination error!");
                            return Mono.just(false);
                        }
                    })
                    .publishOn(ErasureServer.DISK_SCHEDULER)
                    .flatMap(b -> {
                        String objectVnodeId = pool.getObjectVnodeId(fileName);
                        return pool.mapToNodeInfo(objectVnodeId)
                                .flatMap(nodeList -> ErasureClient.deleteObjectFile(pool, fileName, nodeList).var1)
                                .doOnNext(del -> {
                                    if (!del) {
                                        log.error("the bucket {} inventory {} remove inventory manifest.json failed.", sourceBucket, inventoryId);
                                    } else {
                                        log.debug("the bucket {} inventory {} remove inventory manifest.json successfully.", sourceBucket, inventoryId);
                                    }
                                }).map(del -> b);
                    });
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(false);
        }
    }

    @Override
    public void run() {
        start();
    }


    public volatile InventoryTask.ExecuteState runningState = InventoryTask.ExecuteState.NOT_ENDED;

    /**
     * 记录清单任务的执行状态
     */
    enum ExecuteState {

        /**
         * 清单文件没有全部生成完毕
         */
        NOT_ENDED("0"),

        /**
         * 所有的清单文件全部生成完毕
         */
        INVENTORY_FILE_ENDED("1"),

        /**
         * 清单任务完成，并且成功生成了manifest.json文件
         */
        INVENTORY_TASK_ENDED("2");

        String value;

        ExecuteState(String value) {
            this.value = value;
        }

    }

    /**
     * 更新任务状态
     */
    public void updateArchive(InventoryTask.ExecuteState state) {
        runningState = state;
        archive();
    }

    /**
     * 将当前任务的执行轨迹存档
     */
    public void archive() {
        // 已经成功导出的部分文件
        List<InventoryManifest.InventoryFile> files = this.manifest.getFiles();
        JSONObject jsonObject = this.manifest.buildManifest();
        jsonObject.put("files", files);
        JSONObject archive = new JSONObject();
        archive.put("manifest", jsonObject);
        archive.put("cursor", dataSource.cursor(null) == null ? "" : new String(dataSource.cursor(null)));
        archive.put("uuid", InventoryService.LOCAL_VM_UUID);
        archive.put("disk", InventoryService.getLocalDiskByBucket(sourceBucket));
        archive.put("state", runningState.value);
        if("Incremental".equals(manifest.getInventoryType())){
            if (StringUtils.isNotEmpty(archive.getString("cursor"))) {
                REDIS_CONN_POOL.getShortMasterCommand(REDIS_TASKINFO_INDEX)
                        .hset(sourceBucket + "_incrementalStamp", inventoryId, archive.getString("cursor"));
            }
        }
        REDIS_CONN_POOL.getShortMasterCommand(REDIS_TASKINFO_INDEX)
                .hset(sourceBucket + "_archive", inventoryId, archive.toString());
    }

    /**
     * 删除存档
     */
    public void deleteArchive() {
        REDIS_CONN_POOL.getShortMasterCommand(REDIS_TASKINFO_INDEX).hdel(sourceBucket + "_archive", inventoryId);
    }

    /**
     * 根据存档信息,将任务回滚到上一次中断时的执行状态
     */
    public InventoryTask rollback() {
        try {
            String archive = REDIS_CONN_POOL.getCommand(REDIS_TASKINFO_INDEX)
                    .hget(sourceBucket + "_archive", inventoryId);
            if (StringUtils.isEmpty(archive)) {
                return this;
            }
            JSONObject jsonObject = JSONObject.parseObject(archive);
            String cursor = jsonObject.getString("cursor");
            JSONObject manifest = jsonObject.getJSONObject("manifest");
            if (manifest == null) {
                return this;
            }
            String fileName = manifest.getString("fileName");
            String sourceBucket = manifest.getString("sourceBucket");
            String destinationBucket = manifest.getString("destinationBucket");
            String version = manifest.getString("version");
            String creationTimestamp = manifest.getString("creationTimestamp");
            String fileFormat = manifest.getString("fileFormat");
            String inventoryType = manifest.getString("inventoryType");
            String[] fileSchema = manifest.getObject("fileSchema", String[].class);
            LinkedList<InventoryManifest.InventoryFile> files = manifest.getObject("files", new TypeReference<LinkedList<InventoryManifest.InventoryFile>>() {
            });
            String key = files.getLast().getKey();
            String partNum = key.substring(key.lastIndexOf("_") + 1, key.lastIndexOf("."));
            // 恢复数据源
            this.dataSource.cursor(cursor.getBytes());
            // 恢复targetObjectName
            this.targetObject = key.substring(0, key.lastIndexOf("_")) + key.substring(key.lastIndexOf("."));
            // 恢复manifest文件
            this.manifest = new InventoryManifest(fileName, sourceBucket, destinationBucket, version, creationTimestamp, fileFormat, fileSchema, inventoryType);
            this.manifest.getFiles().addAll(files);
            // 恢复partNum
            int partNumber = Integer.parseInt(partNum) + 1;
            this.partNumber.set(partNumber);
            this.isRollback = true;

            // 恢复任务之前的执行状态
            if (jsonObject.containsKey("state")) {
                String state = jsonObject.getString("state");
                if ("1".equals(state)) {
                    this.runningState = InventoryTask.ExecuteState.INVENTORY_FILE_ENDED;
                } else if ("2".equals(state)) {
                    this.runningState = InventoryTask.ExecuteState.INVENTORY_TASK_ENDED;
                } else {
                    this.runningState = InventoryTask.ExecuteState.NOT_ENDED;
                }
            }
        } catch (Exception e) {
            log.error("rollBack task error! ", e);
        }

        return this;
    }

    private static class InventoryRuntimeException extends RuntimeException {
        InventoryRuntimeException(String message) {
            super(message);
        }
    }
}