package com.macrosan.ec.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.doubleActive.deployment.RocksDBCheckPoint;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.rebuild.RebuildCheckpointUtil;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.inventory.datasource.scanner.InventoryListAllScanner;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.snapshot.service.*;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.transaction.UndoLog;
import com.macrosan.storage.client.ListSyncRecorderHandler;
import com.macrosan.storage.metaserver.ObjectSplitTree;
import com.macrosan.storage.metaserver.move.scanner.DefaultMetaDataScanner;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.macrosan.constants.ServerConstants.IS_SYNCING;
import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.database.rocksdb.MSRocksDB.UnsyncRecordDir;
import static com.macrosan.ec.Utils.*;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.storage.metaserver.ShardingWorker.SHARDING_SCHEDULER;

/**
 * @author gaozhiyuan1
 */
@Log4j2
public class ListServerHandler {
    public static final Scheduler REBUILD_SCAN_SCHEDULER;

    static {
        Scheduler scheduler = null;
        try {
            MsExecutor executor = new MsExecutor(PROC_NUM / 4, PROC_NUM / 4, new MsThreadFactory("rebuild-scan"));
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }

        REBUILD_SCAN_SCHEDULER = scheduler;
    }


    public static Mono<Payload> listMultiPartUpload(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String lun = msg.get("lun");
        int maxUploads = Integer.parseInt(msg.get("maxUploads"));
        String prefix = msg.get("prefix");
        String marker = msg.get("marker");
        String delimiter = msg.get("delimiter");
        String uploadIdMarker = msg.get("uploadIdMarker");
        String vnode = msg.get("vnode");
        String snapshotLink = msg.get("snapshotLink");
        String currentSnapshotMark = msg.get("currentSnapshotMark");
        Function<String, List<Tuple3<Boolean, String, InitPartInfo>>> listMultiPartUploadFunction = (snapshotMark) -> {
            String realPrefix = InitPartInfo.getPartKeyPrefix(vnode, bucket, prefix, snapshotMark);

            String lowerPrefix = realPrefix;
            String upperPrefix = InitPartInfo.getPartKeyPrefix(vnode, bucket + ROCKS_SMALLEST_KEY, "", snapshotMark);
            final StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
            final ObjectSplitTree tree = pool.getBucketShardCache().getCache().get(bucket);
            final Tuple2<String, String> upperBoundAndLowerBound = tree.queryNodeUpperBoundAndLowerBound(vnode);
            if (upperBoundAndLowerBound != null) {
                if (StringUtils.isNotBlank(upperBoundAndLowerBound.var1)) {
                    lowerPrefix = InitPartInfo.getPartKeyPrefix(vnode, bucket, upperBoundAndLowerBound.var1 + ONE_STR, snapshotMark);
                }
                if (StringUtils.isNotBlank(upperBoundAndLowerBound.var2)) {
                    upperPrefix = InitPartInfo.getPartKeyPrefix(vnode, bucket, upperBoundAndLowerBound.var2 + ONE_STR, snapshotMark);
                }
            }

            List<Tuple3<Boolean, String, InitPartInfo>> res;
            final Slice lowerSlice = new Slice(lowerPrefix);
            final Slice upperSlice = new Slice(upperPrefix);
            final ReadOptions options = new ReadOptions().setIterateLowerBound(lowerSlice).setIterateUpperBound(upperSlice);
            try (MSRocksIterator keyIterator = MSRocksDB.getRocksDB(lun).newIterator(options)) {
                int count = 0;
                String realMarker = null;
                if (StringUtils.isBlank(marker) && StringUtils.isBlank(prefix)) {
                    keyIterator.seek(InitPartInfo.getPartKeyPrefix(vnode, bucket, "", snapshotMark).getBytes());
                } else if (StringUtils.isNotBlank(uploadIdMarker) && StringUtils.isNotBlank(marker)) {
                    realMarker = InitPartInfo.getPartKey(vnode, bucket, marker, uploadIdMarker, snapshotMark);
                    keyIterator.seek(realMarker.getBytes());
                    if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(realPrefix)) {
                        InitPartInfo initPartInfo = Json.decodeValue(new String(keyIterator.value()), InitPartInfo.class);
                        if (initPartInfo.object.equals(marker)) {
                            keyIterator.next();
                        }
                    }
                } else if (prefix.compareTo(marker) > 0) {
                    realMarker = InitPartInfo.getPartKeyPrefix(vnode, bucket, prefix, snapshotMark);
                    keyIterator.seek(realMarker.getBytes());
                } else if (StringUtils.isNotBlank(delimiter) && marker.endsWith(delimiter)) {
                    byte[] bytes = marker.getBytes();
                    bytes[bytes.length - 1] += 1;
                    realMarker = InitPartInfo.getPartKeyPrefix(vnode, bucket, new String(bytes), snapshotMark);
                    keyIterator.seek(realMarker.getBytes());
                } else {
                    realMarker = StringUtils.isBlank(marker) ? InitPartInfo.getPartKeyPrefix(vnode, bucket, marker, snapshotMark) :
                            InitPartInfo.getPartKey(vnode, bucket, marker, new String(new byte[]{Byte.MAX_VALUE}), snapshotMark);
                    byte[] bytes = realMarker.getBytes();
                    keyIterator.seek(bytes);
                    if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(realPrefix)) {
                        InitPartInfo initPartInfo = Json.decodeValue(new String(keyIterator.value()), InitPartInfo.class);
                        if (initPartInfo.object.equals(marker)) {
                            keyIterator.next();
                        }
                    }
                }

                res = new LinkedList<>();

                while (count <= maxUploads && keyIterator.isValid()) {
                    String key = new String(keyIterator.key());
                    if (!key.startsWith(realPrefix)) {
                        break;
                    }

                    InitPartInfo initPartInfo = Json.decodeValue(new String(keyIterator.value()), InitPartInfo.class);
                    initPartInfo.setMetaData(null);
                    if (!initPartInfo.delete && initPartInfo.isViewable(currentSnapshotMark)) {
                        count++;
                        if (StringUtils.isNotBlank(delimiter)) {
                            int index = initPartInfo.object.substring(prefix.length()).indexOf(delimiter);
                            if (index >= 0) {
                                String partKey = initPartInfo.object.substring(0, index + prefix.length() + delimiter.length());
                                initPartInfo.setObject(partKey);
                                initPartInfo.setUploadId("");
                                byte[] keyBytes = InitPartInfo.getPartKeyPrefix(vnode, bucket, partKey, snapshotMark).getBytes();
                                keyBytes[keyBytes.length - 1] += 1;
                                keyIterator.seek(keyBytes);
                                res.add(new Tuple3<>(true, partKey, initPartInfo));
                            } else {
                                res.add(new Tuple3<>(false, initPartInfo.object, initPartInfo));
                                keyIterator.next();
                            }
                        } else {
                            res.add(new Tuple3<>(false, initPartInfo.object, initPartInfo));
                            keyIterator.next();
                        }
                    } else {
                        keyIterator.next();
                        //res.add(new Tuple3<>(false, initPartInfo.object, initPartInfo));
                    }
                }
            } finally {
                options.close();
                upperSlice.close();
                lowerSlice.close();
            }
            return res;
        };
        List<Tuple3<Boolean, String, InitPartInfo>> res = executeListOperation(listMultiPartUploadFunction, new SnapshotListMultiPartService(), currentSnapshotMark, snapshotLink, maxUploads);
        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
    }

    public static Mono<Payload> listPart(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String uploadId = msg.get("uploadId");
        String object = msg.get("object");
        int maxParts = Integer.parseInt(msg.get("maxParts"));
        String partNumberMarker = msg.get("partNumberMarker");
        String lun = msg.get("lun");
        String vnode = msg.get("vnode");
        String snapshotLink = msg.get("snapshotLink");
        String currentSnapshotMark = msg.get("currentSnapshotMark");

        Function<String, List<PartInfo>> listPartFunction = (snapshotMark) -> {
            List<PartInfo> partInfoList = new LinkedList<>();

            try (MSRocksIterator keyIterator = MSRocksDB.getRocksDB(lun).newIterator()) {
                String partKey = PartInfo.getPartKey(vnode, bucket, object, uploadId, "", snapshotMark);
                int startNum = Integer.parseInt(partNumberMarker) + 1;
                String startPartKey = PartInfo.getPartKey(vnode, bucket, object, uploadId, startNum + "", snapshotMark);
                keyIterator.seek(startPartKey.getBytes());
                int count = 0;

                while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(partKey) && count <= maxParts) {
                    PartInfo partInfo = Json.decodeValue(new String(keyIterator.value()), PartInfo.class);
                    if (partInfo.delete || partInfo.isUnView(currentSnapshotMark)) {
                        keyIterator.next();
                        continue;
                    }
                    partInfoList.add(partInfo);
                    count++;
                    keyIterator.next();
                }
            }
            return partInfoList;
        };
        List<PartInfo> partInfoList = executeListOperation(listPartFunction, new SnapshotListPartInfoService(), currentSnapshotMark, snapshotLink, maxParts);

        return Mono.just(DefaultPayload.create(Json.encode(partInfoList.toArray()), SUCCESS.name()));
    }

    public static Mono<Payload> listInventoryVersions(Payload payload) {
        payload.retain();
        return Mono.just(1L).flatMap(l -> listInventoryVersions0(payload))
                .doFinally(s -> payload.release());
    }

    public static Mono<Payload> listInventoryVersions0(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String disk = msg.get("lun");
        String bucket = msg.get("bucket");
        String prefix = msg.get("prefix");
        int maxKeys = Integer.parseInt(msg.get("maxKeys"));
        String marker = msg.get("marker");
        String vnode = msg.get("vnode");
        String snapshotLink = msg.get("snapshotLink");
        String currentSnapshotMark = msg.get("currentSnapshotMark");

        Function<String, List<Tuple3<Boolean, String, MetaData>>> listFunction = (snapshotMark) -> {
            String realPrefix = Utils.getMetaDataKey(vnode, bucket, prefix, snapshotMark);
            String lowerPrefix = Utils.getMetaDataKey(vnode, bucket, prefix, snapshotMark);
            String upperPrefix = Utils.getMetaDataKey(vnode, bucket + ROCKS_SMALLEST_KEY, prefix, snapshotMark);
            ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucket).getBucketShardCache().get(bucket);
            Tuple2<String, String> upperBoundAndLowerBound = objectSplitTree.queryNodeUpperBoundAndLowerBound(vnode);
            if (upperBoundAndLowerBound != null) {
                if (StringUtils.isNotBlank(upperBoundAndLowerBound.var1)) {
                    lowerPrefix = Utils.getMetaDataKey(vnode, bucket, upperBoundAndLowerBound.var1 + ONE_STR, snapshotMark);
                }
                if (StringUtils.isNotBlank(upperBoundAndLowerBound.var2)) {
                    upperPrefix = Utils.getMetaDataKey(vnode, bucket, upperBoundAndLowerBound.var2 + ONE_STR, snapshotMark);
                }
            }

            List<Tuple3<Boolean, String, MetaData>> result;
            try (Slice lowerSlice = new Slice(lowerPrefix.getBytes());
                 Slice upperSlice = new Slice(upperPrefix.getBytes());
                 ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice).setIterateUpperBound(upperSlice);
                 MSRocksIterator keyIterator = MSRocksDB.getRocksDB(disk).newIterator(readOptions)) {
                String realMarker;
                if (StringUtils.isBlank(marker) && StringUtils.isBlank(prefix)) {
                    keyIterator.seek((Utils.getMetaDataKey(vnode, bucket, "", snapshotMark)).getBytes());
                } else if (prefix.compareTo(marker) > 0) {
                    realMarker = Utils.getMetaDataKey(vnode, bucket, prefix, snapshotMark);
                    keyIterator.seek(realMarker.getBytes());
                } else {
                    realMarker = Utils.getMetaDataKey(vnode, bucket, marker, snapshotMark);
                    keyIterator.seek(realMarker.getBytes());
                    if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(realPrefix)) {
                        MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                        if (InventoryListAllScanner.getKeyStampVersionId(metaData).equals(marker)) {
                            keyIterator.next();
                        }
                    }
                }

                result = new LinkedList<>();
                int count = 0;
                while (count <= maxKeys && keyIterator.isValid()) {
                    String key = new String(keyIterator.key());
                    if (!key.startsWith(realPrefix)) {
                        break;
                    }

                    MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                    metaData.partInfos = null;
                    metaData.userMetaData = null;
                    metaData.objectAcl = null;
                    metaData.fileName = null;

                    result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                    keyIterator.next();

                    count++;
                }
            }
            return result;
        };
        List<Tuple3<Boolean, String, MetaData>> result = executeListOperation(listFunction, new SnapshotListInventoryVersionService(), currentSnapshotMark, snapshotLink, maxKeys);

        return Mono.just(DefaultPayload.create(Json.encode(result.toArray()), SUCCESS.name()));
    }

    public static Mono<Payload> listInventoryIncrementalVersions(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String disk = msg.get("lun");
        String bucket = msg.get("bucket");
        int maxKeys = Integer.parseInt(msg.get("maxKeys"));
        String beginPrefix = msg.get("beginStamp");
        String vnode = msg.get("vnode");
        if (StringUtils.isNotEmpty(beginPrefix) && !beginPrefix.startsWith("+" + vnode)) {
            beginPrefix = "+" + vnode + File.separator + beginPrefix;
        }
        String endStamp = msg.get("endStamp");
        boolean isCurrent = Boolean.valueOf(msg.get("isCurrent"));
        String snapshotLink = msg.get("snapshotLink");
        String currentSnapshotMark = msg.get("currentSnapshotMark");
        String finalBeginPrefix = beginPrefix;
        Function<String, List<Tuple3<Boolean, String, MetaData>>> listInventoryIncrementalVersionsFunction = (snapshotMark) -> {
            String endStampMarker = getLifeCycleStamp(vnode, bucket, endStamp, snapshotMark);
            String realMarker = getMetaDataKey(vnode, bucket, "", snapshotMark);

            List<Tuple3<Boolean, String, MetaData>> result = new LinkedList<>();
            MSRocksDB rocksDb = MSRocksDB.getRocksDB(disk);
            try (MSRocksIterator keyIterator = MSRocksDB.getRocksDB(disk).newIterator();
                 MSRocksIterator latestIter = MSRocksDB.getRocksDB(disk).newIterator()) {
                String newBeginPrefix = joinSnapshotMark(finalBeginPrefix, snapshotMark);
                keyIterator.seek((newBeginPrefix + ONE_STR).getBytes());
                int count = 0;
                while (count <= maxKeys && keyIterator.isValid() && new String(keyIterator.key()).compareTo(endStampMarker) < 0) {
                    String iteratorKey = new String(keyIterator.key());
                    MetaData lifeMeta = Utils.getLifeMeta(iteratorKey, StringUtils.isNotBlank(currentSnapshotMark));
                    String versionKey = Utils.transLifecycleKeyToVersionKey(iteratorKey, StringUtils.isNotBlank(currentSnapshotMark));
                    String lastKey = Utils.transLifecycleKeyToLastKey(iteratorKey, StringUtils.isNotBlank(currentSnapshotMark));

                    byte[] value = keyIterator.value();
                    byte[] lastValue = rocksDb.get(lastKey.getBytes());
                    if (!Utils.isMetaJson(value)) {
                        value = rocksDb.get(versionKey.getBytes());
                        if (value == null) {
                            keyIterator.next();
                            result.add(new Tuple3<>(false, lifeMeta.getKey(), lifeMeta));
                            count++;
                            continue;
                        }
                    }
                    MetaData metaData = Json.decodeValue(new String(value), MetaData.class);
                    lifeMeta.setUnView(metaData.getUnView());
                    if (!bucket.equals(metaData.bucket)) {
                        break;
                    }
                    keyIterator.next();
                    if (metaData.deleteMark || !lifeMeta.bucket.equals(metaData.bucket) || !lifeMeta.key.equals(metaData.key)
                            || !lifeMeta.versionId.equals(metaData.versionId) || !lifeMeta.stamp.equals(metaData.stamp) || metaData.isUnView(currentSnapshotMark)) {
                        result.add(new Tuple3<>(false, lifeMeta.getKey(), lifeMeta));
                        count++;
                        continue;
                    }

                    if (isCurrent) {
                        if (lastValue == null) {
                            result.add(new Tuple3<>(false, lifeMeta.getKey(), lifeMeta));
                            count++;
                            continue;
                        } else {
                            MetaData lastMeta = Json.decodeValue(new String(lastValue), MetaData.class);
                            if (!lifeMeta.stamp.equals(lastMeta.stamp)) {
                                result.add(new Tuple3<>(false, lifeMeta.getKey(), lifeMeta));
                                count++;
                                continue;
                            }
                        }
                    }


                    String objectName = metaData.key;
                    metaData.partInfos = null;
                    metaData.userMetaData = null;
                    metaData.objectAcl = null;
                    metaData.fileName = null;

                    if (!metaData.deleteMarker) {
                        String latestKey = getLatestMetaKey(vnode, bucket, objectName, snapshotMark);
                        byte[] latestValue = rocksDb.get(latestKey.getBytes());
                        if (latestValue != null) {
                            MetaData latestMeta = Json.decodeValue(new String(latestValue), MetaData.class);
                            if (bucket.equals(latestMeta.bucket) && objectName.equals(latestMeta.key) && metaData.versionId.equals(latestMeta.versionId) && latestMeta.latestMetaIsViewable(currentSnapshotMark)) {
                                metaData.setLatest(true);
                                result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                count++;
                            } else {
                                metaData.setLatest(false);
                                if (isCurrent) {
                                    result.add(new Tuple3<>(false, lifeMeta.getKey(), lifeMeta.setLatest(false)));
                                } else {
                                    result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                }

                                count++;
                            }
                        } else {
                            metaData.setLatest(false);
                            if (isCurrent) {
                                result.add(new Tuple3<>(false, lifeMeta.getKey(), lifeMeta.setLatest(false)));
                            } else {
                                result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                            }
                            count++;
                        }

                    } else {
                        String stampKey = getMetaDataKey(vnode, bucket, objectName, metaData.versionId, metaData.stamp, metaData.snapshotMark);
                        latestIter.seek(stampKey.getBytes());
                        if (!latestIter.isValid()) {
                            MetaData invalidMeta = new MetaData()
                                    .setBucket(bucket).setKey(objectName).setVersionId(metaData.versionId)
                                    .setStamp(metaData.stamp).setVersionNum("0").setDeleteMark(true);
                            result.add(new Tuple3<>(false, invalidMeta.getKey(), invalidMeta));
                            count++;
                            continue;
                        }
                        latestIter.next();
                        if (latestIter.isValid() && new String(latestIter.key()).startsWith(realMarker)) {
                            MetaData nextMeta = Json.decodeValue(new String(latestIter.value()), MetaData.class);
                            if (!objectName.equals(nextMeta.key)) {
                                latestIter.prev();
                                if (latestIter.isValid()) {
                                    latestIter.prev();
                                    if (latestIter.isValid() && new String(latestIter.key()).startsWith(stampKey)) {
                                        nextMeta = Json.decodeValue(new String(latestIter.value()), MetaData.class);
                                        if (bucket.equals(nextMeta.bucket) && objectName.equals(nextMeta.key) && Objects.equals(metaData.snapshotMark, nextMeta.snapshotMark)) {
//                                        metaData.setDeleteMark(true);
                                            result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                            count++;
                                            continue;
                                        }
                                    }
                                }
                                if (!metaData.isCurrentSnapshotObject(currentSnapshotMark)) {
                                    // 该对象不是当前快照下的，则还需判断当前快照下是否存在相同key的对象
                                    String curStampKey = getMetaDataKey(vnode, bucket, metaData.key, currentSnapshotMark);
                                    latestIter.seek(curStampKey.getBytes());
                                    boolean curPutFlag = false;
                                    while (latestIter.isValid() && new String(latestIter.key()).startsWith(curStampKey)) {
                                        MetaData curMeta = Json.decodeValue(new String(latestIter.value()), MetaData.class);
                                        if (!curMeta.isDeleteMark() && curMeta.key.equals(metaData.key) && curMeta.bucket.equals(metaData.bucket)) {
                                            // 当前快照下上传过同名对象，则不标记为latest
                                            curPutFlag = true;
                                            break;
                                        }
                                        latestIter.next();
                                    }
                                    metaData.setLatest(!curPutFlag);
                                } else {
                                    metaData.setLatest(true);
                                }
                                result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                count++;
                            } else {
                                metaData.setLatest(false);
                                if (isCurrent) {
                                    result.add(new Tuple3<>(false, lifeMeta.getKey(), lifeMeta.setLatest(false)));
                                } else {
                                    result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                }
//                            result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                count++;
                            }
                        } else {
                            latestIter.seek(stampKey.getBytes());
                            if (latestIter.isValid()) {
                                latestIter.prev();
                                if (latestIter.isValid() && new String(latestIter.key()).startsWith(stampKey)) {
                                    MetaData nextMeta = Json.decodeValue(new String(latestIter.value()), MetaData.class);
                                    if (bucket.equals(nextMeta.bucket) && objectName.equals(nextMeta.key) && Objects.equals(metaData.snapshotMark, nextMeta.snapshotMark)) {
//                                    metaData.setDeleteMark(true);
                                        result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                        count++;
                                        continue;
                                    }
                                }
                            }
                            if (!metaData.isCurrentSnapshotObject(currentSnapshotMark)) {
                                // 该对象不是当前快照下的，则还需判断当前快照下是否存在相同key的对象
                                String curStampKey = getMetaDataKey(vnode, bucket, metaData.key, currentSnapshotMark);
                                latestIter.seek(curStampKey.getBytes());
                                boolean curPutFlag = false;
                                while (latestIter.isValid() && new String(latestIter.key()).startsWith(curStampKey)) {
                                    MetaData curMeta = Json.decodeValue(new String(latestIter.value()), MetaData.class);
                                    if (!curMeta.isDeleteMark() && curMeta.key.equals(metaData.key) && curMeta.bucket.equals(metaData.bucket)) {
                                        // 当前快照下上传过同名对象，则不标记为latest
                                        curPutFlag = true;
                                        break;
                                    }
                                    latestIter.next();
                                }
                                metaData.setLatest(!curPutFlag);
                            } else {
                                metaData.setLatest(true);
                            }
                            result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                            count++;
                        }
                    }
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            return result;
        };

        try {
            List<Tuple3<Boolean, String, MetaData>> result = executeListOperation(listInventoryIncrementalVersionsFunction, new SnapshotListLifeService(), currentSnapshotMark, snapshotLink, maxKeys);
            return Mono.just(DefaultPayload.create(Json.encode(result.toArray()), SUCCESS.name()));
        } catch (Exception e) {
            log.error("List inventory increment objects fail ! ", e);
            return Mono.just(ERROR_PAYLOAD);
        }

    }

    public static Mono<Payload> listInventoryCurrent(Payload payload) {
        payload.retain();
        return Mono.just(1L).flatMap(l -> listObjects(payload))
                .doFinally(l -> payload.release());
    }

    public static Mono<Payload> listObjects(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);

        String disk = msg.get("lun");
        String bucket = msg.get("bucket");
        int maxKeys = Integer.parseInt(msg.get("maxKeys"));
        String prefix = msg.get("prefix");
        String marker = msg.get("marker");
        String delimiter = msg.get("delimiter");
        String vnode = msg.get("vnode");
        String continuationToken = msg.get("continuationToken");
        String snapshotLink = msg.get("snapshotLink");
        String currentSnapshotMark = msg.get("currentSnapshotMark");
        if (null == continuationToken) {
            continuationToken = "";
        }
        String finalContinuationToken = continuationToken;

        Function<String, List<Tuple3<Boolean, String, MetaData>>> listObjectsFunction = (snapshotMark) -> {
            ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucket).getBucketShardCache().get(bucket);
            Tuple2<String, String> upperBoundAndLowerBound = objectSplitTree.queryNodeUpperBoundAndLowerBound(vnode);
            String realPrefix = Utils.getLatestMetaKey(vnode, bucket, prefix, snapshotMark);
            String lowerPrefix = Utils.getLatestMetaKey(vnode, bucket, prefix, snapshotMark);
            String upperPrefix = Utils.getLatestMetaKey(vnode, bucket + ROCKS_SMALLEST_KEY, prefix, snapshotMark);
            if (upperBoundAndLowerBound != null) {
                if (StringUtils.isNotEmpty(upperBoundAndLowerBound.var1)) {
                    lowerPrefix = Utils.getLatestMetaKey(vnode, bucket, upperBoundAndLowerBound.var1 + ZERO_STR, snapshotMark);
                }
                if (StringUtils.isNotEmpty(upperBoundAndLowerBound.var2)) {
                    upperPrefix = Utils.getLatestMetaKey(vnode, bucket, upperBoundAndLowerBound.var2 + ZERO_STR, snapshotMark);
                }
            }
            List<Tuple3<Boolean, String, MetaData>> result;
            try (Slice lowerSlice = new Slice(lowerPrefix.getBytes());
                 Slice upperSlice = new Slice(upperPrefix.getBytes());
                 ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice).setIterateUpperBound(upperSlice);
                 MSRocksIterator keyIterator = MSRocksDB.getRocksDB(disk).newIterator(readOptions)) {
                String realMarker = "";
                if (StringUtils.isBlank(marker) && StringUtils.isBlank(prefix) && StringUtils.isBlank(finalContinuationToken)) {
                    keyIterator.seek(Utils.getLatestMetaKey(vnode, bucket, "", snapshotMark).getBytes());
                } else if (prefix.compareTo(marker) > 0 && prefix.compareTo(finalContinuationToken) > 0) {
                    realMarker = Utils.getLatestMetaKey(vnode, bucket, prefix, snapshotMark);
                    keyIterator.seek(realMarker.getBytes());
                } else if (finalContinuationToken.compareTo(marker) > 0) {
                    if (StringUtils.isNotBlank(delimiter) && finalContinuationToken.endsWith(delimiter)) {
                        byte[] bytes = finalContinuationToken.getBytes();
                        bytes[bytes.length - 1] += 1;
                        realMarker = Utils.getLatestMetaKey(vnode, bucket, new String(bytes), snapshotMark);
                    } else {
                        realMarker = Utils.getLatestMetaKey(vnode, bucket, finalContinuationToken, snapshotMark);
                    }
                    keyIterator.seek(realMarker.getBytes());
                    if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(realPrefix)) {
                        MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                        if (metaData.getKey().equals(finalContinuationToken)) {
                            keyIterator.next();
                        }
                    }
                } else {
                    realMarker = Utils.getLatestMetaKey(vnode, bucket, marker, snapshotMark);
                    keyIterator.seek(realMarker.getBytes());
                    if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(realPrefix)) {
                        MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                        if (metaData.getKey().equals(marker)) {
                            keyIterator.next();
                        }
                    }
                }

                result = new LinkedList<>();
                int count = 0;
                while (count <= maxKeys && keyIterator.isValid()) {
                    String key = new String(keyIterator.key());
                    if (!key.startsWith(realPrefix)) {
                        break;
                    }
                    MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                    metaData.partInfos = null;
                    metaData.userMetaData = null;
                    metaData.objectAcl = null;
                    metaData.fileName = null;
                    // 判断对象是否当前快照不可见
                    if (!metaData.latestMetaIsViewable(currentSnapshotMark)) {
                        keyIterator.next();
                        continue;
                    }
                    if (metaData.inode > 0) {
                        try {
                            String inodeKey = Inode.getKey(vnode, bucket, metaData.inode);
                            MSRocksDB db = MSRocksDB.getRocksDB(disk);
                            if (db != null) {
                                byte[] value = db.get(inodeKey.getBytes());
                                if (value != null) {
                                    Inode inode = Json.decodeValue(new String(value), Inode.class);
                                    inode.setUpdateChunk(null);
                                    inode.setChunkFileMap(null);
                                    metaData.setEndIndex(inode.getSize() - 1);
                                    Inode.mergeMeta(metaData, inode);
                                    metaData.partInfos = null;
                                    metaData.tmpInodeStr = null;
                                } else {
                                    metaData.setEndIndex(-1);
                                    metaData.setVersionNum("0");
                                }
                            }
                        } catch (RocksDBException e) {
                            log.error("get inodeValue error", e);
                        }
                    }
                    if (StringUtils.isNotBlank(delimiter)) {
                        int index = metaData.getKey().substring(prefix.length()).indexOf(delimiter);
                        if (index >= 0) {
                            String commonPrefix = metaData.getKey().substring(0, index + prefix.length() + delimiter.length());
                            byte[] metaDataKey = Utils.getLatestMetaKey(vnode, metaData.getBucket(), commonPrefix, snapshotMark).getBytes();
                            metaDataKey[metaDataKey.length - 1] += 1;
                            keyIterator.seek(metaDataKey);
                            metaData.setKey(commonPrefix);
                            result.add(new Tuple3<>(true, commonPrefix, metaData));
                        } else {
                            result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                            keyIterator.next();
                        }
                    } else {
                        result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                        keyIterator.next();
                    }
                    count++;
                }
            }
            return result;
        };

        List<Tuple3<Boolean, String, MetaData>> result = executeListOperation(listObjectsFunction, new SnapshotListObjectService(), currentSnapshotMark, snapshotLink, maxKeys);

        return Mono.just(DefaultPayload.create(Json.encode(result.toArray()), SUCCESS.name()));
    }

    public static Mono<Payload> listLifecycleObjects(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String disk = msg.get("lun");
        String bucket = msg.get("bucket");
        int maxKeys = Integer.parseInt(msg.get("maxKeys"));
        String beginPrefix = msg.get("beginPrefix");
        String endStamp = msg.get("stamp");
        String vnode = msg.get("vnode");
        String snapshotLink = msg.get("snapshotLink");
        String currentSnapshotMark = msg.get("currentSnapshotMark");
        AtomicBoolean showAll = new AtomicBoolean(false);
        Optional.ofNullable(msg.get("all")).ifPresent(s -> showAll.set(true));
        AtomicBoolean showUsermeta = new AtomicBoolean(false);
        Optional.ofNullable(msg.get("showUsermeta")).ifPresent(s -> showUsermeta.set(true));
        Function<String, List<Tuple3<Boolean, String, MetaData>>> listLifecycleObjectsFunction = (snapshotMark) -> {
            String endStampMarker = getLifeCycleStamp(vnode, bucket, endStamp, snapshotMark);
            String realMarker = getMetaDataKey(vnode, bucket, "", snapshotMark);
            List<Tuple3<Boolean, String, MetaData>> result = new LinkedList<>();
            MSRocksDB rocksDb = MSRocksDB.getRocksDB(disk);
            try (MSRocksIterator keyIterator = MSRocksDB.getRocksDB(disk).newIterator();
                 MSRocksIterator latestIter = MSRocksDB.getRocksDB(disk).newIterator()) {
                String newBeginPrefix = joinSnapshotMark(beginPrefix, snapshotMark);// prefix中添加快照标记
                keyIterator.seek(newBeginPrefix.getBytes());
                int count = 0;
                while (count <= maxKeys && keyIterator.isValid() && new String(keyIterator.key()).compareTo(endStampMarker) < 0) {
                    String iteratorKey = new String(keyIterator.key());
                    MetaData lifeMeta = Utils.getLifeMeta(iteratorKey, StringUtils.isNotBlank(currentSnapshotMark));
                    String versionKey = Utils.transLifecycleKeyToVersionKey(iteratorKey, StringUtils.isNotBlank(currentSnapshotMark));

                    byte[] value = keyIterator.value();
                    if (!Utils.isMetaJson(value)) {
                        value = rocksDb.get(versionKey.getBytes());
                        if (value == null) {
                            keyIterator.next();
                            result.add(new Tuple3<>(false, lifeMeta.getKey(), lifeMeta.clone().setDiscard(true)));
                            count++;
                            continue;
                        }
                    }
                    MetaData metaData = Json.decodeValue(new String(value), MetaData.class);
                    lifeMeta.setUnView(metaData.getUnView());
                    if (!bucket.equals(metaData.bucket)) {
                        break;
                    }
                    keyIterator.next();
                    if (metaData.deleteMark || !lifeMeta.bucket.equals(metaData.bucket) || !lifeMeta.key.equals(metaData.key)
                            || !lifeMeta.versionId.equals(metaData.versionId) || (!lifeMeta.stamp.equals(metaData.stamp) && metaData.getInode() <= 0) || metaData.isUnView(currentSnapshotMark)) {
                        result.add(new Tuple3<>(false, lifeMeta.getKey(), lifeMeta));
                        count++;
                        continue;
                    }

                    String objectName = metaData.key;
                    if (!showAll.get()) {
                        metaData.partInfos = null;
                        if (!showUsermeta.get()){
                            metaData.userMetaData = null;
                        }
                        metaData.objectAcl = null;
                        metaData.fileName = null;
                    }

                    metaData.stamp = lifeMeta.stamp;

                    if (!metaData.deleteMarker) {
                        String latestKey = getLatestMetaKey(vnode, bucket, objectName, snapshotMark);
                        byte[] latestValue = rocksDb.get(latestKey.getBytes());
                        if (latestValue != null) {
                            MetaData latestMeta = Json.decodeValue(new String(latestValue), MetaData.class);
                            if (bucket.equals(latestMeta.bucket) && objectName.equals(latestMeta.key) && metaData.versionId.equals(latestMeta.versionId) && latestMeta.latestMetaIsViewable(currentSnapshotMark)) {
                                metaData.setLatest(true);
                                result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                count++;
                            } else {
                                metaData.setLatest(false);
                                result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                count++;
                            }
                        } else {
                            metaData.setLatest(false);
                            result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                            count++;
                        }

                    } else {
                        String stampKey = getMetaDataKey(vnode, bucket, objectName, metaData.versionId, metaData.stamp, metaData.snapshotMark);
                        latestIter.seek(stampKey.getBytes());
                        if (!latestIter.isValid()) {
                            MetaData invalidMeta = new MetaData()
                                    .setBucket(bucket).setKey(objectName).setVersionId(metaData.versionId)
                                    .setStamp(metaData.stamp).setVersionNum("0").setDeleteMark(true);
                            result.add(new Tuple3<>(false, invalidMeta.getKey(), invalidMeta));
                            count++;
                            continue;
                        }
                        latestIter.next();
                        if (latestIter.isValid() && new String(latestIter.key()).startsWith(realMarker)) {
                            MetaData nextMeta = Json.decodeValue(new String(latestIter.value()), MetaData.class);
                            if (!objectName.equals(nextMeta.key)) {
                                latestIter.prev();
                                if (latestIter.isValid()) {
                                    latestIter.prev();
                                    if (latestIter.isValid() && new String(latestIter.key()).startsWith(stampKey)) {
                                        nextMeta = Json.decodeValue(new String(latestIter.value()), MetaData.class);
                                        if (bucket.equals(nextMeta.bucket) && objectName.equals(nextMeta.key) && Objects.equals(metaData.snapshotMark, nextMeta.snapshotMark)) {
                                            metaData.setDeleteMark(true);
                                            result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                            count++;
                                            continue;
                                        }
                                    }
                                }
                                if (!metaData.isCurrentSnapshotObject(currentSnapshotMark)) {
                                    // 该对象不是当前快照下的，则还需判断当前快照下是否存在相同key的对象
                                    String curStampKey = getMetaDataKey(vnode, bucket, metaData.key, currentSnapshotMark);
                                    latestIter.seek(curStampKey.getBytes());
                                    boolean curPutFlag = false;
                                    while (latestIter.isValid() && new String(latestIter.key()).startsWith(curStampKey)) {
                                        MetaData curMeta = Json.decodeValue(new String(latestIter.value()), MetaData.class);
                                        if (!curMeta.isDeleteMark() && curMeta.key.equals(metaData.key) && curMeta.bucket.equals(metaData.bucket)) {
                                            // 当前快照下上传过同名对象，则不标记为latest
                                            curPutFlag = true;
                                            break;
                                        }
                                        latestIter.next();
                                    }
                                    metaData.setLatest(!curPutFlag);
                                } else {
                                    metaData.setLatest(true);
                                }
                                result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                count++;
                            } else {
                                metaData.setLatest(false);
                                result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                count++;
                            }
                        } else {
                            latestIter.seek(stampKey.getBytes());
                            if (latestIter.isValid()) {
                                latestIter.prev();
                                if (latestIter.isValid() && new String(latestIter.key()).startsWith(stampKey)) {
                                    MetaData nextMeta = Json.decodeValue(new String(latestIter.value()), MetaData.class);
                                    if (bucket.equals(nextMeta.bucket) && objectName.equals(nextMeta.key) && Objects.equals(metaData.snapshotMark, nextMeta.snapshotMark)) {
                                        metaData.setDeleteMark(true);
                                        result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                                        count++;
                                        continue;
                                    }
                                }
                            }
                            if (!metaData.isCurrentSnapshotObject(currentSnapshotMark)) {
                                // 该对象不是当前快照下的，则还需判断当前快照下是否存在相同key的对象
                                String curStampKey = getMetaDataKey(vnode, bucket, metaData.key, currentSnapshotMark);
                                latestIter.seek(curStampKey.getBytes());
                                boolean curPutFlag = false;
                                while (latestIter.isValid() && new String(latestIter.key()).startsWith(curStampKey)) {
                                    MetaData curMeta = Json.decodeValue(new String(latestIter.value()), MetaData.class);
                                    if (!curMeta.isDeleteMark() && curMeta.key.equals(metaData.key) && curMeta.bucket.equals(metaData.bucket)) {
                                        // 当前快照下上传过同名对象，则不标记为latest
                                        curPutFlag = true;
                                        break;
                                    }
                                    latestIter.next();
                                }
                                metaData.setLatest(!curPutFlag);
                            } else {
                                metaData.setLatest(true);
                            }
                            result.add(new Tuple3<>(false, metaData.getKey(), metaData));
                            count++;
                        }
                    }
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            return result;
        };

        try {
            List<Tuple3<Boolean, String, MetaData>> result = executeListOperation(listLifecycleObjectsFunction, new SnapshotListLifeService(), currentSnapshotMark, snapshotLink, maxKeys);
            return Mono.just(DefaultPayload.create(Json.encode(result.toArray()), SUCCESS.name()));
        } catch (Exception e) {
            log.error("List lifecycle metadata fail ! ", e);
            return Mono.just(ERROR_PAYLOAD);
        }
    }

    public static Mono<Payload> listVnodeObj(Payload payload) {
        payload.retain();
        return Mono.just(1L).publishOn(REBUILD_SCAN_SCHEDULER).flatMap(l -> listVnodeObj0(payload))
                .doFinally(s -> payload.release());
    }

    private static Mono<Payload> listVnodeObj0(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String marker = msg.get("marker");
        List<FileMeta> res = new ArrayList<>();
        String[] link = Json.decodeValue(msg.get("link"), String[].class);

        int count = 0;
        int curIndex = -1;
        String prefix;

        if (StringUtils.isBlank(marker)) {
            curIndex = 0;
            marker = SysConstants.ROCKS_FILE_META_PREFIX + link[curIndex] + "_";
        } else {
            String lastVnode = marker.split("_")[0].replace("#", "");
            for (int i = 0; i < link.length; i++) {
                if (link[i].equalsIgnoreCase(lastVnode)) {
                    curIndex = i;
                    break;
                }
            }
        }

        if (curIndex == -1) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "");
        }

        long resSize = 0L;

        if (MSRocksDB.getRocksDB(lun) == null) {
            return Mono.just(ERROR_PAYLOAD);
        }

        try (MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator()) {
            for (int i = curIndex; i < link.length; i++) {
                iterator.seek(marker.getBytes());
                if (iterator.isValid() && new String(iterator.key()).equalsIgnoreCase(marker)) {
                    iterator.next();
                }

                prefix = SysConstants.ROCKS_FILE_META_PREFIX + link[i] + "_";

                while (iterator.isValid() && new String(iterator.key()).startsWith(prefix)) {
                    count++;
                    res.add(Json.decodeValue(new String(iterator.value()), FileMeta.class));
                    resSize += iterator.value().length;
                    if (count == 1000 || resSize > 1024 * 1024) {
                        return Mono.just(DefaultPayload.create(Json.encode(res.toArray(new FileMeta[0])), SUCCESS.name()));
                    }
                    iterator.next();
                }

                if (i != link.length - 1) {
                    marker = SysConstants.ROCKS_FILE_META_PREFIX + link[i + 1] + "_";
                }
            }
        }

        return Mono.just(DefaultPayload.create(Json.encode(res.toArray(new FileMeta[0])), SUCCESS.name()));
    }

    /**
     * @param key rocksdb 的key
     * @return var1 是vnode之前的字符串 var2是vnode
     */
    public static Tuple2<String, String> getVnode(String key) {
        int prefixIndex = 0;
        int vnodePrefix = 0;
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c >= '0' && c <= '9') {
                prefixIndex = i;
                break;
            }
        }

        for (int i = prefixIndex + 1; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c < '0' || c > '9') {
                vnodePrefix = i;
                break;
            }
        }

        return new Tuple2<>(key.substring(0, prefixIndex), key.substring(prefixIndex, vnodePrefix));
    }

    public static char getVnodeSuffix(String key) {
        int prefixIndex = 0;
        int vnodePrefix = 0;
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c >= '0' && c <= '9') {
                prefixIndex = i;
                break;
            }
        }

        for (int i = prefixIndex + 1; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c < '0' || c > '9') {
                vnodePrefix = i;
                break;
            }
        }
        return key.charAt(vnodePrefix);
    }

    public static Mono<Payload> nextPrefix(Payload payload) {
        payload.retain();
        return Mono.just(1L).publishOn(REBUILD_SCAN_SCHEDULER).flatMap(l -> nextPrefix0(payload))
                .doFinally(s -> payload.release());
    }

    private static Mono<Payload> nextPrefix0(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String prefix = msg.get("prefix");
        boolean removeDisk = Boolean.parseBoolean(msg.get("removeDisk"));
        if (removeDisk && lun.contains("index")) {
            if (BatchRocksDB.errorLun.contains(lun)) {
                return Mono.just(DefaultPayload.create("", ERROR.name()));
            }
            lun = RebuildCheckpointUtil.getCheckPointLun(lun);
            try {
                if (RebuildCheckpointUtil.isDirectoryExists("/" + lun)) {
                    // checkpoint目录不存在
                    return Mono.just(DefaultPayload.create("", REBUILD_CREATE_CHECK_POINT.name()));
                }
            } catch (Exception e) {
                return Mono.just(DefaultPayload.create("", ERROR.name()));
            }
        }

        MSRocksDB db = MSRocksDB.getRocksDB(lun);
        if (db == null) {
            return Mono.just(DefaultPayload.create("", ERROR.name()));
        }
        try (MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator()) {
            if (null == prefix) {
                iterator.seek(new byte[0]);
            } else if (StringUtils.isBlank(prefix)) {
                byte[] bytes = "9".getBytes();
                bytes[bytes.length - 1] += 1;
                iterator.seek(bytes);
            } else {
                byte[] bytes = prefix.getBytes();
                bytes[bytes.length - 1] += 1;
                iterator.seek(bytes);
            }

            if (iterator.isValid()) {
                Tuple2<String, String> tuple2 = getVnode(new String(iterator.key()));
                return Mono.just(DefaultPayload.create(tuple2.var1, SUCCESS.name()));
            } else {
                return Mono.just(DefaultPayload.create("", ERROR.name()));
            }
        }
    }

    public static Mono<Payload> listShardingMetaObj(Payload payload) {
        payload.retain();
        return Mono.just(1L).publishOn(SHARDING_SCHEDULER)
                .flatMap(l -> ListShardedMetaData(payload))
                .doFinally(s -> payload.release());
    }

    public static Mono<Payload> listMetaObj(Payload payload) {
        payload.retain();
        return Mono.just(1L).publishOn(REBUILD_SCAN_SCHEDULER).flatMap(l -> listMetaObj0(payload))
                .doFinally(s -> payload.release());
    }

    private static Mono<Payload> listMetaObj0(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String marker = msg.get("marker");
        String prefix = msg.get("prefix");
        String bucket = msg.get("bucket");
        boolean remove = Boolean.parseBoolean(msg.get("remove"));
        List<Tuple2<String, String>> res = new LinkedList<>();
        // 是否根据marker向前遍历
        boolean prev = "1".equals(msg.get("prev"));
        String[] link = Json.decodeValue(msg.get("link"), String[].class);
        Set<String> vnodeSet = new HashSet<>();
        Collections.addAll(vnodeSet, link);

        int count = 0;
        int curIndex = -1;

        if (StringUtils.isBlank(marker)) {
            curIndex = 0;
            marker = StringUtils.isBlank(bucket) ? prefix + link[0] + getSeparator(prefix) : prefix + link[0] + File.separatorChar + bucket;
        } else {
            Tuple2<String, String> last = getVnode(marker);
            String lastVnode = last.var2;
            for (int i = 0; i < link.length; i++) {
                if (link[i].equalsIgnoreCase(lastVnode)) {
                    curIndex = i;
                    break;
                }
            }
        }

        if (curIndex == -1) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "");
        }

        long resSize = 0L;

        if (remove && lun.contains("index")) {
            lun = RebuildCheckpointUtil.getCheckPointLun(lun);
            if (RebuildCheckpointUtil.isDirectoryExists("/" + lun)) {
                // checkpoint目录不存在
                return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), REBUILD_CREATE_CHECK_POINT.name()));
            }
        }
        MSRocksDB rocksDB = MSRocksDB.getRocksDB(lun);
        if (rocksDB == null) {
            return Mono.just(ERROR_PAYLOAD);
        }

        try (MSRocksIterator iterator = rocksDB.newIterator()) {
            for (int i = curIndex; i < link.length; i++) {
                iterator.seek(marker.getBytes());
                if ((iterator.isValid() && new String(iterator.key()).equalsIgnoreCase(marker)) || prev) {
                    if (prev) {
                        iterator.prev();
                    } else {
                        iterator.next();
                    }
                }

                while (iterator.isValid()) {
                    Tuple2<String, String> tuple2 = getVnode(new String(iterator.key()));
                    if (tuple2.var1.equalsIgnoreCase(prefix)) {
                        if (tuple2.var2.equalsIgnoreCase(link[i])
                                && (StringUtils.isEmpty(bucket) || new String(iterator.key()).startsWith(prefix + link[i] + File.separator + bucket))) {
                            byte[] key = iterator.key();
                            byte[] value = iterator.value();
                            if (new String(key).startsWith("+") && !Utils.isMetaJson(value)) {
                                JsonObject jsonObject = new JsonObject();
                                jsonObject.put("versionNum", "0");
                                value = jsonObject.toString().getBytes();
                            } else if (new String(key).startsWith(ROCKS_AGGREGATION_RATE_PREFIX)) {
                                JsonObject jsonObject = new JsonObject();
                                jsonObject.put("versionNum", "0");
                                jsonObject.put("value", Base64.getEncoder().encodeToString(value));
                                value = jsonObject.toString().getBytes();
                            }
                            count++;
                            resSize += iterator.key().length + iterator.value().length;
                            if (prefix.equalsIgnoreCase(ROCKS_BUCKET_META_PREFIX)) {
                                MetaData metaData = new MetaData()
                                        .setBucket("bucket")
                                        .setKey("key")
                                        .setDeleteMark(false)
                                        .setVersionNum(VersionUtil.getVersionNum(false));
                                res.add(new Tuple2<>(new String(iterator.key()), Json.encode(metaData)));
                            } else {
                                res.add(new Tuple2<>(new String(iterator.key()), new String(value)));
                            }
                            if (count == 1000 || resSize > 1024 * 1024) {
                                return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                            }

                            if (prev) {
                                iterator.prev();
                            } else {
                                iterator.next();
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }

                if (i != link.length - 1) {
                    marker = prefix + link[i + 1] + getSeparator(prefix);
                }
            }
        }

        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
    }

    private final static Set<String> _SET = new HashSet<String>() {
        {
            add(ROCKS_FILE_META_PREFIX);
            add(ROCKS_CHUNK_FILE_KEY);
        }
    };

    public static String getSeparator(String prefix) {
        if (_SET.contains(prefix)) {
            return "_";
        } else {
            return File.separator;
        }
    }

    public static Mono<Payload> ListShardedMetaData(Payload payload) {
        long start = System.currentTimeMillis();
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String bucket = msg.get("bucket");
        // 桶的分片所对应的vnode
        String vnode = msg.get("sourceVnode");
        // 需要迁移/平衡的两个vnode中的分界线
        String startMarker = msg.get("startMarker");
        // 元数据迁移的方向:next - 对应着rocksdb从分界线从前往后进行扫描;prev - 对应rocksdb从分界线从后往前扫描
        final DefaultMetaDataScanner.SCAN_SEQUENCE sequence = DefaultMetaDataScanner.SCAN_SEQUENCE.valueOf(msg.get("sequence"));
        // 扫描哪种类型的元数据:*、-、+、""
        String prefix = msg.get("prefix");
        // 上一次扫描到的元数据key的位置
        String marker = msg.get("marker");
        if (StringUtils.isBlank(marker)) {
            marker = startMarker;
        } else {
            Tuple2<String, String> tuple2 = getVnode(marker);
            if (!tuple2.var1.equals(prefix) || !tuple2.var2.equals(vnode)) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, String.format("bucket %s vnode %s divider %s encounter error maker:%s.", bucket, vnode, startMarker, marker));
            }
        }

        List<Tuple2<String, String>> res = new LinkedList<>();
        int count = 0;
        long resSize = 0L;

        MSRocksDB rocksDB = MSRocksDB.getRocksDB(lun);
        if (rocksDB == null) {
            return Mono.just(ERROR_PAYLOAD);
        }

        try (MSRocksIterator iterator = rocksDB.newIterator()) {
            if (sequence.equals(DefaultMetaDataScanner.SCAN_SEQUENCE.PREV)) {
                iterator.seekForPrev(marker.getBytes());
                if ((iterator.isValid() && new String(iterator.key()).equalsIgnoreCase(marker))) {
                    iterator.prev();
                }
            } else {
                iterator.seek(marker.getBytes());
                if (iterator.isValid() && new String(iterator.key()).equalsIgnoreCase(marker)) {
                    iterator.next();
                }
            }

            while (iterator.isValid()) {
                Tuple2<String, String> tuple2 = getVnode(new String(iterator.key()));
                if (tuple2.var1.equalsIgnoreCase(prefix) && tuple2.var2.equals(vnode)) {
                    if (new String(iterator.key()).startsWith(prefix + vnode + File.separator + bucket + File.separator)) {
                        byte[] key = iterator.key();
                        byte[] value = iterator.value();

                        if (new String(key).startsWith("+") && !Utils.isMetaJson(value)) {
                            JsonObject jsonObject = new JsonObject();
                            jsonObject.put("versionNum", "0");
                            value = jsonObject.toString().getBytes();
                        }

                        count++;
                        resSize += iterator.key().length + iterator.value().length;

                        if (prefix.equalsIgnoreCase(ROCKS_BUCKET_META_PREFIX)) {
                            MetaData metaData = new MetaData()
                                    .setBucket("bucket")
                                    .setKey("key")
                                    .setDeleteMark(false)
                                    .setVersionNum(VersionUtil.getVersionNum(false));
                            res.add(new Tuple2<>(new String(iterator.key()), Json.encode(metaData)));
                        } else {
                            res.add(new Tuple2<>(new String(iterator.key()), new String(value)));
                        }

                        if (count == 1000 || resSize > 1024 * 1024) {
                            return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                        }

                        if (sequence.equals(DefaultMetaDataScanner.SCAN_SEQUENCE.PREV)) {
                            iterator.prev();
                        } else {
                            iterator.next();
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        long end = System.currentTimeMillis();
        long cost = end - start;
        if (cost >= 30000) {
            log.warn("bucket {} ListShardedMetaData cost too much time:{} ms.", bucket, cost);
        } else {
            log.debug("bucket {} ListShardedMetaData cost:{} ms.", bucket, cost);
        }
        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
    }

    public static Mono<Payload> listVersions(Payload payload) throws RocksDBException {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        int maxKeys = Integer.parseInt(msg.get("maxKeys"));
        String prefix = msg.get("prefix");
        String marker = msg.get("marker");
        String delimiter = msg.get("delimiter");
        String versionIdMarker = msg.get("versionIdMarker");
        String vnode = msg.get("vnode");
        String limit = msg.get("limit");
        String snapshotLink = msg.get("snapshotLink");
        String currentSnapshotMark = msg.get("currentSnapshotMark");
        Function<String, List<Tuple3<Boolean, String, MetaData>>> listVersionFunction = (snapshotMark) -> {
            String realPrefix = Utils.getMetaDataKey(vnode, bucket, prefix, snapshotMark);

            ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucket).getBucketShardCache().get(bucket);
            Tuple2<String, String> upperBoundAndLowerBound = objectSplitTree.queryNodeUpperBoundAndLowerBound(vnode);
            String lowerPrefix = Utils.getMetaDataKey(vnode, bucket, prefix, snapshotMark);
            String upperPrefix = Utils.getMetaDataKey(vnode, bucket + ROCKS_SMALLEST_KEY, prefix, snapshotMark);
            if (upperBoundAndLowerBound != null) {
                if (StringUtils.isNotBlank(upperBoundAndLowerBound.var1)) {
                    lowerPrefix = Utils.getMetaDataKey(vnode, bucket, upperBoundAndLowerBound.var1 + ONE_STR, snapshotMark);
                }
                if (StringUtils.isNotBlank(upperBoundAndLowerBound.var2)) {
                    upperPrefix = Utils.getMetaDataKey(vnode, bucket, upperBoundAndLowerBound.var2 + ONE_STR, snapshotMark);
                }
            }
            MSRocksDB db = MSRocksDB.getRocksDB(msg.get("lun"));
            int count = 0;
            boolean reverseLookup = false;
            String realMarker;
            String preKey = null;
            String lastKey;
            try (Slice lowerSlice = new Slice(lowerPrefix.getBytes());
                 Slice upperSlice = new Slice(upperPrefix.getBytes());
                 ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice).setIterateUpperBound(upperSlice);
                 MSRocksIterator keyIterator = db.newIterator(readOptions)) {
                if (StringUtils.isBlank(marker) && StringUtils.isBlank(prefix)) {
                    keyIterator.seek(Utils.getMetaDataKey(vnode, bucket, "", snapshotMark).getBytes());
                } else if (StringUtils.isNotBlank(versionIdMarker) && StringUtils.isNotBlank(marker)) {
                    realMarker = Utils.getVersionMetaDataKey(vnode, bucket, marker, versionIdMarker, snapshotMark);
                    byte[] realVersionValue = db.get(realMarker.getBytes());
                    keyIterator.seek(realMarker.getBytes());
                    if (count <= maxKeys && realVersionValue != null) {
                        MetaData value = Json.decodeValue(new String(realVersionValue), MetaData.class);
                        realMarker = Utils.getMetaDataKey(vnode, bucket, marker, value.getVersionId(), value.getStamp(), snapshotMark);
                        keyIterator.seek(realMarker.getBytes());
                        if (keyIterator.isValid() && new String(keyIterator.key()).equals(realMarker)) {
                            keyIterator.prev();
                            if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(realPrefix)) {
                                keyIterator.seek((Utils.getMetaDataKey(vnode, bucket, marker + ONE_STR, snapshotMark)).getBytes());
                            } else {
                                value = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                                if (!value.key.equals(marker)) {
                                    keyIterator.seek((Utils.getMetaDataKey(vnode, bucket, marker + ONE_STR, snapshotMark)).getBytes());
                                } else {
                                    reverseLookup = true;
                                }
                            }
                        } else {
                            keyIterator.seek((Utils.getMetaDataKey(vnode, bucket, marker + ONE_STR, snapshotMark)).getBytes());
                        }
                    } else {
                        if (snapshotMark == null || snapshotMark.equals(currentSnapshotMark)) {
                            keyIterator.seek((Utils.getMetaDataKey(vnode, bucket, marker + ONE_STR, snapshotMark)).getBytes());
                        } else {
                            keyIterator.seek((Utils.getMetaDataKey(vnode, bucket, marker + ONE_STR, snapshotMark)).getBytes());
                            keyIterator.prev();
                            if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(realPrefix)) {
                                keyIterator.seek((Utils.getMetaDataKey(vnode, bucket, marker + ONE_STR, snapshotMark)).getBytes());
                            } else {
                                MetaData value = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                                if (!value.key.equals(marker)) {
                                    keyIterator.seek((Utils.getMetaDataKey(vnode, bucket, marker + ONE_STR, snapshotMark)).getBytes());
                                } else {
                                    reverseLookup = true;
                                }
                            }
                        }
                    }
                } else if (prefix.compareTo(marker) > 0) {
                    realMarker = Utils.getMetaDataKey(vnode, bucket, prefix, snapshotMark);
                    keyIterator.seek(realMarker.getBytes());
                } else if (StringUtils.isNotBlank(delimiter) && marker.endsWith(delimiter)) {
                    byte[] bytes = marker.getBytes();
                    bytes[bytes.length - 1] += 1;
                    realMarker = Utils.getMetaDataKey(vnode, bucket, new String(bytes), snapshotMark);
                    keyIterator.seek(realMarker.getBytes());
                } else {
                    realMarker = Utils.getMetaDataKey(vnode, bucket, marker, snapshotMark);
                    keyIterator.seek(realMarker.getBytes());
                    if (keyIterator.isValid()) {
                        MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                        if (metaData.getKey().equals(marker)) {
                            keyIterator.seek((realMarker + ONE_STR).getBytes());
                        }
                    }
                }

                if (keyIterator.isValid() && !reverseLookup && new String(keyIterator.key()).startsWith(realPrefix)) {
                    MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                    if (metaData.bucket.equals(bucket)) {
                        realMarker = Utils.getMetaDataKey(vnode, bucket, metaData.key + ONE_STR, snapshotMark);
                        keyIterator.seekForPrev(realMarker.getBytes());
                    }
                }

                List<Tuple3<Boolean, String, MetaData>> res = new LinkedList<>();
                while (count <= maxKeys && keyIterator.isValid()) {
                    String metaKey = new String(keyIterator.key());
                    if (!metaKey.startsWith(realPrefix)) {
                        break;
                    }
                    MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                    metaData.partInfos = null;
                    metaData.userMetaData = null;
                    metaData.objectAcl = null;
                    metaData.fileName = null;
                    if (metaData.inode > 0) {
                        try {
                            String inodeKey = Inode.getKey(vnode, bucket, metaData.inode);
                            if (db != null) {
                                byte[] value = db.get(inodeKey.getBytes());
                                if (value != null) {
                                    Inode inode = Json.decodeValue(new String(value), Inode.class);
                                    inode.setUpdateChunk(null);
                                    inode.setChunkFileMap(null);
                                    metaData.setEndIndex(inode.getSize() - 1);
                                    Inode.mergeMeta(metaData, inode);
                                    metaData.partInfos = null;
                                    metaData.tmpInodeStr = null;
                                } else {
                                    metaData.setEndIndex(-1);
                                    metaData.setVersionNum("0");
                                }
                            }
                        } catch (RocksDBException e) {
                            log.error("get inodeValue error", e);
                        }
                    }

                    String key = metaData.getKey();
                    if (reverseLookup && !key.equals(marker)) {
                        keyIterator.seek((Utils.getMetaDataKey(vnode, bucket, marker, snapshotMark) + ONE_STR).getBytes());
                        if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(realPrefix)) {
                            break;
                        }
                        String nextKey = Json.decodeValue(new String(keyIterator.value()), MetaData.class).key;
                        keyIterator.seekForPrev((Utils.getMetaDataKey(vnode, bucket, nextKey, snapshotMark) + ONE_STR).getBytes());
                        reverseLookup = false;
                        continue;
                    }

                    if (count == 0) {
                        keyIterator.next();
                        if (keyIterator.isValid()) {
                            if (new String(keyIterator.key()).startsWith(realPrefix)) {
                                MetaData metaValue = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                                metaData.setLatest(!key.equals(metaValue.getKey()) || metaValue.isUnView(currentSnapshotMark));
                            } else {
                                metaData.setLatest(true);
                            }
                            keyIterator.prev();
                        } else {
                            metaData.setLatest(true);
                            keyIterator.seek(metaKey.getBytes());
                        }
                    } else {
                        metaData.setLatest(!preKey.equals(key));
                    }
                    if (metaData.isLatest() && !metaData.isCurrentSnapshotObject(currentSnapshotMark) && metaData.isViewable(currentSnapshotMark)) {
                        metaData.setLatest(metaData.latestMetaIsViewable(currentSnapshotMark));
                        if (metaData.isLatest()) {
                            String currentLatestKey = Utils.getLatestMetaKey(vnode, bucket, metaData.key, currentSnapshotMark);
                            byte[] bytes = db.get(currentLatestKey.getBytes());
                            if (bytes != null) {
                                MetaData meta = Json.decodeValue(new String(bytes), MetaData.class);
                                metaData.setLatest(meta.snapshotMark.equals(metaData.snapshotMark));
                            }
                        }
                    }
                    lastKey = key;
                    if (metaData.isViewable(currentSnapshotMark)) {
                        preKey = key;
                        count++;
                    }
                    if (StringUtils.isNotBlank(delimiter)) {
                        int index = key.substring(prefix.length()).indexOf(delimiter);
                        if (index >= 0) {
                            String keyName = key.substring(0, index + prefix.length() + delimiter.length());
                            byte[] keyBytes = Utils.getMetaDataKey(vnode, bucket, keyName, snapshotMark).getBytes();
                            keyBytes[keyBytes.length - 1] += 1;
                            keyIterator.seek(keyBytes);
                            metaData.setKey(keyName);
                            if (limit != null) {
                                if (keyName.equals(prefix) && metaData.isViewable(currentSnapshotMark)) {
                                    res.add(new Tuple3<>(true, keyName, metaData));
                                }
                            } else if (metaData.isViewable(currentSnapshotMark)) {
                                res.add(new Tuple3<>(true, keyName, metaData));
                            }
                            if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(realPrefix)) {
                                break;
                            }
                            String nextKey = Json.decodeValue(new String(keyIterator.value()), MetaData.class).key;
                            keyIterator.seekForPrev((Utils.getMetaDataKey(vnode, bucket, nextKey, snapshotMark) + ONE_STR).getBytes());
                            reverseLookup = false;
                            continue;
                        } else {
                            if (limit != null) {
                                if (key.equals(prefix) && metaData.isViewable(currentSnapshotMark)) {
                                    res.add(new Tuple3<>(false, key, metaData));
                                }
                            } else if (metaData.isViewable(currentSnapshotMark)) {
                                res.add(new Tuple3<>(false, key, metaData));
                            }
                            keyIterator.prev();
                        }
                    } else {
                        if (limit != null) {
                            if (key.equals(prefix) && metaData.isViewable(currentSnapshotMark)) {
                                res.add(new Tuple3<>(false, key, metaData));
                            }
                        } else if (metaData.isViewable(currentSnapshotMark)) {
                            res.add(new Tuple3<>(false, key, metaData));
                        }
                        keyIterator.prev();
                    }

                    boolean next = false;
                    MetaData lastMetaData;
                    if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(realPrefix)) {
                        next = true;
                    } else {
                        lastMetaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                        if (!lastKey.equals(lastMetaData.key) || metaKey.equals(new String(keyIterator.key()))) {
                            next = true;
                        }
                    }
                    if (next) {
                        keyIterator.seek((Utils.getMetaDataKey(vnode, bucket, lastKey + ONE_STR, snapshotMark)).getBytes());
                        if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(realPrefix)) {
                            lastMetaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                            keyIterator.seekForPrev((Utils.getMetaDataKey(vnode, bucket, lastMetaData.key, snapshotMark) + ONE_STR).getBytes());
                        }
                    }
                }
                return res;
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        };
        List<Tuple3<Boolean, String, MetaData>> res = executeListOperation(listVersionFunction, new SnapshotListVersionService(), currentSnapshotMark, snapshotLink, maxKeys);
        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
    }

    public static Mono<Payload> listSyncRecorder(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        long maxKeys = Long.parseLong(msg.get("maxKeys"));
        if (maxKeys <= 0) {
            maxKeys = ListSyncRecorderHandler.MAX_COUNT;
        }
        List<Tuple2<String, UnSynchronizedRecord>> res = new LinkedList<>();
        MSRocksDB db = MSRocksDB.getRocksDB(msg.get("lun"));
        String bucket = msg.get("bucket");
        String marker = msg.get("marker");
        int clusterIndex = Integer.parseInt(msg.get("clusterIndex"));
        if (db == null) {
            log.error("listSyncRecorder interrupt, no db {}", msg.get("lun"));
            return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
        }

        String recordKeyPrefix = "";
        if ("1".equals(msg.get("deleteSource"))) {
            recordKeyPrefix = UnSynchronizedRecord.getRecorderPrefixLocal(bucket);
        } else if ("1".equals(msg.get("onlyDelete"))) {
            recordKeyPrefix = UnSynchronizedRecord.getOnlyDeletePrefix(bucket);
        } else {
            recordKeyPrefix = UnSynchronizedRecord.getRecorderPrefixAsync(bucket, clusterIndex, !msg.get("lun").endsWith(UnsyncRecordDir));
        }

        try (Slice lowerSlice = new Slice(marker);
             Slice upperSlice = new Slice(recordKeyPrefix + ROCKS_SMALLEST_KEY);
             ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
             MSRocksIterator iterator = db.newIterator(readOptions)) {
            iterator.seek(marker.getBytes());
            int count = 0;
            while (iterator.isValid() && new String(iterator.key()).startsWith(recordKeyPrefix)) {
                if (IS_THREE_SYNC) {
                    if (!LOCAL_CLUSTER_INDEX.equals(THREE_SYNC_INDEX) && clusterIndex != THREE_SYNC_INDEX
                            && UnSynchronizedRecord.isAsyncRecord(new String(iterator.key()))) {
                        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                    }
                } else {
                    // 非async站点且指针已经到了async记录，说明没有该双活站点的相关记录，返回list结果
                    if (!ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)
                            && UnSynchronizedRecord.isAsyncRecord(new String(iterator.key()))) {
                        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                    }
                }
                UnSynchronizedRecord record = Json.decodeValue(new String(iterator.value()), UnSynchronizedRecord.class);
                if ("1".equals(msg.get("onlyDelete"))){
                    // 关同步开关删除时要扫描到after_init
                } else {
                    if (record.deleteMark || record.type() == UnSynchronizedRecord.Type.NONE) {
                        iterator.next();
                        continue;
                    }
                }
                record.headers.put(IS_SYNCING, "1");
//                String stamp = record.headers.get("stamp");
//                // EC_version一秒钟+nodeAmount，客户端速度为1MB/s，此即为5G的对象上传完毕前后的EC_version的变化量。
//                int nodeNum = INDEX_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX).length;
//                long nodeAmount = UPLOAD_MAX_INTERVAL * nodeNum * 1000L;
//                // 没到时间的预提交记录暂不不处理
//                if (!record.commited && ((System.currentTimeMillis() - Long.parseLong(stamp)) * nodeNum < nodeAmount)) {
//                    record.deleteMark = true;
//                }


                count++;
                res.add(new Tuple2<>(new String(iterator.key()), record));
                if (count >= maxKeys) {
                    return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                }
                iterator.next();
            }
        }

        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
    }

    public static Mono<Payload> listSyncRecordCheckpoint(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        int maxKeys = Integer.parseInt(msg.get("maxKeys"));
        List<Tuple2<String, UnSynchronizedRecord>> res = new LinkedList<>();
        String lun = msg.get("lun");
        MSRocksDB db = MSRocksDB.getRocksDB(lun);
        String bucket = msg.get("bucket");
        String marker = msg.get("marker");
        int clusterIndex = Integer.parseInt(msg.get("clusterIndex"));

        String recordKeyPrefix = UnSynchronizedRecord.getRecorderPrefixAsync(bucket, clusterIndex, false);

        try (Slice lowerSlice = new Slice(marker.getBytes());
             Slice upperSlice = new Slice((recordKeyPrefix + ROCKS_SMALLEST_KEY).getBytes());
             ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
             MSRocksIterator iterator = db.newIterator(readOptions)) {
            iterator.seek(marker.getBytes());
            int count = 0;
            while (iterator.isValid() && new String(iterator.key()).startsWith(recordKeyPrefix)) {
                if (IS_THREE_SYNC) {
                    if (!LOCAL_CLUSTER_INDEX.equals(THREE_SYNC_INDEX) && clusterIndex != THREE_SYNC_INDEX
                            && UnSynchronizedRecord.isAsyncRecord(new String(iterator.key()))) {
                        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                    }
                } else {
                    // 非async站点且指针已经到了async记录，说明没有该双活站点的相关记录，返回list结果
                    if (!ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)
                            && UnSynchronizedRecord.isAsyncRecord(new String(iterator.key()))) {
                        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                    }
                }
                UnSynchronizedRecord record = Json.decodeValue(new String(iterator.value()), UnSynchronizedRecord.class);
                if (record.deleteMark) {
                    iterator.next();
                    continue;
                }
                count++;
                res.add(new Tuple2<>(new String(iterator.key()), record));
                if (count >= maxKeys) {
                    return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                }
                iterator.next();
            }
        }

        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
    }

    public static Mono<Payload> dealHisCheckPoint(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        int index = Integer.parseInt(msg.get("index"));
        RocksDBCheckPoint.init(index);
        return Mono.just(SUCCESS_PAYLOAD);
    }


    public static Mono<Payload> listComponentTask(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        int maxKeys = Integer.parseInt(msg.get("maxKeys"));
        List<Tuple2<String, ComponentRecord>> res = new LinkedList<>();
        String lun = msg.get("lun");
        MSRocksDB db = MSRocksDB.getRocksDB(lun);
        String type = msg.get("type");
        String bucket = msg.get("bucket");
        String marker = msg.get("marker");
        String taskMarker = msg.get("taskMarker");

        String rocksKeyPrefix = ComponentRecord.rocksKeyPrefix(bucket, type, taskMarker);
        if (StringUtils.isBlank(marker)) {
            marker = rocksKeyPrefix;
        }

        try (Slice lowerSlice = new Slice(marker.getBytes());
             Slice upperSlice = new Slice((rocksKeyPrefix + ROCKS_SMALLEST_KEY).getBytes());
             ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
             MSRocksIterator iterator = db.newIterator(readOptions)) {
            iterator.seek(marker.getBytes());
            int count = 0;
            while (iterator.isValid() && new String(iterator.key()).startsWith(rocksKeyPrefix)) {
                ComponentRecord task = Json.decodeValue(new String(iterator.value()), ComponentRecord.class);
                count++;
                res.add(new Tuple2<>(new String(iterator.key()), task));
                if (count >= maxKeys) {
                    return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                }
                iterator.next();
            }
        }

        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
    }

    public static Mono<Payload> listDirPlus(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String prefix = msg.get("prefix");
        String marker = msg.get("marker");
        String bucket = msg.get("bucket");
        long maxSize = Long.parseLong(msg.get("maxSize"));
        String lun = msg.get("lun");
        String vnode = msg.get("vnode");
        String pattern = msg.dataMap.getOrDefault("pattern", "*");
        int queryClass = Integer.parseInt(msg.dataMap.getOrDefault("queryClass", "-1"));

        List<Tuple2<String, Inode>> res = new LinkedList<>();

        String realMarker = Utils.getLatestMetaKey(vnode, bucket, marker);
        String realPrefix = Utils.getLatestMetaKey(vnode, bucket, prefix);
        byte[] realSeek;

        if (StringUtils.isNotBlank(marker)) {
            //跳过文件夹
            realSeek = realMarker.getBytes();

            if (marker.endsWith("/")) {
                realSeek[realSeek.length - 1] += 1;
            }

        } else {
            realSeek = realPrefix.getBytes();
        }

        try (MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator()) {
            iterator.seek(realSeek);

            if (iterator.isValid()) {
                if (StringUtils.isNotBlank(marker)) {
                    if (realMarker.equals(new String(iterator.key()))) {
                        iterator.next();
                    }
                } else {
                    //目录对象本身
                    if (realPrefix.equals(new String(iterator.key()))) {
                        iterator.next();
                    }
                }
            }

            // 如果当前的pattern模式是指定文件模式而不是通配符模式，则可以为search pattern加速
            // 如果是指定文件模式，匹配项应当是prefix目录下的pattern条目
            AtomicBoolean returnEmpty = new AtomicBoolean(false);
            if (!pattern.contains("*") && !pattern.contains("?")) {
                seekPattern(realPrefix, pattern, iterator,returnEmpty);

            }
            if (!"*".equals(pattern)) {
                if (pattern.endsWith("*")) {
                    String p0 = pattern.substring(0, pattern.length() - 1);
                    if (!p0.contains("*") && !p0.contains("?")) {
                        seekPattern(realPrefix, p0, iterator,returnEmpty);
                    }
                }
            }

            if (returnEmpty.get()){
                return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
            }
            int size = 0;

            while (iterator.isValid() && new String(iterator.key()).startsWith(realPrefix)) {
                String key = new String(iterator.key());
                String subKey = key.substring(realPrefix.length());

                if (!"*".equals(pattern) && !CifsUtils.isMatch(subKey, pattern)) {
                    String f = subKey;
                    if (subKey.contains("/")) {
                        f = subKey.substring(0, subKey.indexOf("/"));
                    }

                    if (!CifsUtils.isMatch(f, pattern)) {
                        iterator.next();
                        continue;
                    }
                }

                Inode inode = null;
                MetaData metaData = Json.decodeValue(new String(iterator.value()), MetaData.class);

                //dir
                if (subKey.contains("/")) {
                    String dir = subKey.substring(0, subKey.indexOf("/") + 1);
                    if (dir.equals(subKey)) {
                        //fuse dir
                        boolean newDirInode = true;
                        boolean notRepairInode = false;
                        if (metaData.inode > 0) {
                            String inodeKey = Inode.getKey(vnode, bucket, metaData.inode);
                            byte[] inodeBytes = MSRocksDB.getRocksDB(lun).get(inodeKey.getBytes());
                            if (inodeBytes != null) {
                                inode = Json.decodeValue(new String(MSRocksDB.getRocksDB(lun).get(inodeKey.getBytes())), Inode.class);
                                inode.setCookie(metaData.cookie);
                                inode.setObjName(metaData.getKey());
                                inode.setVersionId(metaData.getVersionId());
                                inode.getInodeData().clear();
                                CifsUtils.setDefaultCifsMode(inode);
                                newDirInode = false;
                            } else {
                                notRepairInode = true;
                            }
                        }
                        if (newDirInode) {
                            inode = new Inode()
                                    .setVersionNum(!notRepairInode ? metaData.getVersionNum() : "0")
                                    .setBucket(metaData.getBucket())
                                    .setVersionId(metaData.getVersionId())
                                    .setObjName(metaData.getKey())
                                    .setReference(Utils.metaHash(metaData));
//                                    .setCookie(metaData.getCookie());
                        }
                    } else {
                        //s3 dir 并且dir对象不存在
                        inode = new Inode()
                                .setVersionNum("")
                                .setBucket(metaData.getBucket())
                                //多版本会出现重复的versionId，但是对象名不同，暂不处理
                                .setVersionId(metaData.getVersionId())
                                .setReference(DEFAULT_META_HASH)
                                .setObjName(prefix + dir);
                    }

                    byte[] nextSeek = (realPrefix + dir).getBytes();
                    nextSeek[nextSeek.length - 1] += 1;
                    iterator.seek(nextSeek);
                } else {
                    boolean newObjInode = true;
                    boolean notRepairInode = false;
                    if (metaData.inode > 0) {
                        //fuse file
                        String inodeKey = Inode.getKey(vnode, bucket, metaData.inode);
                        byte[] dirInodeBytes = MSRocksDB.getRocksDB(lun).get(inodeKey.getBytes());
                        if (dirInodeBytes != null) {
                            inode = Json.decodeValue(new String(dirInodeBytes), Inode.class);
                            //处理硬链接
                            inode.setCookie(metaData.cookie);
                            inode.setObjName(metaData.getKey());
                            inode.setVersionId(metaData.getVersionId());
                            inode.getInodeData().clear();
                            CifsUtils.setDefaultCifsMode(inode);
                            newObjInode = false;
                        } else {
                            notRepairInode = true;
                        }
                    }
                    if (newObjInode) {
                        //s3 file
                        inode = new Inode()
                                .setVersionNum(!notRepairInode ? metaData.versionNum : "0")
                                .setBucket(metaData.getBucket())
                                .setVersionId(metaData.getVersionId())
                                .setObjName(metaData.getKey())
                                .setReference(Utils.metaHash(metaData));
//                                .setCookie(metaData.getCookie());
                    }

                    iterator.next();
                }

                res.add(new Tuple2<>(key, inode));
                size += inode.countQuerySize(queryClass, prefix);

                if (size >= maxSize) {
                    return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                }
                if (!pattern.contains("*") && !pattern.contains("?")) {
                    if (res.size() == 1) {
                        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(ERROR_PAYLOAD);
        }


        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
    }

    public static void seekPattern(String realPrefix, String pattern, MSRocksIterator iterator,AtomicBoolean returnEmpty) {
        byte[] before = iterator.key();
        boolean isSeekBack = true;
        String patternStart = realPrefix + pattern;

        try {
            iterator.seek(patternStart.getBytes());
            String key = new String(iterator.key());

            if (key.startsWith(patternStart)) {
                isSeekBack = false;
            }

//            if (isSeekBack) {
//                iterator.seek(before);
//            }
            returnEmpty.set(isSeekBack);

        } catch (Exception e) {
            log.error("pattern seek error, pattern: {}, patternStart: {}", pattern, patternStart, e);
            iterator.seek(before);
        }
    }

    public static <T> List<T> executeListOperation(Function<String, List<T>> listFunction, SnapshotMergeService<T> snapshotObjectService, String currentSnapshotMark, String snapshotLink, int maxKeys) {
        if (StringUtils.isAnyBlank(currentSnapshotMark, snapshotLink)) {
            // 未开桶快照，或开启桶快照但为创建快照
            return listFunction.apply(currentSnapshotMark);
        }
        // 开桶快照，并且创建快照，则需要进行数据合并
        TreeSet<String> snapshotMarks = Json.decodeValue(snapshotLink, new TypeReference<TreeSet<String>>() {
        });
        return snapshotObjectService.listMerge(snapshotMarks, currentSnapshotMark, maxKeys, listFunction);

    }

    public static Mono<Payload> listAggregationUndoLog(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        int maxKeys = Integer.parseInt(msg.get("maxKeys"));
        String lun = msg.get("lun");
        MSRocksDB db = MSRocksDB.getRocksDB(lun);
        String namespace = msg.get("namespace");
        String nodeUuid = msg.get("nodeUuid");
        String startStamp = msg.get("startStamp");
        String endStamp = msg.get("endStamp");
        String marker = msg.get("marker");
        String rocksKeyPrefix = UndoLog.rocksKeyPrefix(namespace, nodeUuid);
        String lowerMarker = UndoLog.rocksKeyPrefix(namespace, nodeUuid, startStamp);
        String upperMarker = UndoLog.rocksKeyPrefix(namespace, nodeUuid, endStamp);
        if (StringUtils.isEmpty(marker)) {
            marker = lowerMarker;
        }

        List<Tuple2<String, UndoLog>> res = new LinkedList<>();

        try (Slice lowerSlice = new Slice(marker.getBytes());
             Slice upperSlice = new Slice(upperMarker.getBytes());
             ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
             MSRocksIterator iterator = db.newIterator(readOptions)) {
            iterator.seek(marker.getBytes());
            int count = 0;
            while (iterator.isValid() && new String(iterator.key()).startsWith(rocksKeyPrefix)) {
                if (marker.equals(new String(iterator.key()))) {
                    iterator.next();
                    continue;
                }
                UndoLog undoLog = Json.decodeValue(new String(iterator.value()), UndoLog.class);
                count++;
                res.add(new Tuple2<>(new String(iterator.key()), undoLog));
                if (count >= maxKeys) {
                    return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
                }
                iterator.next();
            }
        }
        return Mono.just(DefaultPayload.create(Json.encode(res.toArray()), SUCCESS.name()));
    }

}
