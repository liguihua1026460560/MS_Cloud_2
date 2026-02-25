package com.macrosan.snapshot.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.batch.WriteBatch;
import com.macrosan.ec.Utils;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.snapshot.pojo.SnapshotMergeTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.KeyValue;
import io.vertx.core.json.Json;
import org.rocksdb.*;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.Utils.*;

/**
 * @author zhaoyang
 * @date 2024/07/30
 **/
public class SnapshotUtil {
    /**
     * 获取桶快照相关信息，并放入msg中
     *
     * @param bucket 桶名
     * @param msg    信息
     * @return void
     */
    public static Mono<Void> fetchBucketSnapshotInfo(String bucket, SocketReqMsg msg) {
        return RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX)
                .hmget(bucket, CURRENT_SNAPSHOT_MARK, SNAPSHOT_LINK)
                .collectList()
                .doOnNext(keyValues -> keyValues.forEach(kv -> {
                    if (CURRENT_SNAPSHOT_MARK.equals(kv.getKey())) {
                        msg.put("currentSnapshotMark", kv.getValueOrElse(null));
                    } else if (SNAPSHOT_LINK.equals(kv.getKey())) {
                        msg.put("snapshotLink", kv.getValueOrElse(null));
                    }
                })).then();
    }

    public static Mono<TreeSet<String>> fetchBucketSnapshotInfo(String bucket) {
        return RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX)
                .hmget(bucket, CURRENT_SNAPSHOT_MARK, SNAPSHOT_LINK)
                .collectList()
                .map(keyValues -> {
                    TreeSet<String> result = new TreeSet<>(Comparator.reverseOrder());
                    if (keyValues.get(0) != null && keyValues.get(0).getValue() != null) {
                        result.add(keyValues.get(0).getValue());
                    }
                    String snapshotLink = keyValues.get(1).getValueOrElse(null);
                    if (snapshotLink != null) {
                        List<String> snapshotLinks = Json.decodeValue(snapshotLink, new TypeReference<List<String>>() {
                        });
                        result.addAll(snapshotLinks);
                    }
                    return result;
                });
    }

    public static Mono<SnapshotMergeTask> getBucketSnapshotMergeTask(String bucket, String srcMark) {
        return RedisConnPool.getInstance().getReactive(REDIS_SNAPSHOT_INDEX).hget(SNAPSHOT_MERGE_TASK_PREFIX + bucket, srcMark)
                .map(mergeTask -> Json.decodeValue(mergeTask, SnapshotMergeTask.class));

    }

    public static Mono<String> getDataMergeMapping(String bucket) {
        return RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, DATA_MERGE_MAPPING)
                .defaultIfEmpty("");
    }


    /**
     * 获取桶快照相关信息
     *
     * @param bucket 桶名
     * @return <\currentSnapshotMark,snapshotLink>
     */
    public static Tuple2<String, String> getBucketSnapshotInfo(String bucket) {
        List<KeyValue<String, String>> snapshotInfo = RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hmget(bucket, CURRENT_SNAPSHOT_MARK, SNAPSHOT_LINK);
        return new Tuple2<>(snapshotInfo.get(0).getValueOrElse(null), snapshotInfo.get(1).getValueOrElse(null));
    }


    /**
     * 检测桶是否开启桶快照
     *
     * @param bucket 桶名
     * @return 开启桶快照--返回true
     */
    public static boolean checkBucketSnapshotEnable(String bucket) {
        return "on".equals(RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, SNAPSHOT_SWITCH));
    }

    /**
     * 判断桶中是否存在已创建快照或正在删除的快照或由于快照回滚还未清除完毕的快照
     *
     * @param bucket 桶名
     * @return 是否存在已创建快照或正在删除的快照
     */
    public static boolean hasSnapshotInBucket(String bucket) {
        boolean res = RedisConnPool.getInstance().getCommand(REDIS_SNAPSHOT_INDEX).hlen(BUCKET_SNAPSHOT_PREFIX + bucket) > 0;
        if (!res) {
            return RedisConnPool.getInstance().getCommand(REDIS_SNAPSHOT_INDEX).scard(SNAPSHOT_DISCARD_LIST_PREFIX + bucket) > 0L;
        }
        return true;
    }

    public static void checkOperationCompatibility(String snapshotSwitch) {
        if ("on".equals(snapshotSwitch)) {
            throw new MsException(ErrorNo.OPERATION_CONFLICT, "The current operation is not allowed for the bucket that enables the bucket snapshot.");
        }
    }

    public static void checkOperationCompatibility(Map<String, String> bucketInfo) {
        checkOperationCompatibility(bucketInfo.get(SNAPSHOT_SWITCH));
    }

    /**
     * 判断对象的快照标记是否存在
     * 快照回滚或已删除，则快照标记不存在
     *
     * @param objectSnapshotMark 对象元数据中的快照标记
     * @return 快照标记是否存在
     */
    public static Mono<Boolean> checkSnapshotMarkPresent(String objectSnapshotMark, String bucket) {
        if (objectSnapshotMark == null) {
            return Mono.just(true);
        }
        return RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX)
                .hmget(bucket, CURRENT_SNAPSHOT_MARK, SNAPSHOT_LINK)
                .collectList()
                .map(keyValues -> keyValues.stream().map(kv -> kv.getValueOrElse(null)).filter(Objects::nonNull).collect(Collectors.toSet()).contains(objectSnapshotMark));
    }

    /**
     * 版本号递增
     *
     * @param versionNum 版本号
     */
    public static String getVersionIncrement(String versionNum) {
        String[] versionSplit = versionNum.split("-");
        return versionSplit[0] + "-" + (Integer.parseInt(versionSplit[1]) + 1);
    }


    /**
     * 桶快照 逻辑删除处理
     */
    public static void snapshotLogicalDeleteProcessing(RocksDB db, WriteBatch writeBatch, String snapshotLink, MetaData newMeta, String vnode, boolean versionEnabled) throws RocksDBException {
        TreeSet<String> snapshotLinks = Json.decodeValue(snapshotLink, new TypeReference<TreeSet<String>>() {
        });
        String prevSnapshotMark = snapshotLinks.first();
        if (prevSnapshotMark.equals(newMeta.snapshotMark)) {
            return;
        }
        byte[] simplified = null;
        // 同名对象进行逻辑删除
        String prevVersionKey = getVersionMetaDataKey(vnode, newMeta.bucket, newMeta.key, newMeta.versionId, prevSnapshotMark);
        byte[] prevSnapObj = writeBatch.getFromBatchAndDB(db, prevVersionKey.getBytes());
        if (prevSnapObj != null) {
            MetaData prevSnapMeta = Json.decodeValue(new String(prevSnapObj), MetaData.class);
            if (!prevSnapMeta.deleteMark && prevSnapMeta.isViewable(newMeta.snapshotMark)) {
                String metaKey = getMetaDataKey(vnode, prevSnapMeta.bucket, prevSnapMeta.key, prevSnapMeta.versionId, versionEnabled ? prevSnapMeta.stamp : "0", prevSnapshotMark);
                prevSnapMeta.addUnViewSnapshotMark(newMeta.snapshotMark);
                prevSnapMeta.setWeakUnView(null);
                prevSnapMeta.setVersionNum(getVersionIncrement(prevSnapMeta.versionNum));
                simplified = simplifyMetaJson(prevSnapMeta);
                byte[] prevMetaBytes = Json.encode(prevSnapMeta).getBytes();
                writeBatch.put(prevVersionKey.getBytes(), prevMetaBytes);
                writeBatch.put(metaKey.getBytes(), simplified);
            }
        }

        String prevMarkLatestKey = Utils.getLatestMetaKey(vnode, newMeta.bucket, newMeta.key, prevSnapshotMark);
        byte[] prevLatestMeta = writeBatch.getFromBatchAndDB(db, prevMarkLatestKey.getBytes());
        if (prevLatestMeta == null) {
            return;
        }
        MetaData prevLatestMetaData = Json.decodeValue(new String(prevLatestMeta), MetaData.class);
        if (prevLatestMetaData.versionId.equals(NULL) || prevLatestMetaData.versionId.equals(newMeta.versionId)) {
            if (simplified != null) {
                writeBatch.put(prevMarkLatestKey.getBytes(), simplified);
            }
            return;
        }
        if (!prevLatestMetaData.latestMetaIsViewable(newMeta.snapshotMark)) {
            return;
        }
        // latestKey对应版本号不为null，则置为weakLatestKey，表示只有latestKey不可见
        String latestMetaPrevVersionKey = getVersionMetaDataKey(vnode, prevLatestMetaData.bucket, prevLatestMetaData.key, prevLatestMetaData.versionId, prevSnapshotMark);
        byte[] latestMetaPrevSnapObj = writeBatch.getFromBatchAndDB(db, latestMetaPrevVersionKey.getBytes());
        MetaData prevSnapMeta = Json.decodeValue(new String(latestMetaPrevSnapObj), MetaData.class);
        prevSnapMeta.setVersionNum(getVersionIncrement(prevSnapMeta.versionNum));
        prevSnapMeta.addWeakUnViewSnapshotMark(newMeta.snapshotMark);
        simplified = simplifyMetaJson(prevSnapMeta);
        byte[] prevMetaBytes = Json.encode(prevSnapMeta).getBytes();
        String metaKey = getMetaDataKey(vnode, prevSnapMeta.bucket, prevSnapMeta.key, prevSnapMeta.versionId, versionEnabled ? prevSnapMeta.stamp : "0", prevSnapshotMark);
        writeBatch.put(latestMetaPrevVersionKey.getBytes(), prevMetaBytes);
        writeBatch.put(metaKey.getBytes(), simplified);
        writeBatch.put(prevMarkLatestKey.getBytes(), simplified);

    }

    /**
     * 取消latestKey逻辑删除
     * 待删除对象是当中快照下唯一对象，则需取消对快照前同名对象latestKey的逻辑删除
     * A1   A2  A3
     */
    public static void cancelWeakUnView(RocksDB db, WriteBatch writeBatch, String snapshotLink, String vnode, MetaData meta) throws RocksDBException {
        List<String> snapshotLinks = Json.decodeValue(snapshotLink, new TypeReference<List<String>>() {
        });
        String prevSnapshotMark = snapshotLinks.get(0);
        if (prevSnapshotMark.equals(meta.snapshotMark)) {
            return;
        }
        String prevLatestKey = getLatestMetaKey(vnode, meta.bucket, meta.key, prevSnapshotMark);
        byte[] prevLatestBytes = writeBatch.getFromBatchAndDB(db, prevLatestKey.getBytes());
        // latestKey为null，则不需要恢复
        if (prevLatestBytes == null) {
            return;
        }
        MetaData lastestMetaData = Json.decodeValue(new String(prevLatestBytes), MetaData.class);
        // 已经是可见的了无需在移除
        if (lastestMetaData.latestMetaIsViewable(meta.snapshotMark)) {
            return;
        }
        String prevMetaVersionKey = getVersionMetaDataKey(vnode, lastestMetaData.bucket, lastestMetaData.key, lastestMetaData.versionId, prevSnapshotMark);
        byte[] metaByte = writeBatch.getFromBatchAndDB(db, prevMetaVersionKey.getBytes());
        if (metaByte == null) {
            return;
        }
        MetaData prevSnapshotMeta = Json.decodeValue(new String(metaByte), MetaData.class);
        // 可能存在只有latestKey是unView的情况，移除latestKey的unView前需判断对象元数据是否也被已被逻辑删除
        if (prevSnapshotMeta.isViewable(meta.snapshotMark)) {
            if (lastestMetaData.versionNum.compareTo(meta.versionNum) < 0) {
                // 移除快照创建前该对象的不可见列表
                prevSnapshotMeta.setWeakUnView(null);
                prevSnapshotMeta.setVersionNum(getVersionIncrement(prevSnapshotMeta.versionNum));
                String metaKey = getMetaDataKey(vnode, prevSnapshotMeta.bucket, prevSnapshotMeta.key, prevSnapshotMeta.versionId, prevSnapshotMeta.stamp, prevSnapshotMark);
                byte[] simplified = simplifyMetaJson(prevSnapshotMeta);
                writeBatch.put(prevMetaVersionKey.getBytes(), Json.encode(prevSnapshotMeta).getBytes());
                writeBatch.put(metaKey.getBytes(), simplified);
                writeBatch.put(prevLatestKey.getBytes(), simplified);
            }
        } else {
            // 如果latestKey对应的元数据也被置为unview，则说明再当前版本下将该对象删除了
            // 因此遍历快照前同名对象，选择一个没有被置为unview的最新对象并为其重新上传一个latestKey
            String prevMetaKey = Utils.getMetaDataKey(vnode, lastestMetaData.bucket, lastestMetaData.key, lastestMetaData.versionId, lastestMetaData.stamp, prevSnapshotMark);
            try (Slice lowerSlice = new Slice(prevLatestKey.getBytes());
                 Slice upperSlice = new Slice((prevMetaKey.substring(1) + Utils.ONE_STR).getBytes());
                 ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                 RocksIterator iterator = db.newIterator(readOptions);
                 RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                keyIterator.seek(prevMetaKey.getBytes());
                keyIterator.prev();
                while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(prevLatestKey.substring(1))) {
                    MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                    if (metaData.isViewable(meta.snapshotMark)) {
                        String curLatestKey = replaceSnapshotMark(prevLatestKey, meta.snapshotMark);
                        writeBatch.put(curLatestKey.getBytes(), keyIterator.value());
                        break;
                    }
                    keyIterator.prev();
                }
            }
        }
    }


    /**
     * 为快照前上传的对象上传一个临时的latestKey
     */
    public static void putTempLatestKey(RocksDB db, WriteBatch writeBatch, String latestKey, String currentSnapshotMark, String vnode, MetaData meta, Tuple3<String, String, String> tuple, MetaData objMeta) throws RocksDBException {
        String curLatestKey = replaceSnapshotMark(latestKey, currentSnapshotMark);
        byte[] cuLatestBytes = writeBatch.getFromBatchAndDB(db, curLatestKey.getBytes());
        boolean putTempLatestKey = false;
        if (cuLatestBytes != null) {
            // 当前快照下存在latestKey，但是当前快照下的latestKey为临时latestKey，则可以重新put一个临时latestKey
            if (!Json.decodeValue(new String(cuLatestBytes), MetaData.class).snapshotMark.equals(currentSnapshotMark)) {
                putTempLatestKey = true;
            }
        } else {
            // 当前快照下latestKey不存在，则还需判断是否存在删除标记
            String curMetaKey = Utils.getMetaDataKey(vnode, meta.bucket, meta.key, currentSnapshotMark);
            try (Slice lowerSlice = new Slice(curLatestKey.substring(1).getBytes());
                 Slice upperSlice = new Slice((curMetaKey + Utils.ONE_STR).getBytes());
                 ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                 RocksIterator iterator = db.newIterator(readOptions);
                 RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                keyIterator.seek(curMetaKey.getBytes());
                if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(curLatestKey.substring(1))) {
                    putTempLatestKey = true;
                }
            }
        }

        if (putTempLatestKey) {
            writeBatch.delete(curLatestKey.getBytes());

            try (Slice lowerSlice = new Slice(latestKey.substring(1).getBytes());
                 Slice upperSlice = new Slice((tuple.var1 + Utils.ONE_STR).getBytes());
                 ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                 RocksIterator iterator = db.newIterator(readOptions);
                 RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {

                keyIterator.seek(tuple.var1.getBytes());
                keyIterator.prev();
                while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                    MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                    if (metaData.isUnView(currentSnapshotMark)) {
                        keyIterator.prev();
                        continue;
                    }
                    boolean insert = !meta.versionId.equals(metaData.versionId);
                    if (insert && metaData.key.equals(objMeta.key) && metaData.bucket.equals(objMeta.bucket) && !metaData.deleteMarker && !metaData.deleteMark) {
                        writeBatch.put(true, curLatestKey.getBytes(), keyIterator.value());
                        break;
                    }
                }
            }
        }
    }


}
