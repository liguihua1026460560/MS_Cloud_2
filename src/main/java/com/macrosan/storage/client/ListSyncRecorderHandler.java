package com.macrosan.storage.client;

import com.macrosan.doubleActive.SyncRecordLimiter;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.ServerConstants.CONTENT_LENGTH;
import static com.macrosan.constants.ServerConstants.RECORD_ORIGIN_UPLOADID;
import static com.macrosan.constants.SysConstants.DATASYNC_BAND_WIDTH_QUOTA;
import static com.macrosan.constants.SysConstants.DATASYNC_THROUGHPUT_QUOTA;
import static com.macrosan.doubleActive.DataSynChecker.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.*;
import static com.macrosan.doubleActive.SyncRecordLimiter.*;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.Type.*;


/**
 * @auther wuhaizhong
 * @date 2021/4/7
 */
@Log4j2
public class ListSyncRecorderHandler extends AbstractListClient<Tuple2<String, UnSynchronizedRecord>> {

    public MonoProcessor<List<UnSynchronizedRecord>> recordProcessor = MonoProcessor.create();

    public static final long MAX_COUNT = 1000;

    /**
     * 返回的记录条数，并非最终结果的条数（已在处理的记录会从list结果剔除）
     */
    @Getter
    private int count = 0;

    @Getter
    String[] markers;

    @Getter
    String nextMarker = "";

    boolean bucketExists;

    /**
     * 用来判断是否应该马上发起下一轮list。
     */
    @Getter
    public boolean nextList = false;

    @Getter
    @Setter
    public long maxKey = MAX_COUNT;

    public ListSyncRecorderHandler(ClientTemplate.ResponseInfo<Tuple2<String, UnSynchronizedRecord>[]> responseInfo,
                                   List<Tuple3<String, String, String>> nodeList, String[] markers, String bucket, boolean bucketExists) {
        super(responseInfo, nodeList, bucket);
        this.markers = markers;
        this.bucketExists = bucketExists;
    }


    @Override
    protected void publishResult() {
        res.onNext(true);
    }

    @Override
    protected String getKey(Tuple2<String, UnSynchronizedRecord> tuple2) {
        return tuple2.var1;
    }

    @Override
    protected int compareTo(Tuple2<String, UnSynchronizedRecord> t1, Tuple2<String, UnSynchronizedRecord> t2) {
        if (t2.var1.equals(t1.var1)) {
            return t1.var2.getVersionNum().compareTo(t2.var2.getVersionNum());
        } else {
            return t1.var1.compareTo(t2.var1);
        }
    }

    @Override
    protected void handleResult(Tuple2<String, UnSynchronizedRecord> tuple2) {
    }

    @Override
    protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList) {
        return Mono.just(true);
    }

    @Override
    protected void putErrorList(Counter counter) {

    }

    @Override
    protected void publishErrorList() {
    }

    /**
     * 移除父类对错误数量的判断，在失败节点大于k时仍然按成功流程继续
     */
    @Override
    public void handleComplete() {
        try {
            int listNow = listingCount.get();
            if (listNow <= 0) {
                listNow = 1;
            }

            LinkedList<UnSynchronizedRecord> recordList = new LinkedList<>();
            ConcurrentHashMap<String, ConcurrentHashSet<String>> recordTypeSet = new ConcurrentHashMap<>();
            // 一个对象名下只有预提交或者其他站点发来的差异记录，将该对象名保存在此
            ConcurrentHashMap<String, ConcurrentHashSet<String>> recordCommitSet = new ConcurrentHashMap<>();

            count = linkedList.size();
            for (Counter counter : linkedList) {
                //该条记录已在处理中，跳过
                UnSynchronizedRecord record = counter.t.var2;
                String uploadId = getOringinUploadId(record);
                String type = getRecordType(record);
                synchronized (recordSet) {

                    if (StringUtils.isNotEmpty(uploadId)) {
                        record.headers.put(RECORD_ORIGIN_UPLOADID, uploadId);
                        if (recordSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(record.recordKey)) {
                            continue;
                        }
                        if (skipMutual(type, record)) {
                            continue;
                        }
                        if (syncPartLimiter.get() && PART_SYNCING_AMOUNT.get() > DEFAULT_PART_MAX_COUNT) {
                            continue;
                        }

                        // initPart消息单独处理，防止compete消息记录在前面导致同uploadId的initpart消息获取不到
                        if (initUploadRecordSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(uploadId)) {
                            continue;
                        }
                        if (record.type() == UnSynchronizedRecord.Type.ERROR_INIT_PART_UPLOAD) {
                            if (mergeUploadRecordSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(uploadId)
                                    || uploadIdRecordSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(uploadId)) {
                                continue;
                            }
                            initUploadRecordSet.get(record.bucket).add(uploadId);
                        } else if (record.type() == UnSynchronizedRecord.Type.ERROR_PART_UPLOAD) {
                            if (mergeUploadRecordSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(uploadId)) {
                                continue;
                            }
                            if ("appendObject".equals(uploadId)) {
                                if (uploadIdRecordSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(uploadId)) {
                                    continue;
                                }
                            }
                            uploadIdRecordSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).add(uploadId);
                            uploadCountSet.computeIfAbsent(record.bucket + uploadId, v -> new AtomicLong()).incrementAndGet();
                        } else if (record.type() == UnSynchronizedRecord.Type.ERROR_COMPLETE_PART) {
                            if (mergeUploadRecordSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(uploadId)
                                    || uploadIdRecordSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(uploadId)) {
                                continue;
                            }
                            mergeUploadRecordSet.get(record.bucket).add(uploadId);
                        }
                        PART_SYNCING_AMOUNT.incrementAndGet();
                        recordSet.get(record.bucket).add(record.rocksKey());
                        String checkRecordKey = record.index + File.separator + record.bucket + File.separator + record.object + File.separator + type + File.separator + record.versionId;
                        checkRecordMap.computeIfAbsent(checkRecordKey, v -> new ConcurrentHashMap<>()).put(record.recordKey, new Tuple2<>(System.currentTimeMillis(), record));
                    } else {
                        if (recordSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(record.recordKey)) {
                            continue;
                        }
                        if (skipMutual(type, record)) {
                            continue;
                        }
                        String checkRecordKey = record.index + File.separator + record.bucket + File.separator + record.object + File.separator + type + File.separator + record.versionId;
                        String checkRecordKey2 = record.object + File.separator + type + File.separator + record.versionId;
                        boolean skip = false;
                        if (recordTypeSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(checkRecordKey2)) {
                            if (recordCommitSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).contains(checkRecordKey2)) {
                                if (checkRecordStatus(record)) {
                                    continue;
                                }
                                // 本次扫描list到过同名对象record0，且record0是预提交记录或者是其他站点转发来的，
                                if (checkRecordMap.containsKey(checkRecordKey)) {
                                    boolean hasFirmRecord = false;
                                    for (Tuple2<Long, UnSynchronizedRecord> value : checkRecordMap.get(checkRecordKey).values()) {
                                        UnSynchronizedRecord oldRecord = value.var2;
                                        if (oldRecord.syncStamp.compareTo(record.syncStamp) < 0) {
                                            if (recordList.remove(oldRecord)) {
                                                oldRecord.headers.put("del_same", "1");
                                                recordList.add(oldRecord);
                                                hasFirmRecord = true;
                                            }
                                        } else {
                                            if (recordList.remove(oldRecord)) {
                                                // 本轮扫描存在syncStamp大的oldRecord，因为是预提交记录，所以本轮暂不处理。
                                                recordSet.get(oldRecord.bucket).add(oldRecord.recordKey);
                                                checkRecordMap.get(checkRecordKey).remove(oldRecord.recordKey);
                                                hasFirmRecord = true;
                                            }
                                        }
                                    }
                                    if (hasFirmRecord) {
                                        recordCommitSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).remove(checkRecordKey2);
                                    }
                                }
                            } else {
                                // 本次扫描list到过同名对象record0，且record0不是预提交记录也不是其他站点转发来的
                                if (checkRecordMap.containsKey(checkRecordKey)) {
                                    for (Tuple2<Long, UnSynchronizedRecord> value : checkRecordMap.get(checkRecordKey).values()) {
                                        UnSynchronizedRecord oldRecord = value.var2;
                                        // 对同名对象的相同类型差异记录进行判断，删除syncStamp小的差异
                                        if (oldRecord.syncStamp.compareTo(record.syncStamp) < 0) {
                                            if (checkRecordStatus(record)) {
                                                skip = true;
                                                break;
                                            }
                                            if (recordList.remove(oldRecord)) {
                                                // 可能是以前的扫描没处理完，本轮recordList并不会有该oldRecord
                                                oldRecord.headers.put("del_same", "1");
                                                recordList.add(oldRecord);
                                            }
                                        } else {
                                            record.headers.put("del_same", "1");
                                        }
                                    }
                                }
                            }
                        } else {
                            if (checkRecordStatus(record)) {
                                recordCommitSet.computeIfAbsent(record.bucket, v -> new ConcurrentHashSet<>()).add(checkRecordKey2);
                            }
                        }

                        if (skip) {
                            continue;
                        }

                        recordSet.get(record.bucket).add(record.recordKey);
                        recordTypeSet.get(record.bucket).add(checkRecordKey2);
                        checkRecordMap.computeIfAbsent(checkRecordKey, v -> new ConcurrentHashMap<>()).put(record.recordKey, new Tuple2<>(System.currentTimeMillis(), record));
                    }

                    TOTAL_SYNCING_AMOUNT.incrementAndGet();
                    long dataSize = Long.parseLong(record.headers.getOrDefault(CONTENT_LENGTH, "0")) / 1024;
                    TOTAL_SYNCING_SIZE.addAndGet(dataSize);
                }
                recordList.add(record);

                boolean breakLoop = false;
                long dataSize = Long.parseLong(record.headers.getOrDefault(CONTENT_LENGTH, "0")) / 1024;
                // 异构复制和moss的异步复制做区分限流，因为硬件的性能不同
                if (hasExtraLimiter(record.index, DATASYNC_THROUGHPUT_QUOTA)) {
                    SyncRecordLimiter extraCountLimiter = getExtraLimiter(record.index, DATASYNC_THROUGHPUT_QUOTA);
                    extraCountLimiter.acquireToken(1L);
                    if (extraCountLimiter.reachLimit()) {
                        log.debug("reach extraCountLimiter. {}", record.index);
                        break;
                    }
                }

                if (hasExtraLimiter(record.index, DATASYNC_BAND_WIDTH_QUOTA)) {
                    SyncRecordLimiter extraSizeLimiter = getExtraLimiter(record.index, DATASYNC_BAND_WIDTH_QUOTA);
                    extraSizeLimiter.acquireToken(dataSize);
                    if (extraSizeLimiter.reachLimit()) {
                        log.debug("reach extraSizeLimiter. {}", record.index);
                        break;
                    }
                }

                SyncRecordLimiter bucketTpLimiter = getBucketLimiter(record.index, record.bucket, DATASYNC_THROUGHPUT_QUOTA);
                SyncRecordLimiter bucketBwLimiter = getBucketLimiter(record.index, record.bucket, DATASYNC_BAND_WIDTH_QUOTA);

                if (bucketTpLimiter != null) {
                    log.debug("sync scan limit--------count {} {} {}", bucketTpLimiter.getName(), bucketTpLimiter.getlimit(), bucketTpLimiter.getTokensAmount());
                }
                if (bucketBwLimiter != null) {
                    log.debug("sync scan limit--------size {} {} {}", bucketBwLimiter.getName(), bucketBwLimiter.getlimit(), bucketBwLimiter.getTokensAmount());
                }

                // 优先判断桶是否有异步复制限制。
                // 如果设置桶带宽和sizeLimiter较为接近，可能会在上下两轮分别被限制一次，导致实际带宽偏低
                if (hasSyncLimitation(bucketTpLimiter)) {
                    bucketTpLimiter.acquireToken(1L);
                    if (bucketTpLimiter.reachLimit()) {
                        log.debug("reach bucketTpLimiter. {}, {}", record.bucket, bucketTpLimiter.getlimit());
                        breakLoop = true;
                    }
                }
                if (hasSyncLimitation(bucketBwLimiter) && bucketExists) {
                    bucketBwLimiter.acquireToken(dataSize);
                    if (bucketBwLimiter.reachLimit()) {
                        log.debug("reach bucketBwLimiter. {}, {}", record.bucket, bucketBwLimiter.getlimit());
                        breakLoop = true;
                    }
                }

                if (hasSyncLimitation(countLimiter)) {
                    countLimiter.acquireToken(1L);
                    if (countLimiter.reachLimit()) {
                        log.debug("reach countLimiter. {}, {}", record.bucket, countLimiter.getlimit());
                        breakLoop = true;
                    }
                }
                // 不存在的桶不用考虑有实际的数据需要同步
                if (hasSyncLimitation(sizeLimiter) && bucketExists) {
                    sizeLimiter.acquireToken(dataSize);
                    if (sizeLimiter.reachLimit()) {
                        log.debug("reach sizeLimiter. {}, {}", record.bucket, sizeLimiter.getlimit());
                        breakLoop = true;
                    }
                }

                //超出负载，丢弃剩余部分。
                if (TOTAL_SYNCING_AMOUNT.get() > DEFAULT_MAX_COUNT
                        || TOTAL_SYNCING_SIZE.get() > DEFAULT_MAX_SIZE) {
                    breakLoop = true;
                }

                if (breakLoop) {
                    break;
                }
            }

            if (count >= maxKey) {
                // list还未结束
                nextList = true;
            } else {
                // list结束，但是部分差异记录没有在本轮处理。依然需要继续list
                if (recordList.size() < count) {
                    nextList = true;
                }
            }

            if (recordList.peekLast() != null) {
                nextMarker = recordList.peekLast().rocksKey();
            }

            recordProcessor.onNext(recordList);
            publishResult();
            linkedList.clear();
        } catch (Exception e) {
            log.error("list sync recorder error, ", e);
        }
    }

    private static boolean checkRecordStatus(UnSynchronizedRecord record) {
        if (record.commited) {
            return record.headers.containsKey("Sync-Notify");
        }
        return true;
    }

    public static String getRecordType(UnSynchronizedRecord record) {
        switch (record.type()) {
            case ERROR_PUT_OBJECT:
            case ERROR_PUT_OBJECT_VERSION:
            case ERROR_SYNC_HIS_OBJECT: {
                return "PUT_OBJECT";
            }
            case ERROR_DELETE_OBJECT: {
                return "DELETE_OBJECT";
            }
            default:
                return record.type().toString();
        }
    }

    @Override
    public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, Tuple2<String, UnSynchronizedRecord>[]> tuple) {
        int index = tuple.var1;
        Tuple2<String, UnSynchronizedRecord>[] keyRecord = tuple.var3;
        if (null != keyRecord && keyRecord.length > 0) {
            markers[index] = keyRecord[keyRecord.length - 1].var1;
        }

        super.handleResponse(tuple);
    }

    public static Set<String> mutualTypeSetA = new HashSet<>();
    public static Set<String> mutualTypeSetB = new HashSet<>();

    static {
        mutualTypeSetA.add("DELETE_OBJECT");
        mutualTypeSetB.add("PUT_OBJECT");
        mutualTypeSetB.add("COPY_OBJECT");
        // 回收站的同步相当于put
        mutualTypeSetB.add("TRASH_DELETE");

        mutualTypeSetA.add(ERROR_PART_ABORT.name());
        mutualTypeSetB.add(ERROR_INIT_PART_UPLOAD.name());
        mutualTypeSetB.add(ERROR_PART_UPLOAD.name());
        mutualTypeSetB.add(ERROR_COMPLETE_PART.name());
    }

    // 如果有符合条件的差异记录还未处理完，跳过本条差异记录
    // 有同名对象的数据在，则删除操作不进行
    boolean skipMutual(String type, UnSynchronizedRecord record) {
        if (mutualTypeSetA.contains(type)) {
            for (String type1 : mutualTypeSetB) {
                String key = record.index + File.separator + record.bucket + File.separator + record.object + File.separator + type1 + File.separator + record.versionId;
                if (checkRecordMap.containsKey(key)) {
                    return true;
                }
            }
        }
        return false;
    }
}
