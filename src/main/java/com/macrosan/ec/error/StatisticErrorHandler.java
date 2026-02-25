/**
 * Copyright (C), 2019-2020,杭州宏杉科技股份有限公司
 * FileName: StatisticErrorHandler
 * Author: xiangzicheng-PC
 * Date: 2020/7/9 10:10
 * Description: 流量统计相关的错误处理方法
 * History:
 */

package com.macrosan.ec.error;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.ECUtils;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.quota.StatisticsRecorder;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_MERGE_RECORD;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_MINUTE_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.MERGE_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.PUT_MINUTE_RECORD;
import static com.macrosan.utils.quota.StatisticsRecorder.STATISTIC_STORAGE_POOL;

/**
 * 流量统计相关的错误处理方法
 *
 * @author xiangzicheng-PC
 * @create 2020/7/9
 * @since 1.0.0
 */
@Log4j2
public class StatisticErrorHandler {
    @HandleErrorFunction(ERROR_PUT_MINUTE_RECORD)
    public static Mono<Boolean> putMinuteRecord(String value) {
        final StatisticsRecorder.StatisticRecord record = Json.decodeValue(value, StatisticsRecorder.StatisticRecord.class);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(record.getRecordTime());
        final int day = calendar.get(Calendar.DAY_OF_YEAR);
        calendar.setTime(new Date());
        final int nowDay = calendar.get(Calendar.DAY_OF_YEAR);
        //如果这条记录距离当前时间已经超过了2天，那这条分钟记录应该已经被整合了，无需恢复
        if (nowDay - day >= 2) {
            return Mono.just(true);
        }
        final String accountId = record.getAccountId();
        final StoragePool storagePool;
        if (STATISTIC_STORAGE_POOL == null) {
            storagePool = StoragePoolFactory.getStoragePool(StorageOperate.META);
        } else {
            storagePool = STATISTIC_STORAGE_POOL;
        }
        final String accountVnode = storagePool.getBucketVnodeId(accountId);
        record.setVnode(accountVnode);
        return storagePool.mapToNodeInfo(accountVnode)
                .flatMap(nodeList -> ECUtils.putRocksKey(storagePool, record.getKey(), Json.encode(record),
                        PUT_MINUTE_RECORD, ERROR_PUT_MINUTE_RECORD, nodeList, null)
                );
    }

    @HandleErrorFunction(ERROR_MERGE_RECORD)
    public static Mono<Boolean> putMergeRecord(String value, SocketReqMsg msg) {
        final Map<String, StatisticsRecorder.StatisticRecord> recordHashMap =
                Json.decodeValue(value, new TypeReference<Map<String, StatisticsRecorder.StatisticRecord>>() {
                });
        final String accountId = recordHashMap.values().iterator().next().getAccountId();
        final StoragePool storagePool;
        if (STATISTIC_STORAGE_POOL == null) {
            storagePool = StoragePoolFactory.getStoragePool(StorageOperate.META);
        } else {
            storagePool = STATISTIC_STORAGE_POOL;
        }
        final String accountVnode = storagePool.getBucketVnodeId(accountId);
        String key = msg.get("key");
        if (StringUtils.isBlank(key)) {
            return Mono.just(true);
        }

        return storagePool.mapToNodeInfo(accountVnode)
                .flatMap(nodeList -> ECUtils.putRocksKey(storagePool, key, value, MERGE_RECORD, ERROR_MERGE_RECORD, nodeList, null));

    }
}
