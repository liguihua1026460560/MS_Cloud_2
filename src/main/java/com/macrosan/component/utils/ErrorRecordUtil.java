package com.macrosan.component.utils;

import com.alibaba.fastjson.JSON;
import com.macrosan.component.ErrorRecord;
import com.macrosan.component.enums.ErrorEnum;
import com.macrosan.component.pojo.*;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.message.jsonmsg.MetaData;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import static com.macrosan.component.ComponentStarter.COMP_SCHEDULER;
import static com.macrosan.constants.ServerConstants.COMPONENT_RECORD_INNER_MARKER;
import static com.macrosan.constants.SysConstants.REDIS_COMPONENT_ERROR_INDEX;

/**
 * @author zhaoyang
 * @date 2023/12/01
 **/
@Log4j2
public class ErrorRecordUtil {


    private static final IamRedisConnPool IAM_POOL = IamRedisConnPool.getInstance();

    // 单个任务失败记录最大数量
    public final static int ERROR_RECORD_MAX_SIZE = 1000;


    /**
     * 根据record构建失败记录，并添加到redis中
     *
     * @param errorEnum
     * @param record
     * @return
     */
    public static Mono<Boolean> putErrorRecord(ErrorEnum errorEnum, ComponentRecord record) {
        if (COMPONENT_RECORD_INNER_MARKER.equals(record.taskMarker)) {
            ObjectErrorRecord objectErrorRecord = new ObjectErrorRecord();
            objectErrorRecord.setObject(record.getObject());
            objectErrorRecord.setBucket(record.getBucket());
            objectErrorRecord.setVersionId(record.getVersionId());
            objectErrorRecord.setErrorMessage(errorEnum.getErrorMessage());
            objectErrorRecord.setProcessType(errorEnum.getProcessType());
            objectErrorRecord.setErrorTime(String.valueOf(System.currentTimeMillis()));
            return putErrorRecord(objectErrorRecord);
        } else {
            TaskErrorRecord taskErrorRecord = new TaskErrorRecord();
            taskErrorRecord.setObject(record.getObject());
            taskErrorRecord.setBucket(record.getBucket());
            taskErrorRecord.setVersionId(record.getVersionId());
            taskErrorRecord.setTaskName(record.taskName);
            taskErrorRecord.setErrorMessage(errorEnum.getErrorMessage());
            taskErrorRecord.setErrorTime(String.valueOf(System.currentTimeMillis()));
            return putErrorRecord(taskErrorRecord);
        }
    }


    // 添加失败记录时需要进行数量限制 ，使用synchronized
    private static final Object PUT_ERROR_RECORD_LOCK = new Object();

    /**
     * 将错误记录添加到redis中
     *
     * @param errorRecord 错误记录
     * @param <T>         错误记录类型
     * @return 添加结果
     */
    public static <T extends ErrorRecord> Mono<Boolean> putErrorRecord(T errorRecord) {
        // 将错误记录 添加到redis中
        String redisKey = errorRecord.getRedisKey();
        return Mono.just(1)
                .publishOn(COMP_SCHEDULER)
                .map(b -> {
                    synchronized (PUT_ERROR_RECORD_LOCK) {
                        Long hlen = IAM_POOL.getCommand(REDIS_COMPONENT_ERROR_INDEX).hlen(redisKey);
                        if (hlen >= ERROR_RECORD_MAX_SIZE) {
                            return true;
                        }
                        return IAM_POOL.getShortMasterCommand(REDIS_COMPONENT_ERROR_INDEX).hset(redisKey, errorRecord.getRedisFieldKey(), JSON.toJSONString(errorRecord));
                    }
                }).flatMap(ret -> {
                    log.debug("putErrorRecord key:{},value:{},ret:{}", redisKey, errorRecord, ret);
                    return Mono.just(ret);
                });
    }

}
