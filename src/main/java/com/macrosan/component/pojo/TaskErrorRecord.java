package com.macrosan.component.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import com.macrosan.component.ErrorRecord;
import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;

import static com.macrosan.constants.ServerConstants.TASK_ERROR_RECORD_PREFIX;

/**
 * @author zhaoyang
 * @date 2023/12/14
 * @description 任务级别失败记录
 **/
@Data
public class TaskErrorRecord implements ErrorRecord {
    private String object;
    private String bucket;
    private String versionId;
    @JSONField(serialize = false)
    private String taskName;
    private String errorMessage;
    private String errorTime;
    private String errorMark = RandomStringUtils.randomAlphanumeric(10);

    @Override
    @JSONField(serialize = false)
    public String getRedisKey() {
        return TASK_ERROR_RECORD_PREFIX + this.getTaskName();
    }

    @Override
    @JSONField(serialize = false)
    public String getRedisFieldKey() {
        return errorMark;
    }

}