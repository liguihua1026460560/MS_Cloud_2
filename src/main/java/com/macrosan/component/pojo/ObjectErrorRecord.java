package com.macrosan.component.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import com.macrosan.component.ErrorRecord;
import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;

import static com.macrosan.constants.ServerConstants.OBJECT_ERROR_RECORD_PREFIX;

/**
 * @author zhaoyang
 * @date 2023/12/14
 * @description 对象级别失败记录
 **/
@Data
public class ObjectErrorRecord implements ErrorRecord {
    private String object;
    private String bucket;
    private String versionId;
    private String errorMessage;
    private String errorTime;
    private String processType;
    private String errorMark = RandomStringUtils.randomAlphanumeric(10);


    @Override
    @JSONField(serialize = false)
    public String getRedisKey() {
        return OBJECT_ERROR_RECORD_PREFIX + bucket;
    }

    @Override
    @JSONField(serialize = false)
    public String getRedisFieldKey() {
        return errorMark;
    }

}
