package com.macrosan.component.pojo;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.macrosan.httpserver.MsHttpRequest;
import io.vertx.core.MultiMap;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * 用户提前设置好的策略。保存在表3，每个账户的每个组件为一个map，strategyName-Json.encode(this)
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class ComponentStrategy {
    @JsonIgnore
    public static final String REDIS_KEY = "component_strategy";

    @JsonAttribute
    public String strategyName;
    @JsonAttribute
    public ComponentRecord.Type type;
    // uri格式为?image-process=scaling,w_100,h_100/quality,q_80，process格式为scaling,w_100,h_100/quality,q_80
    @JsonAttribute
    public String process;

    /**
     * 处理完后将图片发送到目标位置，如果为空则表示覆盖原对象。
     * 格式为destBucket/destPrefix
     */
    @JsonAttribute
    public String destination;

    /**
     * 策略的唯一标识
     */
    @JsonAttribute
    public String strategyMark;

    /**
     * 是否删除源文件
     */
    @JsonAttribute
    public boolean deleteSource;

    /**
     * 是否复制源对象 用户元数据
     */
    @JsonAttribute
    public boolean copyUserMetaData;

    public ComponentStrategy() {
    }

    public ComponentStrategy(Map<String, String> redisMap) {
        this.strategyName = redisMap.get("strategyName");
        this.type = ComponentRecord.Type.parseType(redisMap.get("type"));
        this.process = redisMap.get("process");
        this.destination = redisMap.get("destination");
        this.strategyMark = redisMap.get("strategyMark");
        this.deleteSource = Boolean.parseBoolean(redisMap.get("deleteSource"));
        this.copyUserMetaData = StringUtils.isBlank(redisMap.get("copyUserMetaData")) || Boolean.parseBoolean(redisMap.get("copyUserMetaData"));
    }

    public ComponentStrategy(String strategyName, String type, String process, String destination, boolean deleteSource, boolean copyUserMetaData) {
        this.strategyName = strategyName;
        this.type = ComponentRecord.Type.parseType(type);
        this.process = process;
        this.destination = destination;
        this.deleteSource = deleteSource;
        this.copyUserMetaData = copyUserMetaData;
    }

    public ComponentStrategy initFromRequest(MsHttpRequest request) {
        MultiMap params = request.params();
        boolean match = false;
        if (!params.isEmpty()) {
            for (Map.Entry<String, String> entry : params) {
                match = isMatch(match, entry);
            }
        }
        if (!match) {
            throw new RuntimeException("no such process type, url: " + request.uri());
        }
        return this;
    }

    public boolean isMatch(boolean match, Map.Entry<String, String> entry) {
        for (ComponentRecord.Type type : ComponentRecord.Type.values()) {
            String s = entry.getKey();
            if (s.equals(type.name)) {
                this.setType(type);
                this.setProcess(entry.getValue());
                match = true;
                break;
            }
        }
        return match;
    }

    public static String strategyRedisKey(String strategyName) {
        return REDIS_KEY + "_" + strategyName;
    }


}
