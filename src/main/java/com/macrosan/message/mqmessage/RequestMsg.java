package com.macrosan.message.mqmessage;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

/**
 * RequestMsg
 * 用于调用方法的消息类
 *
 * @author liyixin
 * @date 2018/11/1
 */
@CompiledJson(formats = CompiledJson.Format.ARRAY, minified = true)
@Data
public class RequestMsg {

    @JsonAttribute(nullable = false, index = 1)
    public int signature;

    @JsonAttribute(nullable = false, index = 2)
    public UnifiedMap<String, String> paramMap;

    public static class Builder {

        private Builder() {
        }
    }
}
