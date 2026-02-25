package com.macrosan.message.socketmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

/**
 * SocketResponseMsg
 *
 * @author shilinyong
 * @date 2019/07/29
 */
@CompiledJson(onUnknown = CompiledJson.Behavior.DEFAULT)
@Data
@Accessors(chain = true)
public class SocketResponseMsg {

    @JsonAttribute(nullable = false)
    public String version = "1.0";

    @JsonAttribute(nullable = false)
    public String msgType;

    @JsonAttribute(nullable = false)
    public long msgLen = 0;

    @JsonAttribute(nullable = false)
    public Map<String, String> dataMap;
    public Map<String, String> res;

    public SocketResponseMsg() {
    }

    public SocketResponseMsg(String msgType, long msgLen) {
        this.msgType = msgType;
        this.msgLen = msgLen;
        dataMap = new HashMap<>(16);
        res = new HashMap<>(16);
    }

    public SocketResponseMsg put(String key, String value) {
        res.put(key, value);
        return this;
    }

//    public String get(String key) {
//        return dataMap.get(key);
//    }

    public String get(String key) {
        return res.get(key);
    }

//    public String getAndRemove(String key) {
//        return dataMap.remove(key);
//    }
}
