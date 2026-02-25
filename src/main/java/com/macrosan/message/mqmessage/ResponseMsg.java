package com.macrosan.message.mqmessage;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.macrosan.utils.serialize.JaxbUtils;
import lombok.Data;
import lombok.experimental.Accessors;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.DEFAULT_DATA;

/**
 * ResponseMsg
 * 用于在事件总线上通信的数据类
 *
 * @author liyixin
 * @date 2018/10/31
 */
@CompiledJson(formats = CompiledJson.Format.ARRAY, minified = true)
@Data
@Accessors(chain = true)
public class ResponseMsg {

    /**
     * 表示Http状态码
     */
    @JsonAttribute(nullable = false, index = 1)
    public int httpCode = 200;

    /**
     * 保存返回的数据
     */
    @JsonAttribute(nullable = false, index = 2)
    public byte[] data = DEFAULT_DATA;

    @JsonAttribute(index = 3)
    public UnifiedMap<String, String> headers = new UnifiedMap<>();

    /**
     * 默认设置为未知错误
     */
    @JsonAttribute(ignore = true)
    public int errCode = UNKNOWN_ERROR;

    public ResponseMsg() {
    }

    public ResponseMsg(int errCode, byte[] data) {
        this.errCode = errCode;
        this.data = data;
    }

    public ResponseMsg(int errCode, Object msg) {
        this.errCode = errCode;
        this.data = JaxbUtils.toByteArray(msg);
    }

    public ResponseMsg(int errCode) {
        this.errCode = errCode;
    }

    public ResponseMsg setData(Object msg) {
        this.data = JaxbUtils.toByteArray(msg);
        return this;
    }

    public ResponseMsg setData(String msg) {
        this.data = msg.getBytes();
        return this;
    }

    public ResponseMsg setData(byte[] data) {
        this.data = data;
        return this;
    }

    @JsonAttribute(ignore = true)
    public ResponseMsg addHeader(String key, String value) {
        headers.put(key, value);
        return this;
    }
}
