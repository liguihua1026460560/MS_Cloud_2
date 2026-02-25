package com.macrosan.message.socketmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.macrosan.constants.ErrorNo;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * BaseResMsg
 *
 * @author liyixin
 * @date 2019/1/10
 */
@Data
@Accessors(chain = true)
@CompiledJson
public class BaseResMsg {

    /***版本信息**/
    @JsonAttribute
    public String version;

    /***消息类别**/
    @JsonAttribute
    public String msgType;

    /***返回码  默认操作成功**/
    @JsonAttribute(name = "res_code")
    public int code = ErrorNo.SUCCESS_STATUS;
}
