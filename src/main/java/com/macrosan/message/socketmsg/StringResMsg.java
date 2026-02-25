package com.macrosan.message.socketmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * MapResMsg
 *
 * @author liyixin
 * @date 2019/1/10
 */
@EqualsAndHashCode(callSuper = true)
@CompiledJson
@Data
public class StringResMsg extends BaseResMsg {

    @JsonAttribute(name = "res_data")
    public String data;
}
