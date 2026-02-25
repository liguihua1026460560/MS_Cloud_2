package com.macrosan.message.socketmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.macrosan.message.jsonmsg.ListPartsResultVo;
import lombok.Getter;
import lombok.Setter;

/**
 * describe:
 *
 * @author chengyinfeng
 * @date 2019/01/14
 */
@CompiledJson
public class ListPartResMsg extends BaseResMsg {

    @JsonAttribute(name = "res_data")
    @Getter
    @Setter
    public ListPartsResultVo data;
}
