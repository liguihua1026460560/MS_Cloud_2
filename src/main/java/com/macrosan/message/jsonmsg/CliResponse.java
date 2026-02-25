package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;

/**
 * CliResponse
 *
 * @author liyixin
 * @date 2019/10/17
 */
@Data
@CompiledJson
public class CliResponse {

    @JsonAttribute
    public String status;

    @JsonAttribute
    public String[] params = new String[0];
}
