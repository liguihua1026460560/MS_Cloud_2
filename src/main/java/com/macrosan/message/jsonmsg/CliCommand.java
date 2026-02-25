package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;

/**
 * Command
 *
 * @author liyixin
 * @date 2019/10/17
 */
@Data
@CompiledJson
public class CliCommand {
    @JsonAttribute
    public String command;
    @JsonAttribute
    public String[] params;
}
