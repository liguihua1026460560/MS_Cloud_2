package com.macrosan.message.jsonmsg;

import com.alibaba.fastjson.annotation.JSONField;
import com.dslplatform.json.CompiledJson;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@CompiledJson
@Accessors(chain = true)
public class PolicyBool {

    @JSONField(name = "SecureTransport")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private Boolean secureTransport;

    @JSONField(name = "moss:SecureTransport")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private Boolean mossSecureTransport;

}
