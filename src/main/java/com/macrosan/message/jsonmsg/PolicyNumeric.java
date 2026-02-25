package com.macrosan.message.jsonmsg;

import com.alibaba.fastjson.annotation.JSONField;
import com.dslplatform.json.CompiledJson;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@CompiledJson
@Accessors(chain = true)
public class PolicyNumeric {

    @JSONField(name = "max-keys")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String maxKeys;

    @JSONField(name = "moss:max-keys")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossMaxKeys;

    @JSONField(name = "object-lock-remaining-retention-days")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String retentionDay;

    @JSONField(name = "moss:object-lock-remaining-retention-days")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossRetentionDay;

}
