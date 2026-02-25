package com.macrosan.message.jsonmsg;

import com.alibaba.fastjson.annotation.JSONField;
import com.dslplatform.json.CompiledJson;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@CompiledJson
@Accessors(chain = true)
public class PolicyDate {

    @JSONField(name = "CurrentTime")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String currentTime;

    @JSONField(name = "moss:CurrentTime")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossCurrentTime;

    @JSONField(name = "object-lock-retain-until-date")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String retentionDate;

    @JSONField(name = "moss:object-lock-retain-until-date")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossRetentionDate;

}
