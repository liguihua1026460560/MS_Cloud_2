package com.macrosan.message.jsonmsg;

import com.alibaba.fastjson.annotation.JSONField;
import com.dslplatform.json.CompiledJson;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import lombok.experimental.Accessors;


@Data
@CompiledJson
@Accessors(chain = true)
public class Address {

  @JSONField(name = "SourceIp")
  @JsonDeserialize(using = CustomJsonDateDeserializer.class)
  private Object sourceIp;

  @JSONField(name = "moss:SourceIp")
  @JsonDeserialize(using = CustomJsonDateDeserializer.class)
  private Object mossSourceIp;

}
