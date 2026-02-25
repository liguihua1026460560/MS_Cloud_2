package com.macrosan.message.jsonmsg;

import com.alibaba.fastjson.annotation.JSONField;
import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

@Data
@CompiledJson
@Accessors(chain = true)
public class Principal {

  @JSONField(name = "MOSS", alternateNames = {"AWS"})
  public List<String> service;
}
