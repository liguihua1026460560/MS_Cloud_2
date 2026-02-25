package com.macrosan.message.jsonmsg;

import com.alibaba.fastjson.annotation.JSONField;
import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

@Data
@CompiledJson
@Accessors(chain = true)
public class BucketPolicy {
  @JSONField(name = "Version")
  @JsonAttribute(nullable = false)
  public String version;

  @JSONField(name = "Statement", ordinal = 1)
  @JsonAttribute(nullable = false)
  public List<Statement> statement;

}
