package com.macrosan.message.jsonmsg;

import com.alibaba.fastjson.annotation.JSONField;
import com.dslplatform.json.CompiledJson;

import lombok.Data;
import lombok.experimental.Accessors;


import java.util.List;

@Data
@CompiledJson
@Accessors(chain = true)
public class Statement {

  @JSONField(name = "Sid")
  public String Sid;

  @JSONField(name = "Principal")
  public Principal Principal;

  @JSONField(name = "NotPrincipal")
  public Principal NotPrincipal;

  @JSONField(name = "Action")
  public List<String> Action;

  @JSONField(name = "NotAction")
  public List<String> NotAction;

  @JSONField(name = "Effect")
  public String Effect;

  @JSONField(name = "Resource")
  public List<String> Resource;

  @JSONField(name = "NotResource")
  public List<String> NotResource;

  @JSONField(name = "Condition")
  public  Condition Condition;
}

