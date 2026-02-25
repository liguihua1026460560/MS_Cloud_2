package com.macrosan.message.jsonmsg;

import com.alibaba.fastjson.annotation.JSONField;
import com.dslplatform.json.CompiledJson;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@CompiledJson
@Accessors(chain = true)
public class Condition {
  @JSONField(name = "IpAddress")
  public Address ip;

  @JSONField(name = "NotIpAddress")
  public Address notIp;

  @JSONField(name = "Bool")
  public PolicyBool bool;

  @JSONField(name = "DateEquals")
  public PolicyDate DateEquals;

  @JSONField(name = "DateNotEquals")
  public PolicyDate DateNotEquals;

  @JSONField(name = "DateLessThan")
  public PolicyDate DateLessThan;

  @JSONField(name = "DateLessThanEquals")
  public PolicyDate DateLessThanEquals;

  @JSONField(name = "DateGreaterThan")
  public PolicyDate DateGreaterThan;

  @JSONField(name = "DateGreaterThanEquals")
  public PolicyDate DateGreaterThanEquals;

  @JSONField(name = "NumericEquals")
  public PolicyNumeric NumericEquals;

  @JSONField(name = "NumericNotEquals")
  public PolicyNumeric NumericNotEquals;

  @JSONField(name = "NumericLessThan")
  public PolicyNumeric NumericLessThan;

  @JSONField(name = "NumericLessThanEquals")
  public PolicyNumeric NumericLessThanEquals;

  @JSONField(name = "NumericGreaterThan")
  public PolicyNumeric NumericGreaterThan;

  @JSONField(name = "NumericGreaterThanEquals")
  public PolicyNumeric NumericGreaterThanEquals;

  @JSONField(name = "StringEquals")
  public PolicyString StringEquals;

  @JSONField(name = "StringNotEquals")
  public PolicyString StringNotEquals;

  @JSONField(name = "StringEqualsIgnoreCase")
  public PolicyString StringEqualsIgnoreCase;

  @JSONField(name = "StringNotEqualsIgnoreCase")
  public PolicyString StringNotEqualsIgnoreCase;

  @JSONField(name = "StringLike")
  public PolicyString StringLike;

  @JSONField(name = "StringNotLike")
  public PolicyString StringNotLike;
}
