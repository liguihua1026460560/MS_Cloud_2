package com.macrosan.utils.sts;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.macrosan.message.jsonmsg.Principal;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

@Data
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class Statement {
    @JSONField(name = "Sid")
    public String sid;

    @JSONField(name = "Principal")
    public Principal principal;

    @JSONField(name = "NotPrincipal")
    public Principal notPrincipal;

    @JSONField(name = "Action")
    public List<String> action;

    @JSONField(name = "NotAction")
    public List<String> notAction;

    @JSONField(name = "Effect")
    public String effect;

    @JSONField(name = "Resource")
    public List<String> resource;

    @JSONField(name = "NotResource")
    public List<String> notResource;

    @JSONField(name = "Condition")
    public com.macrosan.message.jsonmsg.Condition condition;
}