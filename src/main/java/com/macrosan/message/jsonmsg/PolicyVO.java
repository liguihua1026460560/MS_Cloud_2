package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * 以policyId为key的value值模板
 *
 * @author LiuChuang
 * @Copyright MacroSAN Technologies Co., Ltd. All rights reserved.
 * @date 2018年12月6日 上午11:04:37
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class PolicyVO {
    @JsonAttribute
    public String version;
    @JsonAttribute
    public List<StatementVO> statement;

}
