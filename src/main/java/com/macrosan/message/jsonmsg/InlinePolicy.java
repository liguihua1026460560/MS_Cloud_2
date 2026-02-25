package com.macrosan.message.jsonmsg;/**
 * @author niechengxing
 * @create 2024-09-12 17:25
 */

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 *@program: MS_Cloud
 *@description: 临时凭证的内联策略信息
 *@author: niechengxing
 *@create: 2024-09-12 17:25
 */
@Data
@CompiledJson
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class InlinePolicy {
    @JsonAttribute
    public String policyId;

    @JsonAttribute
    public String policy;
}

