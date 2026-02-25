package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * <p>
 * 以组id为key的value值模板
 * </p>
 *
 * @author LiuChuang
 * @Copyright MacroSAN Technologies Co., Ltd. All rights reserved.
 * @date 2018年11月30日 下午3:05:42
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class GroupVO {
    @JsonAttribute
    public String accountId;
    @JsonAttribute
    public List<String> policyIds;
}
