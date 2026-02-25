package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;

/**
 * <p>
 * 以AK为key的value值模板
 * </p>
 *
 * @author LiuChuang
 * @Copyright MacroSAN Technologies Co., Ltd. All rights reserved.
 * @date 2018年12月6日 上午9:57:37
 */
@Data
@CompiledJson
public class AccessKeyVO {
    @JsonAttribute
    public String userId;
    @JsonAttribute
    public String secretKey;
    @JsonAttribute
    public String accountId;
    @JsonAttribute
    public String userName;
    @JsonAttribute
    public String acl;
}
