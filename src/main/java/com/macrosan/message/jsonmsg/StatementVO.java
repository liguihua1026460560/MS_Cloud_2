package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * <p>
 * 策略Statement实体
 * </p>
 *
 * @author LiuChuang
 * @Copyright MacroSAN Technologies Co., Ltd. All rights reserved.
 * @date 2018年12月6日 上午11:43:45
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class StatementVO {
    @JsonAttribute
    public List<String> action;
    @JsonAttribute
    public String effect;
    @JsonAttribute
    public List<String> resource;

}
