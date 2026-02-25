package com.macrosan.utils.sts;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @Description: TODOo
 * @Author wanhao
 * @Date 2023/8/1 0001 上午 9:19
 */
@Data
@Accessors(chain = true)
public class PolicyJsonObj {
    private String version;
    private List<Statement> statement;
}
