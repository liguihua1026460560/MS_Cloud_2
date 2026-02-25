package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;

import static com.macrosan.component.utils.ParamsUtils.isPositiveInteger;

/**
 * @author zhaoyang
 * @date 2024/01/05
 * @description: 内切圆参数
 **/
public class ImageCircleParams implements ProcessParams {

    public String r;

    @Override
    public void checkParams() {
        if (StringUtils.isBlank(r)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "inscribed circle process: r is invalid.");
        }
        // r为正整数 1-4096
        if (!isPositiveInteger(r) || Integer.parseInt(r) > 4096) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "inscribed circle process: r is invalid.");
        }
    }
}
