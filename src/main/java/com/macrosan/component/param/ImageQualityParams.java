package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;

import static com.macrosan.component.utils.ParamsUtils.isPositiveInteger;

/**
 * @author zhaoyang
 * @date 2025/06/23
 * @description 图片质量变化参数
 **/
public class ImageQualityParams implements ProcessParams {

    public String q;


    @Override
    public void checkParams() {
        if (StringUtils.isBlank(q)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "Image quality change quality cannot be empty");
        }
        if ((!isPositiveInteger(q) || Integer.parseInt(q) > 100)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "Image quality change q must be numeric");
        }
    }

}
