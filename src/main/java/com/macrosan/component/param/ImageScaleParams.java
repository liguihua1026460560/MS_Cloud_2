package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;

import static com.macrosan.component.utils.ParamsUtils.isPositiveInteger;

/**
 * @author zhaoyang
 * @date 2024/01/05
 * @description: 图片缩放参数
 **/
public class ImageScaleParams implements ProcessParams {
    public String scale;
    public String h;
    public String w;

    @Override
    public void checkParams() {
        if (StringUtils.isAllBlank(scale, h, w)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "scale,h,w can't be all empty");
        }
        // 缩放比例scale的值是否为正小数 0.01-100
        if (StringUtils.isNotBlank(scale) && (!scale.matches("^[1-9]\\d*|[0-9]+(\\.[0-9]{1,2})?$")||Float.parseFloat(scale) <0.01||Float.parseFloat(scale) > 100)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "scale is invalid");
        }
        // h 正整数
        if (StringUtils.isNotBlank(h) && (!isPositiveInteger(h) || Integer.parseInt(h) > 16384)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "h is invalid");
        }
        // w 正整数
        if (StringUtils.isNotBlank(w) && (!isPositiveInteger(w) || Integer.parseInt(w) > 16384)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "w is invalid");
        }

    }
}
