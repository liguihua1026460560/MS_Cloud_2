package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;

import static com.macrosan.component.utils.ParamsUtils.isPositiveIntegerOrZero;

/**
 * @author zhaoyang
 * @date 2024/01/05
 * @description 自定义裁剪参数
 **/
public class ImageCropParams implements ProcessParams {
    public String x;
    public String y;
    public String w;
    public String h;

    private final int MAXIMUM_SIDE_LENGTH = 16384;

    @Override
    public void checkParams() {
        if (StringUtils.isNotBlank(x) && (!isPositiveIntegerOrZero(x) || Integer.parseInt(x) > MAXIMUM_SIDE_LENGTH)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "x is invalid");
        }
        if (StringUtils.isNotBlank(y) && (!isPositiveIntegerOrZero(y) || Integer.parseInt(y) > MAXIMUM_SIDE_LENGTH)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "y is invalid");
        }
        if (StringUtils.isNotBlank(w) && (!isPositiveIntegerOrZero(w) || Integer.parseInt(w) > MAXIMUM_SIDE_LENGTH)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "w is invalid");
        }
        if (StringUtils.isNotBlank(h) && (!isPositiveIntegerOrZero(h) || Integer.parseInt(h) > MAXIMUM_SIDE_LENGTH)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "h is invalid");
        }
    }
}
