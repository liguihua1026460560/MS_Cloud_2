package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;

import static com.macrosan.component.utils.ParamsUtils.isPositiveIntegerOrZero;

/**
 * @author zhaoyang
 * @date 2024/01/05
 * @description 图片旋转参数
 **/
public class ImageRotateParams implements ProcessParams {
    public String angle;

    @Override
    public void checkParams() {
        if (StringUtils.isBlank(angle)){
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,"Image rotation angle cannot be empty");
        }
        // 校验角度是否合法 0-360
        if (!isPositiveIntegerOrZero(angle) || Integer.parseInt(angle) > 360){
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,"Image rotation angle must be a positive integer between 0 and 360");
        }

    }
}
