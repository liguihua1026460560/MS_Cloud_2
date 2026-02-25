package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;

import static com.macrosan.component.ComponentStarter.SUPPORT_IMAGE_FORMAT;

/**
 * @author zhaoyang
 * @date 2024/01/05
 * @description 图片格式转换参数
 **/
public class ImageFormatParams implements ProcessParams {

    /**
     * 格式转换的格式
     */
    public String f;

    @Override
    public void checkParams() {
        if (StringUtils.isBlank(f)){
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "Format conversion format is cannot be empty");
        }
        if(!SUPPORT_IMAGE_FORMAT.contains(f.toLowerCase())){
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "Format conversion does not support the image format");
        }

    }
}
