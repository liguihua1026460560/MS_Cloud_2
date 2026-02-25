package com.macrosan.component.param;

import com.macrosan.component.utils.ParamsUtils;
import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;

import java.util.Base64;

/**
 * @author zhaoyang
 * @date 2024/01/05
 * @description 文字水印参数
 **/
public class ImageWaterMarkTextParams implements ProcessParams {

    public String word;
    public String size;
    public String color;
    public String location;
    public String fill;
    public String t;
    public String rotate;
    public String padx;
    public String pady;

    @Override
    public void checkParams() {
        // 水印文字不能为空
        if (StringUtils.isBlank(word)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "watermark text word cannot be empty");
        }else {
            try {
                Base64.getUrlDecoder().decode(word);
            } catch (Exception e) {
                throw new MsException(ErrorNo.NOT_BASE64_ENCODING, "watermark text process: word is not base64 encoding");
            }
        }
        // 文字大小 正整数 不能超过1000
        if (StringUtils.isNotBlank(size) && (!ParamsUtils.isPositiveInteger(size) || Integer.parseInt(size) > 1000)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "font size is invalid");
        }
        // 文字颜色
        if (StringUtils.isNotBlank(color) && !color.matches("^#[A-Fa-f0-9]{6}$")) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "color is invalid");
        }
        // 文字位置 取值1-9
        if (StringUtils.isNotBlank(location) && (!ParamsUtils.isPositiveInteger(location) || Integer.parseInt(location) > 9)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "location is invalid");
        }
        // 文字是否填充 取值1或0
        if (StringUtils.isNotBlank(fill) && !("1".equals(fill) || "0".equals(fill))) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "fill is invalid");
        }
        // 文字透明度 正整数 取值0-100
        if (StringUtils.isNotBlank(t) && (!ParamsUtils.isPositiveIntegerOrZero(t) || Integer.parseInt(t) > 100)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "t is invalid");
        }
        // 文字旋转角度 正整数 取值0-360
        if (StringUtils.isNotBlank(rotate) && (!ParamsUtils.isPositiveIntegerOrZero(rotate) || Integer.parseInt(rotate) > 360)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "rotate is invalid");
        }
        // 文字水平间距 正整数 取值0-4096
        if (StringUtils.isNotBlank(padx) && (!ParamsUtils.isPositiveIntegerOrZero(padx) || Integer.parseInt(padx) > 4096)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "padx is invalid");
        }
        // 文字垂直间距 正整数 取值0-4096
        if (StringUtils.isNotBlank(pady) && (!ParamsUtils.isPositiveIntegerOrZero(pady) || Integer.parseInt(pady) > 4096)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "pady is invalid");
        }
    }
}
