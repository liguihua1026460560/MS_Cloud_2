package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;

import java.util.Base64;

import static com.macrosan.component.utils.ParamsUtils.isPositiveInteger;
import static com.macrosan.component.utils.ParamsUtils.isPositiveIntegerOrZero;

/**
 * @author zhaoyang
 * @date 2024/01/05
 * @description 图片水印参数
 **/
public class ImageWatermarkImageParams implements ProcessParams {

    public String image;
    public String location;

    public String t;

    public String p;

    public String versionId;

    @Override
    public void checkParams() {
        // 图片路径不能为空
        if (StringUtils.isBlank(this.image)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "watermark image process: image is invalid.");
        } else {
            try {
                byte[] decode = Base64.getUrlDecoder().decode(this.image);
                String imagePath = new String(decode);
                if (imagePath.split("/").length < 2) {
                    throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "watermark image process: image path is invalid.");
                }
            } catch (Exception e) {
                if (e instanceof MsException) {
                    throw (MsException) e;
                }
                throw new MsException(ErrorNo.NOT_BASE64_ENCODING, "watermark image process: image is not base64 encoding");
            }
        }

        // 水印位置 正整数 1-9
        if (StringUtils.isNotBlank(location) && (!isPositiveInteger(location) || Integer.parseInt(location) > 9)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "watermark image process: location is invalid.");
        }
        // t 透明度 正整数 0-100
        if (StringUtils.isNotBlank(t) && (!isPositiveIntegerOrZero(t) || Integer.parseInt(t) > 100)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "watermark image process: t is invalid.");
        }
        // p 水印图片缩放比例 正整数 1-100
        if (StringUtils.isNotBlank(p) && (!isPositiveInteger(p) || Integer.parseInt(p) > 100)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "watermark image process: p is invalid.");
        }
    }
}
