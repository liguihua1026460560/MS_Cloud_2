package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * @author zhaoyang
 * @date 2023/09/20
 **/
@Data
public class VideoSnapshotsParams implements ProcessParams {
    /**
     * 视频截帧的起始时间，单位为毫秒 默认值为0
     */
    public String ss;
    /**
     * 图片输出格式，取值：jpg、png
     */
    public String f;
    /**
     * 截帧数量,默认为不限制数量（截帧到视频结束）
     */
    public String num;
    /**
     * 截帧间隔，单位为毫秒，默认截取所有视频帧
     */
    public String interval;
    public String w;
    public String h;
    public String p;

    public static final String JPG = "jpg";
    public static final String PNG = "png";

    @Override
    public void checkParams() {
        if (StringUtils.isNotBlank(ss)) {
            // 正整数 0-100000000
            if (!ss.matches("^(100000000|0|[1-9]\\d{0,7})$")) {
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
            }
        }
        if (StringUtils.isEmpty(f)) {
            //格式不能为空
            throw new MsException(ErrorNo.IMAGE_FORMAT_NOT_EMPTY, "image format not empty");
        } else {
            if (!JPG.equals(f) && !PNG.equals(f)) {
                //格式不支持
                throw new MsException(ErrorNo.IMAGE_FORMAT_NOT_SUPPORTED, "image format not supported");
            }
        }
        if (StringUtils.isNotBlank(num)) {
            // 正整数 0-1000000
            if (!num.matches("^(1000000|0|[1-9]\\d{0,5})$")) {
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
            }
        }
        // 间隔不能为空
        if (StringUtils.isBlank(interval)) {
            throw new MsException(ErrorNo.INTERVAL_NOT_EMPTY, "Interval cannot be empty");
        } else {
            // 正整数 0-1000000
            if (!interval.matches("^(1000000|0|[1-9]\\d{0,5})$")) {
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
            }
            // 间隔不能小于200ms
            if (Integer.parseInt(interval) < 200) {
                throw new MsException(ErrorNo.INVALID_INTERVAL_PARAM, "Interval cannot be less than 200 ms");
            }
        }
    }
}
