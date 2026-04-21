package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 视频压缩参数
 * 支持两种模式：
 * 1. 比例模式：ratio（0.1 - 1.0）
 * 2. 具体参数模式：crf(18-51)、vb(kbps)、vcodec(h264/h265)、f(mp4/original)、maxrate(kbps)
 *
 * @author zhaoyang
 * @date 2026/03/25
 **/
@Data
public class VideoCompressParams implements ProcessParams {

    /**
     * 压缩比例（0.1 - 1.0）
     */
    String ratio;

    /**
     * CRF质量（18-51），越小质量越好
     */
    String crf;

    /**
     * 视频码率（kbps）
     */
    String vb;

    /**
     * 视频编码器（h264/h265）
     */
    String vcodec;

    /**
     * 输出格式（mp4/original）
     */
    String f;

    /**
     * 最大码率（kbps）
     */
    String maxrate;

    private static final Set<String> SUPPORT_VCODEC = new HashSet<>(Arrays.asList("h264", "h265"));
    private static final Set<String> SUPPORT_FORMAT = new HashSet<>(Arrays.asList("mp4"));

    @Override
    public void checkParams() {
        // ratio、crf、vb 三选一，只能选一个
        boolean hasRatio = StringUtils.isNotBlank(ratio);
        boolean hasCrf = StringUtils.isNotBlank(crf);
        boolean hasVb = StringUtils.isNotBlank(vb);

        int selectedCount = (hasRatio ? 1 : 0) + (hasCrf ? 1 : 0) + (hasVb ? 1 : 0);
        if (selectedCount == 0) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                    "video compress must have one of ratio/crf/vb.");
        }
        if (selectedCount > 1) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                    "video compress can only select one of ratio/crf/vb.");
        }

        // 比例模式校验
        if (hasRatio) {
            try {
                double ratioValue = Double.parseDouble(ratio);
                if (ratioValue < 0.01 || ratioValue > 1.0) {
                    throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                            "ratio must be between 0.01 and 1.0.");
                }
            } catch (NumberFormatException e) {
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                        "ratio must be a valid decimal number.");
            }
        }

        // CRF 参数校验
        if (hasCrf) {
            try {
                int crfValue = Integer.parseInt(crf);
                if (crfValue < 18 || crfValue > 51) {
                    throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                            "crf must be between 18 and 51.");
                }
            } catch (NumberFormatException e) {
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                        "crf must be a valid integer.");
            }
        }

        // VB 参数校验
        if (hasVb) {
            checkRangeInteger(vb, "vb", 10, 100000);
        }

        if (StringUtils.isBlank(vcodec)||!SUPPORT_VCODEC.contains(vcodec.toLowerCase())) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                    "vcodec must be h264 or h265.");
        }

        if (StringUtils.isNotBlank(f)) {
            if (!SUPPORT_FORMAT.contains(f.toLowerCase())) {
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                        "f must be mp4");
            }
        }

        if (StringUtils.isNotBlank(maxrate)) {
            checkRangeInteger(maxrate, "maxrate", 10, 100000);
        }
    }

    /**
     * 检查整数是否在指定范围内
     */
    private void checkRangeInteger(String value, String paramName, int min, int max) {
        try {
            int intValue = Integer.parseInt(value);
            if (intValue < min || intValue > max) {
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                        paramName + " must be between " + min + " and " + max + ".");
            }
        } catch (NumberFormatException e) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                    paramName + " must be a valid integer.");
        }
    }
}
