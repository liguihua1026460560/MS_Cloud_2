package com.macrosan.component.param;

import com.macrosan.component.utils.ParamsUtils;
import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;

/**
 * @author zhaoyang
 * @date 2026/04/15
 **/
public class PngCompressParams implements ProcessParams {
    public String mode;

    @Override
    public void checkParams() {
        if (StringUtils.isBlank(mode)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                    "mode is must be lossy or lossless");
        }
        if (!ParamsUtils.isLossless(mode) && !ParamsUtils.isLossy(mode)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                    "mode is must be lossy or lossless");
        }
        if (ParamsUtils.isLossless(mode) && ParamsUtils.isLossy(mode)){
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM,
                    "mode is must be lossy or lossless");
        }
    }
}
