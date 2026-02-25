package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;

/**
 * @author zhaoyang
 * @date 2026/01/21
 **/
public class ImageCompressParams implements ProcessParams {
    String c;

    @Override
    public void checkParams() {
        if (c == null || (!c.equalsIgnoreCase("true") && !c.equalsIgnoreCase("false"))) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "c is invalid.");
        }
    }
}
