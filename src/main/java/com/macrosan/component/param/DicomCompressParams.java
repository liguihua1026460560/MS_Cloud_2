package com.macrosan.component.param;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;

/**
 * @author zhaoyang
 * @date 2026/04/21
 **/
public class DicomCompressParams implements ProcessParams {
    public String c;

    @Override
    public void checkParams() {
        if (!"true".equals(c)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "c is must be true.");
        }
    }
}
