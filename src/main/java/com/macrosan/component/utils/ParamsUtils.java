package com.macrosan.component.utils;

import com.macrosan.component.param.*;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhaoyang
 * @date 2023/09/20
 **/

@Log4j2
public class ParamsUtils {

    public static Map<String, Class> imageProcessParamMap;
    public static Map<String, Class> videoProcessparamMap;

    static {
        imageProcessParamMap = new HashMap<>();
        videoProcessparamMap = new HashMap<>();
        videoProcessparamMap.put("snapshots", VideoSnapshotsParams.class);
        imageProcessParamMap.put("watermark-text", ImageWaterMarkTextParams.class);
        imageProcessParamMap.put("scaling", ImageScaleParams.class);
        imageProcessParamMap.put("format", ImageFormatParams.class);
        imageProcessParamMap.put("rotate", ImageRotateParams.class);
        imageProcessParamMap.put("crop", ImageCropParams.class);
        imageProcessParamMap.put("circle", ImageCircleParams.class);
        imageProcessParamMap.put("rounded-corners", ImageRoundedCornersParams.class);
        imageProcessParamMap.put("watermark-image", ImageWatermarkImageParams.class);
        imageProcessParamMap.put("quality", ImageQualityParams.class);
        imageProcessParamMap.put("compress", ImageCompressParams.class);
    }

    /**
     * param确定了处理的方法后，需要执行在Processing类中的具体的处理步骤。
     * value中步骤以逗号分割，具体的值再用_分割
     */
    public static Map<String, String> dealWithTaskValue(String value) {
        Map<String, String> map = new HashMap<>();
        String[] split = value.split(",");
        for (String s : split) {
            String[] s1 = s.split("_", 2);
            map.put(StringUtils.trim(s1[0]), StringUtils.trim(s1[1]));
        }
        return map;
    }

    /**
     * 每一种多媒体处理方法包含很多参数，
     *
     * @param process 由uri中的参数处理得来，本次multiMediaTask的处理的具体值，例如处理缩放时taskValue为w_100,h_100
     * @param <T>
     * @return
     */
    public static <T extends ProcessParams> T getParams(String processType, String process) {
        T t = null;
        try {
            String[] s = process.split(",", 2);
            String processKey = StringUtils.trim(s[0]);
            String processValue = StringUtils.trim(s[1]);
            Class<T> paramsClass = getClassByProcessType(processType, processKey);
            if (paramsClass == null) {
                throw new MsException(ErrorNo.NO_SUCH_PROCESS, "no such process");
            }
            // 初始化实例
            Constructor<T> constructor = paramsClass.getDeclaredConstructor();
            t = constructor.newInstance();
            Map<String, String> taskValueMap = dealWithTaskValue(processValue);

            for (Field field : paramsClass.getDeclaredFields()) {
                String operateKey = field.getName();
                if (taskValueMap.containsKey(operateKey)) {
                    field.setAccessible(true);
                    field.set(t, taskValueMap.get(operateKey));
                }
            }
        } catch (Exception e) {
            if (e instanceof MsException && ((MsException) e).getErrCode() == ErrorNo.NO_SUCH_PROCESS) {
                throw new MsException(ErrorNo.NO_SUCH_PROCESS, "no such process");
            } else {
                log.error("reflect param error", e);
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, e.getMessage(), e.getCause());
            }
        }
        return t;
    }

    public static <T> Class<T> getClassByProcessType(String processType, String processName) {
        if (ComponentRecord.Type.VIDEO.name.equals(processType)) {
            return videoProcessparamMap.get(processName);
        } else {
            return imageProcessParamMap.get(processName);
        }
    }

    /**
     * 判断是否为0或正整数
     */
    public static boolean isPositiveIntegerOrZero(String str) {
        if (StringUtils.isBlank(str)) {
            return false;
        }
        return str.matches("^0|[1-9]\\d*$");
    }


    /**
     * 判断是否为正整数
     */
    public static boolean isPositiveInteger(String str) {
        if (StringUtils.isBlank(str)) {
            return false;
        }
        return str.matches("^[1-9]\\d*$");
    }

}
