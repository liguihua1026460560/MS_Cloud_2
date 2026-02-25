package com.macrosan.clearmodel;

import com.macrosan.message.xmlmsg.clear.ClearConfigration;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JaxbUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.*;
import java.time.format.DateTimeFormatter;

import static com.macrosan.clearmodel.ClearModelExecutor.*;
import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;

@Slf4j
public class ClearModelUtils {

    // 校验时间格式是否合法（H:mm 或 HH:mm）
    public static boolean isValidTimeFormat(String time) {
        if (time == null || time.isEmpty()) return false;

        // 正则表达式说明：
        // ^          : 字符串开始
        // (0?[0-9]|1[0-9]|2[0-3]) : 小时部分（0-23，允许前导零）
        // :          : 分隔符
        // ([0-5][0-9]) : 分钟部分（00-59）
        // $          : 字符串结束
        String pattern = "^(0?[0-9]|1[0-9]|2[0-3]):([0-5][0-9])$";
        return time.matches(pattern);
    }

    public static boolean isCurrentTimeInRange(String startTime, String endTime) {
        LocalTime start = LocalTime.parse(startTime,DateTimeFormatter.ofPattern("H:mm"));
        LocalTime end = LocalTime.parse(endTime,DateTimeFormatter.ofPattern("H:mm"));
        LocalTime now = LocalTime.now();

        if (start.equals(end)) {
            return true; // 开始结束时间相同视为全天有效
        } else if (start.isBefore(end)) {
            return !now.isBefore(start) && !now.isAfter(end);
        } else {
            // 跨天情况：当前时间 >= start 或 <= end
            return !now.isBefore(start) || !now.isAfter(end);
        }
    }

    public static boolean validatePercentage(String capacityRemaining) {
        try {
            int percent = Integer.parseInt(capacityRemaining);
            return percent >= 0 && percent <= 100 && Integer.toString(percent).equals(capacityRemaining);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // 计算Long值与百分比的乘积（四舍五入）
    public static long calculateResult(long value, String percentage) {
        int percent = Integer.parseInt(percentage);
        double factor = percent / 100.0;
        return (long) Math.floor(value * factor);
    }

    public static String getXmlConfig(ClearConfigration clearConfigration) {
        String xmlConfig;
        String model = clearConfigration.getModel();
        boolean isAllowedModel = MODEL_DEFAULT.equals(model);
        if (!isAllowedModel){
            throw new MsException(INVALID_MODEL, "the model is invalid");
        }
        ClearConfigration config = new ClearConfigration();
        if (MODEL_DEFAULT.equals(model)) {
            config.setModel(MODEL_DEFAULT);
            boolean flag1 = StringUtils.isEmpty(clearConfigration.getStartTime());
            boolean flag2 = StringUtils.isNotEmpty(clearConfigration.getStartTime());
            boolean flag3 = StringUtils.isEmpty(clearConfigration.getEndTime());
            boolean flag4 = StringUtils.isNotEmpty(clearConfigration.getEndTime());
            if ((flag1 && flag4) || (flag2 && flag3) ){
                throw new MsException(EMPTY_TIME, "the startTime or endTime is empty");
            }
            if (flag2 && flag4){
                if (!isValidTimeFormat(clearConfigration.getStartTime()) || !isValidTimeFormat(clearConfigration.getEndTime())){
                    throw new MsException(INVALID_TIME, "the time is invalid");
                }
                config.setEndTime(clearConfigration.getEndTime());
                config.setStartTime(clearConfigration.getStartTime());
            }
            if (StringUtils.isNotEmpty(clearConfigration.getCapacityRemaining())){
                if (!validatePercentage(clearConfigration.getCapacityRemaining())){
                    throw new MsException(INVALID_REMAINING, "the capacityRemaining is invalid");
                }else {
                    config.setCapacityRemaining(clearConfigration.getCapacityRemaining());
                }
            }
            if (StringUtils.isNotEmpty(clearConfigration.getObjectRemaining())){
                if (!validatePercentage(clearConfigration.getObjectRemaining())){
                    throw new MsException(INVALID_REMAINING, "the objectRemaining is invalid");
                }else {
                    config.setObjectRemaining(clearConfigration.getObjectRemaining());
                }
            }
        }
        xmlConfig = new String(JaxbUtils.toByteArray(config));
        return xmlConfig;
    }

}
