package com.macrosan.utils.msutils;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * TODO
 *
 * @Author chengyinfeng
 * @Date 2024/10/22 10:28
 */
public class CommonUtils {

    public static String getPercentageValue(String total, String used) {
        if ("0".equals(total) || "0".equals(used)) {
            return "0 %";
        }
        double percentage = (Double.parseDouble(used) / Double.parseDouble(total)) * 100;

        // 使用 BigDecimal 保留两位小数
        BigDecimal bd = new BigDecimal(percentage);
        // 保留两位小数并四舍五入
        bd = bd.setScale(2, RoundingMode.HALF_UP);
        return bd + " %";
    }

    public static void main(String[] args) {
        System.out.println(getPercentageValue("100000", "1084"));
    }
}
