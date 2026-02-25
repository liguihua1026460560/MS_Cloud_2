package com.macrosan.utils.msutils;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import static com.macrosan.utils.regex.PatternConst.DATE_TIME_MSEC_T_Z_REG;

/**
 * MsDateUtils
 * <p>
 * 处理时间转换
 *
 * @author liyixin
 * @date 2018/12/17
 */
public class MsDateUtils {

    private static final Logger logger = LogManager.getLogger(MsDateUtils.class.getName());

    private static final String ISO8601 = "yyyy-MM-dd'T'HH:mm:ss'.000Z'";

    public static final String ISO8601_AWS_HEADER = "yyyyMMdd'T'HHmmss'Z'";

    public static final String STANDARD_ISO8601 = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static final String POLICY_ISO8601 = "yyyy-MM-dd";

    private static final String STANDARD = "yyyy-MM-dd HH:mm:ss";

    public static final String STANDARD_TZ = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final String GMT = "EEE, dd MMM yyyy HH:mm:ss 'GMT'";

    private static final String UTC = "EEE, dd MMM yyyy HH:mm:ss 'UTC'";

    private static final String GMT_HEADER = "EEE, dd MMM yyyy HH:mm:ss '+0000'";

    private static final String FTP_TIME_HEADER = "yyyyMMddHHmmss.SSS";

    private MsDateUtils() {
    }


    /**
     * stampToSimpleDate
     * <p>
     * 将时间戳变成形如yyyy-MM-dd HH:mm:ss格式的字符串(本地时间)
     *
     * @param stamp 时间戳
     * @return 格式化后的字符串
     */
    public static String stampToSimpleDate(String stamp) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String simpleDate = simpleDateFormat.format(new Date(Long.parseLong(stamp)));
        return simpleDate;
    }

    /**
     * <p>
     * 将时间戳变成形如yyyy-MM-dd HH:mm:ss格式的字符串，会将GMT时间转成UTC时间
     *
     * @param stamp 时间戳
     * @return 格式化后的字符串
     */
    public static String stampToDate(String stamp) {
        return DateFormatUtils.formatUTC(Long.parseLong(stamp), STANDARD);
    }

    /**
     * 仅改变格式，不改变时区，不会改变时间大小
     * 将时间戳变成形如yyyy-MM-dd HH:mm:ss格式的字符串
     *
     * @param stamp 时间戳
     * @return 时间字符串
     */
    public static String stampToDateFormat(String stamp) {
        return DateFormatUtils.formatUTC(Long.parseLong(stamp), STANDARD_TZ);
    }

    /**
     * ISO8601转为时间戳
     *
     * @param date TZ时间戳, 时区为UTC
     * @return 本地时间戳
     */
    public static String tzToStamp(String date) {
        try {
            Date parseDate = DateUtils.parseDate(date, Locale.ENGLISH, STANDARD_ISO8601);
            TimeZone zone = TimeZone.getDefault();
            return String.valueOf(parseDate.getTime() + zone.getRawOffset());
        } catch (Exception e) {
            logger.error("parse time :" + date + "to stamp fail");
            return "0";
        }
    }

    /**
     * ISO8601转为时间戳，桶策略使用
     *
     * @param date TZ时间戳, 时区为UTC
     * @return 本地时间戳
     */
    public static String policyToStamp(String date) {
        try {
            Date parseDate = DateUtils.parseDate(date, Locale.ENGLISH, POLICY_ISO8601, STANDARD_ISO8601, STANDARD_TZ);
            return String.valueOf(parseDate.getTime());
        } catch (Exception e) {
            logger.error("policy parse time :" + date + "to stamp fail");
            return "0";
        }
    }


    /**
     * stampToDate
     * <p>
     * 将时间戳变成GMT格式
     *
     * @param stamp 时间戳
     * @return 格式化后的字符串
     */
    public static String stampToGMT(long stamp) {
        return DateFormatUtils.formatUTC(stamp, GMT);
    }

    /**
     * stampToDate
     * <p>
     * 将时间戳变成ISO8601格式的字符串
     * 注：现在是在末尾直接加上.000Z，这种做法是不正确的
     *
     * @param stamp 时间戳
     * @return ISO8601格式的时间
     */
    public static String stampToISO8601(String stamp) {
        return DateFormatUtils.formatUTC(Long.parseLong(stamp), ISO8601);
    }

    /**
     * stampToDate
     * <p>
     * 将时间戳变成ISO8601格式的字符串
     * 注：现在是在末尾直接加上.000Z，这种做法是不正确的
     *
     * @param stamp 时间戳
     * @return ISO8601格式的时间
     */
    public static String stampToISO8601(long stamp) {
        return DateFormatUtils.formatUTC(stamp, ISO8601);
    }

    /**
     * 将表示当前时间的时间戳转换为形如 EEE, d MMM yyyy HH:mm:ss 'GMT' 的GMT时间
     *
     * @return 转换为英文的GMT时间
     */

    static final TimeZone GNT_TIME_ZONE = TimeZone.getTimeZone("GMT");

    public static String nowToGMT() {
        return DateFormatUtils.format(new Date(), GMT, GNT_TIME_ZONE, Locale.ENGLISH);
    }

    /**
     * 将目前支持的任意格式的时间转化为ISO8601格式的时间
     *
     * @param date 符合目前支持的格式的时间
     * @return ISO8601格式的时间
     */
    public static String dateToISO8601(String date) {
        try {
            Date parseDate = DateUtils.parseDate(date, Locale.ENGLISH, STANDARD, GMT, ISO8601_AWS_HEADER);
            return DateFormatUtils.format(parseDate, ISO8601);
        } catch (ParseException e) {
            logger.error("parse msutils :" + date + " fail", e);
            return "";
        }
    }

    /**
     * 将目前支持的任意格式的时间转化为时间戳
     *
     * @param date 符合目前支持的格式的时间
     * @return 时间戳
     */
    public static Long dateToStamp(String date) {
        try {
            Date parseDate = DateUtils.parseDate(date, Locale.ENGLISH, GMT, ISO8601_AWS_HEADER, ISO8601, UTC, GMT_HEADER, STANDARD);
            return parseDate.getTime();
        } catch (ParseException e) {
            logger.error("parse time :" + date + "to stamp fail");
            return 0L;
        }
    }

    /**
     * 将yyyy-MM-dd HH:mm:ss格式的时间转换为时间戳
     *
     * @param date 符合yyyy-MM-dd HH:mm:ss格式的时间
     * @return 时间戳
     */
    public static Long standardDateToStamp(String date) {
        try {
            Date parseDate = DateUtils.parseDate(date, Locale.ENGLISH, STANDARD);
            return parseDate.getTime();
        } catch (ParseException e) {
            logger.info("parse time :" + date + "to stamp fail");
            return 0L;
        }
    }

    /**
     * 判断时间是否符合ISO8601的格式
     *
     * @param date 符合目前支持的格式的时间
     * @return 是否符合ISO8601的格式
     */
    public static boolean judgeDate(String date) {
        return DATE_TIME_MSEC_T_Z_REG.matcher(date).matches();
    }

    public static String formatFTPTime(long stamp) {
        return DateFormatUtils.formatUTC(stamp, FTP_TIME_HEADER);
    }
}
