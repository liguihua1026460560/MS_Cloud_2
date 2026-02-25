package com.macrosan.utils.regex;

import com.macrosan.httpserver.ServerConfig;

/**
 * PatternConst
 * <p>
 * 记录了用到的正则表达式
 *
 * @author liyixin
 * @date 2019/1/8
 */
public class PatternConst {

    private PatternConst() {
    }

    public static final Pattern OBJECT_NAME_PATTERN = Pattern.compile("^[^\\\\/].{0,1023}");

    public static final Pattern DNS_PATTERN = Pattern.compile(ServerConfig.getInstance().getDns() + "|\\.");

    public static final Pattern BUCKET_NAME_PATTERN = Pattern.compile("^(?!-)[a-z0-9][a-z0-9-]{2,58}+(?<!-)$");

    public static final Pattern LIFECYCLE_RULE_ID = Pattern.compile("^[a-zA-Z0-9\\u4e00-\\u9fff_.-]+$");

    public static final Pattern DNS_REGION_PATTERN1 = Pattern.compile(".*?\\." + ServerConfig.getInstance().getDnsPrefix() + "\\..*?\\." + ServerConfig.getInstance().getDnsSuffix());

    public static final Pattern DNS_REGION_PATTERN2 = Pattern.compile("\\." + ServerConfig.getInstance().getDnsPrefix() + "\\..*?\\." + ServerConfig.getInstance().getDnsSuffix());

    public static final Pattern DATE_TIME_MSEC_T_Z_REG = Pattern.compile("^(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)T"
            + "(20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d\\.000Z");

	public static final Pattern ILLEGAL_CHARACTER = Pattern.compile("[\\u00A0\\s\"`~!@#$%^&*()+=|{}':;',\\[\\]<>/?~！@#￥%……&*()——+|{}【】‘；：”“'。，、？]+");

    public static final Pattern CHINESE_CHARACTER = Pattern.compile("[\\u2E80-\\uFE4F]+");

    public static final Pattern BUCKET_QUOTA_PATTERN = Pattern.compile("^[1-9][0-9]{0,18}|0$");

    public static final Pattern BUCKET_LIFEDAYS_PATTERN = Pattern.compile("^[1-9]+[0-9]*$");
    /**
     * YYYY-MM-DD HH:MM:SS
     */
    public static final Pattern TRAFFIC_STATIS_TIME_PATTERN = Pattern.compile("^(((01[0-9]{2}|0[2-9][0-9]{2}|[1-9][0-9]{3})-(0?[13578]|1[02])-(0?[1-9]" +
            "|[12]\\d|3[01]))|((01[0-9]{2}|0[2-9][0-9]{2}|[1-9][0-9]{3})-(0?[13456789]|1[012])-(0?[1-9]|[12]\\d|30))" +
            "|((01[0-9]{2}|0[2-9][0-9]{2}|[1-9][0-9]{3})-0?2-(0?[1-9]|1\\d|2[0-8]))|(((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|" +
            "[13579][26])|((04|08|12|16|[2468][048]|[3579][26])00))-0?2-29)) (20|21|22|23|[0-1]?\\d):[0-5]?\\d:[0-5]?\\d$");

    //iam相关
    public static final Pattern USER_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]{5,32}$");

    public static final Pattern ACCOUNT_NAME_PATTERN = Pattern.compile("^[a-zA-Z-][a-zA-Z0-9_-]{2,31}$");

    public static final Pattern USER_PASSWORD_PATTERN = Pattern.compile("(((?=.*?\\d)(?=.*?[A-Z])((?=.*?[a-z])|(?=.*?[\\.\\-_:,!@#%&*\\(\\)])))|((?=.*?[a-z])(?=.*?[\\.\\-_:,!@#%&*\\(\\)])((?=.*?\\d)|(?=.*?[A-Z]))))(^[a-zA-Z0-9\\.\\-_:,!@#%&*\\(\\)]{8,32}$)");

    public static final Pattern GROUP_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]{1,64}$");

    public static final Pattern POLICY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]{1,64}$");

    public static final Pattern PERF_QUOTA_PATTERN = Pattern.compile("^[1-9][0-9]{0,13}|0$");

    public static final Pattern CAPACITY_QUOTA_PATTERN = Pattern.compile("^[1-9][0-9]{0,18}|0$");

    public static final Pattern LIST_PARTS_PATTERN = Pattern.compile("[0-9]*");

    public static final Pattern ACCESSKEYID_PATTERN = Pattern.compile("^[A-Z0-9]{12,20}$");

    public static final Pattern ACCESSKEY_PATTERN = Pattern.compile( "^[A-Za-z0-9_-]{3,128}$");

    public static final Pattern SECRETKEY_PATTERN = Pattern.compile("^[0-9A-Za-z+=_\\-)(*&^%$#@!\\\\/?><:\"]{3,128}$");

    public static final Pattern ACCOUNTID_PATTERN = Pattern.compile("^[0-9]{12}$");

    public static final Pattern IP_CIDR_PATTERN = Pattern.compile("^(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\/([1-9]|[1-2]\\d|3[0-2])$");

    public static final Pattern IPV6_CIDR_PATTERN = Pattern.compile("^((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){1,7}:)|(([0-9A-Fa-f]{1,4}:){1,6}:[0-9A-Fa-f]{1,4})|(([0-9A-Fa-f]{1,4}:){1,5}(:[0-9A-Fa-f]{1,4}){1,2})|(([0-9A-Fa-f]{1,4}:){1,4}(:[0-9A-Fa-f]{1,4}){1,3})|(([0-9A-Fa-f]{1,4}:){1,3}(:[0-9A-Fa-f]{1,4}){1,4})|(([0-9A-Fa-f]{1,4}:){1,2}(:[0-9A-Fa-f]{1,4}){1,5})|([0-9A-Fa-f]{1,4}:((:[0-9A-Fa-f]{1,4}){1,6}))|(:((:[0-9A-Fa-f]{1,4}){1,7}|:)))(%[0-9a-zA-Z]{1,})?(\\/(12[0-8]|1[01][0-9]|[1-9]?[0-9]))?$");

    public static final Pattern VALIDITY_TIME_PATTERN = Pattern.compile("^([0-9]|[1-9][0-9]|[1-2][0-9][0-9]|[3][0-5][0-9]|(360|361|363|362|364|365))$");

    public static final Pattern TAG_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-!#$^&+*.'~|%`_]+$");
}

