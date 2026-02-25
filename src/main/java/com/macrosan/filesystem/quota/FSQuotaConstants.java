package com.macrosan.filesystem.quota;

import java.util.HashSet;
import java.util.Set;

public class FSQuotaConstants {

    /**
     * 通用常量
     */
    public static final String QUOTA_TYPE = "quota_type";
    public static final String DIR_NAME = "dir_name";
    public static final String QUOTA_SUFFIX = "_quota";
    public static final String QUOTA_PREFIX = "quota_";
    public static final String QUOTA_KEY = "quota_key";
    public static final String QUOTA_KEY_CREATE_S3_INODE = "quota_key_create_s3_inode";
    public static final String CREATE_S3_INODE_TIME = "create_s3_inode_time";
    public static final String MODIFY = "modify";
    public static final String FILES_SOFT_QUOTA = "quota_limit_files_soft";
    public static final String FILES_HARD_QUOTA = "quota_limit_files_hard";
    public static final String CAPACITY_SOFT_QUOTA = "quota_limit_capacity_soft";
    public static final String CAPACITY_HARD_QUOTA = "quota_limit_capacity_hard";
    public static final int MAX_SCAN_COUNT = 100_000;

    /**
     * 目录配额相关
     */
    public static final int FS_DIR_QUOTA = 3;
    public static final String FS_QUOTA_DIR_ALARM_PREFIX = "dir_";
    public static final String FS_DIR_QUOTA_PREFIX = "quota_dir_";
    public static final int MAX_DIR_QUOTA_COUNT = 256;
    /**
     * 用户配额相关
     */
    public static final String FS_UID = "uid";
    public static final String FS_QUOTA_USR_ALARM_PREFIX = "user_";
    public static final String FS_CIFS_USER = "cifs_user";
    public static final int FS_USER_QUOTA = 0;
    public static final String FS_USER_QUOTA_PREFIX = "quota_user_";
    public static final int MAX_USER_QUOTA_COUNT = 256;
    /**
     * 用户组配额相关
     */
    public static final String FS_QUOTA_GROUP_ALARM_PREFIX = "group_";
    public static final String FS_GID = "gid";
    public static final String FS_GROUP_QUOTA_PREFIX = "quota_group_";
    public static final int FS_GROUP_QUOTA = 1;
    public static final int MAX_GROUP_QUOTA_COUNT = 256;


    /**
     * 告警相关
     */
    public static final String FS_QUOTA_CAP_ALARM_PREFIX = "alarm_cap_quota_";
    public static final String FS_QUOTA_CAP_ALARM_PREFIX0 = "alarm_cap_";
    public static final String FS_QUOTA_FILES_ALARM_PREFIX = "alarm_files_quota_";
    public static final String FS_QUOTA_FILES_ALARM_PREFIX0 = "alarm_files_";
    public static final String LAST_ALARM_SUFFIX = "_last_alarm";
    public static final String EXCEED_QUOTA_HARD_LIMIT = "2";
    public static final String EXCEED_QUOTA_SOFT_LIMIT = "1";
    public static final String NOT_EXCEED_QUOTA = "0";


    public static final int FS_ALARM_CHECK_CONCURRENCY = 4;

    /**
     * 向前滚动时间扫描，比设置时刻大3分钟
     */
    public static final long QUOTA_SCAN_EXPAND_TIME = 3 * 60 * 1000;

    public static final int QUOTA_UPDATE_DIR_EXPAND_TIME = 2 * 60 * 1000;

    public static final long BLOCK_SIZE = 4096L;

    /**
     * 配设置的间隔时间，单位秒，删除配额后，五分钟之内不能再次设置
     */
    public static final int QUOTA_SET_INTERVAL = 300;

    public static final String SMB_CAP_QUOTA_FILE_NAME = "$Extend/$Quota:$Q:$INDEX_ALLOCATION";

    public static final String S3_BUCKET_CAP_HARD_LIMIT = "quota_value";
    public static final String S3_BUCKET_CAP_SOFT_LIMIT = "soft_quota_value";
    public static final String S3_BUCKET_OBJ_NUM_HARD_LIMIT = "objnum_value";
    public static final String S3_BUCKET_OBJ_NUM_SOFT_LIMIT = "soft_objnum_value";

    public static final Set<Integer> FS_QUOTA_TYPE_SET = new HashSet<>();

    static {
        FS_QUOTA_TYPE_SET.add(FS_DIR_QUOTA);
        FS_QUOTA_TYPE_SET.add(FS_USER_QUOTA);
        FS_QUOTA_TYPE_SET.add(FS_GROUP_QUOTA);
    }
}
