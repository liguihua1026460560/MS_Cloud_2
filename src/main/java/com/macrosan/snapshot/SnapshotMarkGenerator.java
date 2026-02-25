package com.macrosan.snapshot;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.VersionUtil;
import com.macrosan.utils.msutils.MsException;

import java.util.Arrays;

/**
 * @author zhaoyang
 * @date 2024/06/25
 **/
public class SnapshotMarkGenerator {

    /**
     * 快照标记长度
     */
    public static final int BUCKET_SNAPSHOT_MARK_LENGTH = 4;

    /**
     * 快照标记最大值
     */
    public static final String MAX_SNAPSHOT_MARK;
    public static final String MIN_SNAPSHOT_MARK;

    public static final String MIN_SNAPSHOT_MARK_PREFIX = "A";
    public static final String MAX_SNAPSHOT_MARK_PREFIX = "Z";
    public static final String MAX_NUMBER_STRING;

    public static final RedisConnPool POOL = RedisConnPool.getInstance();

    static {
        char[] chars = new char[BUCKET_SNAPSHOT_MARK_LENGTH];
        Arrays.fill(chars, '9');
        MAX_NUMBER_STRING = new String(chars);
        MAX_SNAPSHOT_MARK = MAX_SNAPSHOT_MARK_PREFIX + MAX_NUMBER_STRING;
        MIN_SNAPSHOT_MARK = generateSnapshotMark(1, MIN_SNAPSHOT_MARK_PREFIX);
    }


    /**
     * 通过给定的数字，生成快照标记
     *
     * @param mark 标记中的数字
     * @return 快照标记
     */
    private static String generateSnapshotMark(int mark, String prefix) {
        char[] chars = new char[BUCKET_SNAPSHOT_MARK_LENGTH];
        Arrays.fill(chars, '0');
        VersionUtil.getChars(mark, BUCKET_SNAPSHOT_MARK_LENGTH, chars);
        return prefix + new String(chars);
    }


    /**
     * 获取下一个快照标记---获取当前未使用的快照标记
     * 如果当前所有标记都使用，则抛出异常
     *
     * @param currentMark 当前快照标记
     * @return 当前使用的最大的快照标记
     */
    public static String getNextSnapshotMark(String currentMark) {
        String prefix = currentMark.substring(0, MIN_SNAPSHOT_MARK_PREFIX.length());
        String markNumber = currentMark.substring(MIN_SNAPSHOT_MARK_PREFIX.length());
        if (markNumber.equals(MAX_NUMBER_STRING)) {
            if (prefix.equals(MAX_SNAPSHOT_MARK_PREFIX)) {
                throw new MsException(ErrorNo.SNAPSHOT_MARK_OVERFLOW, "bucket snapshot mark overflow");
            }
            char nextPrefix = (char) (prefix.charAt(0) + 1);
            return generateSnapshotMark(1, String.valueOf(nextPrefix));
        }
        int num = Integer.parseInt(markNumber);
        return generateSnapshotMark(++num, prefix);
    }




}
