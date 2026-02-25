package com.macrosan.database.rocksdb;/**
 * @author niechengxing
 * @create 2024-08-15 15:47
 */

/**
 *@program: MS_Cloud
 *@description:  实现rocksdb配置定时compact
 *@author: niechengxing
 *@create: 2024-08-15 15:47
 */
public class CustomOptions {
    protected CustomOptions() {}
    public static void setPeriodicCompactionSeconds0(final long handle, final long periodicCompactionSeconds) {
        setPeriodicCompactionSeconds(handle, periodicCompactionSeconds);
    }

    public static long periodicCompactionSeconds0(final long handle) {
        return periodicCompactionSeconds(handle);
    }

    private static native void setPeriodicCompactionSeconds(
            final long handle, final long periodicCompactionSeconds);
    private static native long periodicCompactionSeconds(final long handle);
}

