package com.macrosan.database.rocksdb;/**
 * @author niechengxing
 * @create 2024-08-14 19:09
 */

import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.Slice;

/**
 *@program: MS_Cloud
 *@description: 自定义实现rocksdb中过滤掉过期数据的compactionFilter
 *@author: niechengxing
 *@create: 2024-08-14 19:09
 */
public class RemoveExpiredDataCompactionFilter extends AbstractCompactionFilter<Slice> {
    protected RemoveExpiredDataCompactionFilter() {
        super(createNewRemoveExpiredDataCompactionFilter0());
    }

    public static native long createNewRemoveExpiredDataCompactionFilter0();
}

