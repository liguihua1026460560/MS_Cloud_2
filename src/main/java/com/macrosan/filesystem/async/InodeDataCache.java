package com.macrosan.filesystem.async;

import com.macrosan.doubleActive.DataSynChecker;
import com.macrosan.filesystem.cache.TimeCache;
import com.macrosan.message.jsonmsg.Inode;
import lombok.extern.log4j.Log4j2;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
public class InodeDataCache {
    private final static TimeCache<Long, CacheMark> markMap = new TimeCache<>(Duration.ofSeconds(5).toNanos(), DataSynChecker.SCAN_SCHEDULER);

    private static final int cacheListThreshold = 100;

    private static class CacheMark {
        long nodeId;
        // key为InodeData的fileName
        // todo f 改成set
        Map<String, Inode.InodeData> inodeDataMap;

        CacheMark(long nodeId) {
            this.nodeId = nodeId;
            inodeDataMap = new HashMap<>();
        }
    }

    static CacheMark getInodeDataCache(Inode curInode) {
        long nodeId = curInode.getNodeId();
        return markMap.compute(nodeId, (k, v) -> {
            if (v == null) {
                v = new CacheMark(nodeId);
                for (Inode.InodeData inodeData : curInode.getInodeData()) {
                     v.inodeDataMap.put(inodeData.fileName, inodeData);
                }
                return v;
            } else {
                return v;
            }
        });
    }

    /**
     * 确认record记录中的InodeData是否需要执行同步。
     * 目的是防止InodeDataList很大的时候，每次同步数据都需要去通过遍历确定fileName是否匹配
     *
     * @param recordFileName record中记录的inodeData.recordFileName
     * @param curInode       getInode的当前结果
     * @return 当前文件相关的InodeData是否匹配record中的InodeData，true表示将继续dealRecord流程，false表示无需处理该record。
     */
    public static boolean checkInodeData(String recordFileName, Inode curInode) {
        // todo del 改成1000。测试稳定性
        if (curInode.getInodeData().size() < cacheListThreshold) {
            return traversingInodeDataList(recordFileName, curInode);
        }

        // 如果查询到的curInode里的inodeData list很长，尝试使用读取内存。
        CacheMark cacheMark = getInodeDataCache(curInode);
        if (cacheMark != null && cacheMark.inodeDataMap.containsKey(recordFileName)) {
            // 此时可能缓存尚未更新。后续的同步流程里readObj会出错并超时，本轮record处理将返回false。
            return true;
        } else {
            // 此时可能是inode已被修改覆盖（record不处理），或者缓存还未更新（record要处理）。
            // 将curInode中的inodeDataList遍历一遍确认fileName是否匹配。
            return traversingInodeDataList(recordFileName, curInode);
        }
    }

    static boolean traversingInodeDataList(String recordFileName, Inode curInode) {
        List<Inode.InodeData> curInodeDataList = curInode.getInodeData();
        boolean res = false;
        for (Inode.InodeData data : curInodeDataList) {
            if (data.fileName.equals(recordFileName)) {
                res = true;
                break;
            }
        }
        return res;
    }

}
