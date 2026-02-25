package com.macrosan.storage.metaserver.statistic;

import com.macrosan.database.rocksdb.MSRocksDB;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.LiveFileMetaData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 由于rocksdbjava 6.8.1版本不支持使用db.getApproximateSize操作，故此处使用java替代该操作
 * 此类可以用于计算指定rocksdb实例中指定范围内的容量使用大致值。
 */
@Log4j2
public class RangeApproximateSizesStatistic {
    public static class Range {
        private final String startKey;
        private final String endKey;

        public Range(String startKey, String endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
        }
    }

    private final MSRocksDB db;

    private double filesSizeErrorMargin = 0.1; // 默认允许10%的误差范围

    public RangeApproximateSizesStatistic(String lun) {
        this.db = MSRocksDB.getRocksDB(lun);
    }

    public RangeApproximateSizesStatistic(String lun, double filesSizeErrorMargin) {
        this.db = MSRocksDB.getRocksDB(lun);
        this.filesSizeErrorMargin = filesSizeErrorMargin;
    }

    /**
     * 获取多个指定范围的大小。
     *
     * @param ranges 范围数组
     * @return 各范围对应的大小。
     */
    public long[] getApproximateSizes(Range[] ranges) {
        // 1. 获取所有SST文件元数据
        List<LiveFileMetaData> liveFiles = db.getLiveFilesMetaData();
        // 2. 按Level分组处理
        Map<Integer, List<LiveFileMetaData>> filesByLevel = liveFiles.stream()
                .collect(Collectors.groupingBy(LiveFileMetaData::level));
        return getApproximateSizes(filesByLevel, ranges);
    }


    public long getApproximateSize(String startKey, String endKey) {
        // 1. 获取所有SST文件元数据
        List<LiveFileMetaData> liveFiles = db.getLiveFilesMetaData();
        // 2. 按Level分组处理
        Map<Integer, List<LiveFileMetaData>> filesByLevel = liveFiles.stream()
                .collect(Collectors.groupingBy(LiveFileMetaData::level));
        return getApproximateSize(filesByLevel, startKey, endKey);
    }

    public long[] getApproximateSizes(Map<Integer, List<LiveFileMetaData>> filesByLevel, Range[] ranges) {
        long[] approximateSizes = new long[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            approximateSizes[i] = getApproximateSize(filesByLevel, range.startKey, range.endKey);
        }
        return approximateSizes;
    }

    /**
     * 获取指定范围的大小。
     *
     * @param startKey 起始键
     * @param endKey   结束键
     * @return approximateSize
     */
    private long getApproximateSize(Map<Integer, List<LiveFileMetaData>> filesByLevel, String startKey, String endKey) {
        long totalFullSize = 0;
        // 保存第0层的文件
        List<LiveFileMetaData> firstFiles = new ArrayList<>();
        // 保存最后一层的文件
        List<LiveFileMetaData> lastFiles = new ArrayList<>();
        // 处理每一层的文件
        Set<Integer> levels = filesByLevel.keySet();
        for (Integer level : levels) {
            if (level == 0) {
                List<LiveFileMetaData> liveFileMetaData = filesByLevel.get(level);
                firstFiles.addAll(liveFileMetaData);
                continue;
            }

            // startKey的文件位置
            List<LiveFileMetaData> files = filesByLevel.get(level);
            int startIndex = findFileIndex(files, startKey.getBytes());
            if (startIndex < 0) {
                continue;
            }
            int endIndex = startIndex;
            if (ByteArrayComparator.compare(files.get(endIndex).largestKey(), endKey.getBytes()) < 0) {
                endIndex = findFileIndex(files, endKey.getBytes());
            }
            // 首先扫描所有中间完整文件（不包括第一个和最后一个）
            for (int i = startIndex + 1; i < endIndex; i++) {
                long fileSize = files.get(i).size();
                totalFullSize += fileSize;
            }

            // 保存第一个和最后一个文件（可能是同一个文件），以便我们以后扫描它们。
            firstFiles.add(files.get(startIndex));
            if (startIndex != endIndex) {
                lastFiles.add(files.get(endIndex));
            }
        }

        // 与[start，end]键范围相交的所有文件大小的总和
        long totalIntersectingSize = 0;
        for (LiveFileMetaData firstFile : firstFiles) {
            totalIntersectingSize += firstFile.size();
        }
        for (LiveFileMetaData lastFile : lastFiles) {
            totalIntersectingSize += lastFile.size();
        }
        // 若相交文件的大小小于等于指定百分比，则将这些文件的一半大小添加到完整文件大小总和中。
        if (filesSizeErrorMargin > 0 && totalIntersectingSize < totalFullSize * filesSizeErrorMargin) {
            totalFullSize += totalIntersectingSize / 2;
        } else {
            // 若totalFullSize为0，则可能不存在该范围内的key或者非常小，则直接忽略相交文件的大小
            if (totalFullSize != 0) {
                totalFullSize += estimateIntersectionSize(firstFiles, startKey.getBytes(), endKey.getBytes());
                totalFullSize += estimateIntersectionSize(lastFiles, startKey.getBytes(), endKey.getBytes());
            }
        }

        return totalFullSize;
    }

    private static long estimateIntersectionSize(List<LiveFileMetaData> files,
                                                 byte[] startKey,
                                                 byte[] endKey) {
        long totalSize = 0;

        for (LiveFileMetaData file : files) {
            // 检查文件范围是否与目标范围重叠
            if (isRangeOverlap(file, startKey, endKey)) {
                // Level 0文件可能完全重叠，直接计入文件大小
                totalSize += estimateFileOverlapSize(file, startKey, endKey);
            }
        }

        return totalSize;
    }

    /**
     * 估算单个文件的重叠大小
     */
    private static long estimateFileOverlapSize(LiveFileMetaData file,
                                                byte[] startKey,
                                                byte[] endKey) {
        // 如果是完全包含，返回整个文件大小
        if (isFullyContained(file, startKey, endKey)) {
            return file.size();
        }

        // 否则按重叠比例估算
        double overlapRatio = calculateOverlapRatio(file, startKey, endKey);
        return (long) (file.size() * overlapRatio);
    }


    /**
     * 检查文件是否完全包含在目标范围内
     */
    private static boolean isFullyContained(LiveFileMetaData file,
                                            byte[] startKey,
                                            byte[] endKey) {
        byte[] fileStartKey = file.smallestKey();
        byte[] fileEndKey = file.largestKey();

        return ByteArrayComparator.compare(fileStartKey, startKey) >= 0 &&
                ByteArrayComparator.compare(fileEndKey, endKey) <= 0;
    }

    /**
     * 检查文件范围是否与目标范围重叠
     */
    private static boolean isRangeOverlap(LiveFileMetaData file,
                                          byte[] startKey,
                                          byte[] endKey) {
        // 获取文件的key范围
        byte[] fileStartKey = file.smallestKey();
        byte[] fileEndKey = file.largestKey();

        // 检查是否有重叠
        return !(ByteArrayComparator.compare(fileEndKey, startKey) < 0 ||
                ByteArrayComparator.compare(fileStartKey, endKey) > 0);
    }

    // TODO: 当前计算重叠比例不精确，需要进一步优化
    private static double calculateOverlapRatio(LiveFileMetaData file,
                                                byte[] startKey,
                                                byte[] endKey) {
        byte[] fileStartKey = file.smallestKey();
        byte[] fileEndKey = file.largestKey();

        // 计算重叠区间
        byte[] overlapStart = ByteArrayComparator.compare(fileStartKey, startKey) > 0 ?
                fileStartKey : startKey;
        byte[] overlapEnd = ByteArrayComparator.compare(fileEndKey, endKey) < 0 ?
                fileEndKey : endKey;

        // 估算重叠比例
        long totalRange = ByteArrayComparator.compare(fileEndKey, fileStartKey);
        long overlapRange = ByteArrayComparator.compare(overlapEnd, overlapStart);

        if (totalRange <= 0) return 1.0; // 避免除以0
        return Math.min(1.0, Math.max(0.0, (double) overlapRange / totalRange));
    }

    private static int findFileIndex(List<LiveFileMetaData> files, byte[] key) {
        if (files.isEmpty()) {
            return -1;
        }
        int left = 0;
        int right = files.size() - 1;

        while (left <= right) {
            int mid = (left + right) / 2;
            LiveFileMetaData file = files.get(mid);

            if (ByteArrayComparator.compare(file.largestKey(), key) < 0) {
                left = mid + 1;
            } else if (ByteArrayComparator.compare(file.smallestKey(), key) > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }

        return left - 1 >= 0 ? left - 1 : -1;
    }

    private static class ByteArrayComparator {
        public static int compare(byte[] a, byte[] b) {
            int minLength = Math.min(a.length, b.length);
            for (int i = 0; i < minLength; i++) {
                int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
                if (diff != 0) {
                    return diff;
                }
            }
            return a.length - b.length;
        }
    }
}
