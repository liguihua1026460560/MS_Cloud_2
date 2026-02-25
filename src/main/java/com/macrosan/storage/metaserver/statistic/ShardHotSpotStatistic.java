package com.macrosan.storage.metaserver.statistic;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.metaserver.ShardingWorker;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.LiveFileMetaData;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Log4j2
public class ShardHotSpotStatistic {

    public static void updateVNodeUsageBytes(StoragePool pool) {
        ShardingWorker.SHARDING_SCHEDULER.schedule(() -> realUpdateVNodeUsageBytes(pool), 10, TimeUnit.SECONDS);
    }

    public static void realUpdateVNodeUsageBytes(StoragePool pool) {
        try {
            String hostUuid = ServerConfig.getInstance().getHostUuid();
            long start  = System.currentTimeMillis();
            // 获取该池当前节点所有的lun
            List<String> localLunList = pool.getCache()
                    .lunSet.stream()
                    .filter(lun -> hostUuid.equals(lun.split("@")[0]))
                    .map(lun -> lun.split("@")[1])
                    .collect(Collectors.toList());
            // 提前获取对应lun的所有sst文件
            Map<String, Map<Integer, List<LiveFileMetaData>>> localLunLiveFileMap = new HashMap<>();
            for (String lun : localLunList) {
                MSRocksDB db = MSRocksDB.getRocksDB(lun);
                if (db == null) {
                    continue;
                }
                List<LiveFileMetaData> liveFileMetaDataList = db.getLiveFilesMetaData();
                Map<Integer, List<LiveFileMetaData>> filesByLevel = liveFileMetaDataList.stream()
                        .collect(Collectors.groupingBy(LiveFileMetaData::level));
                localLunLiveFileMap.put(lun, filesByLevel);
            }
            // 计算每个vnode的sst文件大小
            Set<String> vNodeList = pool.getCache().getCache().keySet();
            Map<String, Long> vNodeUsageBytes = new HashMap<>();
            for (String vNode : vNodeList) {
                long usageBytes = getVNodeUsageBytes(pool, localLunLiveFileMap,vNode);
                vNodeUsageBytes.put(vNode, usageBytes);
            }
            // 从大到小排序处理
            List<Map.Entry<String, Long>> usageBytesList = new ArrayList<>(vNodeUsageBytes.entrySet());
            usageBytesList.sort((o1, o2) -> Long.compare(o2.getValue(), o1.getValue()));
            for (Map.Entry<String, Long> entry : usageBytesList) {
                if (entry.getValue() != 0) {
                    RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_MAPINFO_INDEX)
                            .hset(entry.getKey(), "usage_bytes" + hostUuid, String.valueOf(entry.getValue()));
                } else {
                    RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_MAPINFO_INDEX)
                            .hdel(entry.getKey(), "usage_bytes" + hostUuid);
                }
            }
            long end = System.currentTimeMillis();
            log.debug("updateVNodeUsageBytes {} cost: {} ms", vNodeUsageBytes.size(),(end - start));
        } catch (Exception e){
            log.error("updateVNodeUsageBytes error", e);
        }
        finally {
            ErasureServer.DISK_SCHEDULER.schedule(() -> realUpdateVNodeUsageBytes(pool), 1, TimeUnit.HOURS);
        }
    }

    private static long getVNodeUsageBytes(StoragePool pool,Map<String, Map<Integer, List<LiveFileMetaData>>>  localLunLiveFileMap, String vNode) {
        long[] maxSize = new long[]{0};
        try {
            // 获取node在当前节点的磁盘关联组
            List<String> localLunList = pool.getLocalLun(vNode);
            if (localLunList.isEmpty()) {
                return 0;
            }
            for (String localLun : localLunList) {
                long totalSize = 0;
                MSRocksDB db = MSRocksDB.getRocksDB(localLun);
                if (db == null) {
                    continue;
                }
                Map<Integer, List<LiveFileMetaData>> levelFiles = localLunLiveFileMap.get(localLun);
                if (levelFiles == null) {
                    continue;
                }
                RangeApproximateSizesStatistic rangeApproximateSizesStatistic = new RangeApproximateSizesStatistic(localLun);
                String link = pool.getCache().hget(vNode, "link");
                String[] linkArray = Json.decodeValue(link, String[].class);
                String[] prefixArray = new String[]{"", "*", "-", "+"};
                List<RangeApproximateSizesStatistic.Range> rangeList = new ArrayList<>();
                for (String node : linkArray) {
                    for (String prefix : prefixArray) {
                        String startKey = prefix + node;
                        String endKey = prefix + node + "0";
                        rangeList.add(new RangeApproximateSizesStatistic.Range(startKey, endKey));
                    }
                }
                long[] approximateSizes = rangeApproximateSizesStatistic
                        .getApproximateSizes(levelFiles, rangeList.toArray(new RangeApproximateSizesStatistic.Range[0]));
                for (int i = 0; i < approximateSizes.length; i++) {
                    totalSize += approximateSizes[i];
                }
                if (maxSize[0] == 0 || totalSize > maxSize[0]) {
                    maxSize[0] = totalSize;
                }
            }
        } catch (Exception e) {
            log.error(vNode + " estimateVNodeUsageBytes error", e);
        }
        return maxSize[0];
    }
}
