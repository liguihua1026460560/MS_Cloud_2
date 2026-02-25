package com.macrosan.storage.metaserver;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.storage.VnodeCache;
import com.macrosan.utils.functional.Tuple2;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 用于为桶下的分片分配对应的vnode信息，保证每个分片尽量能够映射在不同的物理磁盘上
 */
@Log4j2
public class ShardingMapping {

    /**
     * 用于缓存分片以及分片以及vnode之间的映射关系，同一组的vnode所关联的物理节点磁盘一致
     */
    private final Map<String, List<String>> linkGroupMapCache = new ConcurrentHashMap<>();
    private final Map<String, BitSet> filter =new ConcurrentHashMap<>();

    /**
     * 缓存vnode相关信息
     */
    private final VnodeCache cache;

    private final String vnodePrefix;

    private final String mapVnodePrefix;

    public ShardingMapping(VnodeCache cache, String vnodePrefix, String mapVnodePrefix) {
        this.cache = cache;
        this.vnodePrefix = vnodePrefix;
        this.mapVnodePrefix = mapVnodePrefix;
    }

    /**
     * 为桶的分片分配指定数量的vnode
     * @param bucketName 桶名
     * @param sourceVnodeArray 已分配的vnode
     * @param target 需要再次分配的vnode数目
     * @return 目标vnode数组
     */
    public synchronized String[] allocate(String bucketName, List<String> sourceVnodeArray, int target) {
        if (!ShardingScheduler.BUCKET_HASH_SWITCH) {
            log.debug("The current environment doest not enabled bucket hash switch.");
            return null;
        }
        if (linkGroupMapCache.isEmpty()) {
            log.info("ShardingMapping uninitialized!");
            return null;
        }
        if (ShardingScheduler.DISABLED_BUCKET_HASH.get()) {
            log.debug("The current environment is undergoing reconstruction and migration, and the bucket hash function will be disabled.");
            return null;
        }
        if (sourceVnodeArray.size() >= linkGroupMapCache.size()) {
            log.debug("The current environment does not support sharding operations.");
            return null;
        }
        target = Math.min(target, linkGroupMapCache.size() - sourceVnodeArray.size());
        String[] res = new String[target];
        for (int i = 0; i < target; i++) {
            Map.Entry<String, Integer> distinct = distinct(sourceVnodeArray);
            if (distinct == null) {
                log.debug("The current environment does not support sharding operations.");
                return null;
            }
            List<String> list = linkGroupMapCache.get(distinct.getKey());
            String vnode = list.get(Math.abs(bucketName.hashCode()) % list.size());
            if (sourceVnodeArray.contains(vnode)) {
                log.debug("The current environment allocate sharding failed.");
                return null;
            }
            res[i] = vnode;
            sourceVnodeArray.add(vnode);
        }
        return res;
    }

    /**
     * 判断vnode是否处于同一分组内
     * @param v1 vnode1
     * @param v2 vnode2
     */
    public synchronized boolean groupIsEquals(String v1, String v2) {
        for (Map.Entry<String, BitSet> entry : filter.entrySet()) {
            BitSet value = entry.getValue();
            if (value.get(Integer.parseInt(v1)) && value.get(Integer.parseInt(v2))) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断两个vnode所在分组是否包含相同的磁盘
     * @param v1 vnode1
     * @param v2 vnode2
     */
    public synchronized boolean hasPublicDisk(String v1, String v2) {
        return countCommonGroup(getGroupByVnode(v1).split(File.separator), getGroupByVnode(v2).split(File.separator)) != 0;
    }

    /**
     * 获取lun冲突的vnode
     * @param vnodeList 桶所有分片对应的vnode
     * @return key:冲突的lun,value: vnode列表
     */
    public Map<String, List<String>> getLunConflictGroup(List<String> vnodeList) {
        Map<String, List<String>> res = new HashMap<>(vnodeList.size());
        for (String node : vnodeList) {
            String[] group = getGroupByVnode(node).split(File.separator);
            for (String g : group) {
                List<String> list = res.getOrDefault(g, new LinkedList<>());
                list.add(node);
                res.put(g, list);
            }
        }
        res.entrySet().removeIf(next -> next.getValue().size() < 2);
        return res;
    }

    /**
     * 获取vnode所在分组
     */
    public synchronized String getGroupByVnode(String vnode) {
        for (Map.Entry<String, BitSet> entry : filter.entrySet()) {
            if (entry.getValue().get(Integer.parseInt(vnode))) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * 采用懒加载处理
     */
    public synchronized void update() {
        // 清空旧的映射关系
        if (!linkGroupMapCache.isEmpty()) {
            linkGroupMapCache.clear();
        }
        // 清空旧的映射记录
        if (!filter.isEmpty()) {
            filter.clear();
        }
        // 记录起始时间，用于记录更新过程耗费的时间
        long start = System.currentTimeMillis();
        try {
            BitSet bitSet = new BitSet(cache.getCache().size());
            for (Map.Entry<String, Map<String, String>> entry : cache.getCache().entrySet()) {
                Tuple2<String, String> tuple2 = getPrefix(entry.getKey());
                String metaPrefix = tuple2.var1;
                String vnode = tuple2.var2;
                // 只对原始vnode进行组别划分
                if (!vnodePrefix.equals(metaPrefix)) {
                    continue;
                }
                // 如果vnode已添加到分组则跳过
                if (bitSet.get(Integer.parseInt(vnode)))  {
                    continue;
                }
                Map<String, String> map = entry.getValue();
                String mapV = map.get("map");
                String prefix;
                String link = map.get("link");
                if (mapV != null) {
                    prefix = "map-" + metaPrefix;
                    link = cache.hget(mapV, "link");
                } else {
                    prefix = metaPrefix;
                }
                String[] bucketVnodes = link.substring(1, link.length() - 1).split(",");
                String diskLinkGroup = Arrays.stream(bucketVnodes).map((v) -> {
                    v = prefix + v;
                    if (v.equals(entry.getKey())) {
                        String uuid = map.get("s_uuid");
                        String lun = map.get("lun_name");
                        return uuid + "@" + lun;
                    }
                    String uuid = cache.hget(v, "s_uuid");
                    String lun = cache.hget(v, "lun_name");
                    return uuid + "@" + lun;
                }).sorted(Comparator.naturalOrder()).collect(Collectors.joining(File.separator));

                if (mapV != null) {
                    for (int i = 0; i < bucketVnodes.length; i++) {
                        String bucketVnode = bucketVnodes[i];
                        String reverse =cache.hget(prefix + bucketVnode, "reverseMap");
                        if (reverse != null && !reverse.isEmpty()) {
                            bucketVnodes[i] = reverse.substring(metaPrefix.length());
                        } else {
                            bucketVnodes[i] = "-" + bucketVnode;
                        }
                    }
                }

                List<String> list = linkGroupMapCache.getOrDefault(diskLinkGroup, new LinkedList<>());
                BitSet filterBitSet = filter.getOrDefault(diskLinkGroup, new BitSet());
                for (String bucketVnode : bucketVnodes) {
                    if (bucketVnode.startsWith("-")) {
                        continue;
                    }
                    if (!bitSet.get(Integer.parseInt(bucketVnode))) {
                        list.add(bucketVnode);
                        filterBitSet.set(Integer.parseInt(bucketVnode));
                        bitSet.set(Integer.parseInt(bucketVnode));
                    }
                }
                filter.put(diskLinkGroup, filterBitSet);
                linkGroupMapCache.put(diskLinkGroup, list);
            }

        } catch (Exception e) {
            log.error("", e);
            throw new RuntimeException("Update ShardingMapping error!", e);
        }
        long end = System.currentTimeMillis();
        if ((end - start) > 1000) {
            log.info("update sharding mapping costs {} ms", (end - start));
        } else {
            log.debug("update sharding mapping costs {} ms", (end - start));
        }
    }

    /**
     * 获取与已分配的关联组差异最大并且磁盘可用数量大于k的关联组
     * todo 需要完善分配逻辑，使分片分配的更均匀
     * @param sourceVnodeArray 已分配的vnode
     */
    private Map.Entry<String, Integer> distinct(List<String> sourceVnodeArray) {
        return distinctList(sourceVnodeArray)
                .stream()
                .filter(entry -> entry.getValue() == 0)
                .findAny()
                .orElse(null);
    }

    private List<Map.Entry<String, Integer>> distinctList (List<String> sourceVnodeArray) {
        Set<String> allGroupName = new HashSet<>(linkGroupMapCache.keySet());
        Set<String> allocatedGroupName = new HashSet<>(sourceVnodeArray.size());
        // 从所有分组中移除已分配的分组
        for (String vnode : sourceVnodeArray) {
            String vnodeLinkGroupName = getVnodeLinkGroupName(vnode);
            allGroupName.remove(vnodeLinkGroupName);
            allocatedGroupName.add(vnodeLinkGroupName);
        }
        // 从剩下的分组中赛选磁盘重叠最少的分组
        Map<String, Integer> countMap = new HashMap<>();
        for (String group : allGroupName) {
            int count = 0;
            String[] linkArray = group.split(File.separator);
            for (String allocatedGroup : allocatedGroupName) {
                String[] links = allocatedGroup.split(File.separator);
                count += countCommonGroup(linkArray, links);
            }
            countMap.put(group, count);
        }

        return countMap
                .entrySet()
                .stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .collect(Collectors.toList());
    }

    /**
     * 统计磁盘关联组中磁盘冲突的数目
     * @param arr1 关联组1
     * @param arr2 关联组2
     */
    private static int countCommonGroup(String[] arr1, String[] arr2) {
        int count = 0;
        if (arr1 == null || arr2 == null || arr1.length == 0 || arr2.length == 0) {
            return count;
        }
        Arrays.sort(arr1);
        Arrays.sort(arr2);
        int i = 0, j = 0;
        while (i < arr1.length && j < arr2.length) {
            if (arr1[i].compareTo(arr2[j]) == 0) {
                count++;
                i++;
                j++;
            } else if (arr1[i].compareTo(arr2[j]) < 0) {
                i++;
            } else {
                j++;
            }
        }
        return count;
    }

    private String getVnodeLinkGroupName(String vnode) {
        for (Map.Entry<String, List<String>> entry : linkGroupMapCache.entrySet()) {
            List<String> value = entry.getValue();
            if (value.contains(vnode)) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * @param num 任意正整数
     * @return num的阶乘
     */
    private static long factorial(int num) {
        if (num < 0) {
            throw new IllegalArgumentException("illegal num:" + num);
        }
        int factorial = 1;
        for (int i = 1; i <= num; i++) {
            factorial *= num;
        }
        return factorial;
    }

    /**
     * @param m 总数
     * @param n 组合数
     * @return 获取组合数量
     */
    private static long combination(int m, int n) {
        if (n < m) {
            return 0;
        }
        return factorial(m) / (factorial(n) * factorial(m - n));
    }

    /**
     * 根据vnode获取其前缀以及vnode
     * @param key 例如:metaa123
     * @return var1:vnode前缀, var2:vnode数值
     */
    private static Tuple2<String, String> getPrefix(String key) {
        int index = 0;
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c >= '0' && c <= '9') {
                index = i;
                break;
            }
        }
        return new Tuple2<>(key.substring(0, index), key.substring(index));
    }
}