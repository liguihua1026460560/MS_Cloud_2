package com.macrosan.storage;

import com.macrosan.ServerStart;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.rebuild.RemovedDisk;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisException;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.REDIS_MAPINFO_INDEX;
import static com.macrosan.ec.Utils.getMqRocksKey;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class VnodeCache {
    private Map<String, Map<String, String>> cache = new ConcurrentHashMap<>();

    public Set<String> lunSet = ConcurrentHashMap.newKeySet();

    public long size;
    public long totalSize;
    public long firstPartCapacity;
    public long firstPartUsedSize;

    private void tryLoadCache(List<String> vnodeList, List<String> mapVnodeList) {
        try (StatefulRedisConnection<String, String> tmpConnection =
                     RedisConnPool.getInstance().getSharedConnection(REDIS_MAPINFO_INDEX).newMaster()) {
            RedisCommands<String, String> commands = tmpConnection.sync();
            commands.multi();
            for (String vnode : vnodeList) {
                commands.hgetall(vnode);
            }

            if (mapVnodeList != null) {
                for (String vnode : mapVnodeList) {
                    commands.hgetall(vnode);
                }
            }

            TransactionResult result = commands.exec();
            for (int i = 0; i < vnodeList.size(); i++) {
                Map<String, String> value = result.get(i);
                cache.put(vnodeList.get(i), new ConcurrentHashMap<>(value));
                String lun = value.get("s_uuid") + '@' + value.get("lun_name");
                lunSet.add(lun);
            }

            if (mapVnodeList != null) {
                for (int i = 0; i < mapVnodeList.size(); i++) {
                    int n = vnodeList.size() + i;
                    Map<String, String> value = result.get(n);
                    cache.put(mapVnodeList.get(i), new ConcurrentHashMap<>(value));
                    String lun = value.get("s_uuid") + '@' + value.get("lun_name");
                    lunSet.add(lun);
                }
            }
        }
    }

    StoragePool pool;

    public VnodeCache(StoragePool pool, List<String> vnodeList, List<String> mapVnodeList) {
        this.pool  = pool;
        if (ServerStart.futures != null) {
            for (String key : vnodeList) {
                try {
                    final Map<String, String> value = ServerStart.futures.get(key).get();
                    cache.put(key, new ConcurrentHashMap<>(value));
                    String lun = value.get("s_uuid") + '@' + value.get("lun_name");
                    lunSet.add(lun);
                } catch (Exception e) {
                   log.error("init vnodeList error.", e);
                }
            }

            if (mapVnodeList != null) {
                for (String key : mapVnodeList) {
                    try {
                        final Map<String, String> value = ServerStart.futures.get(key).get();
                        cache.put(key, new ConcurrentHashMap<>(value));
                        String lun = value.get("s_uuid") + '@' + value.get("lun_name");
                        lunSet.add(lun);
                    } catch (Exception e) {
                        log.error("init mapVnodeList error.", e);
                    }
                }
            }
        } else {
            while (true) {
                try {
                    tryLoadCache(vnodeList, mapVnodeList);
                    break;
                } catch (RedisException e) {
                    log.error("try load cache fail {}. retry later", e.getMessage());
                    synchronized (Thread.currentThread()) {
                        try {
                            Thread.currentThread().wait(Math.abs(new Random().nextInt(10_000)) + 1);
                        } catch (Exception e0) {

                        }
                    }
                }
            }
        }

        getSize();

        log.info("init vnode cache {}", cache.size());
    }

    private void getSize() {
        MSRocksDB db = MSRocksDB.getRocksDB(getMqRocksKey());
        long size = 0L;
        long totalSize = 0L;
        long totFirstPartCapacity = 0L;
        long totFirstPartUsedSize = 0L;
        Iterator<String> iterator = lunSet.iterator();
        while (iterator.hasNext()) {
            try {
                String lun = iterator.next();
                if (RemovedDisk.getInstance().contains(lun)) {
                    db.delete(lun.getBytes());
                    String totalKey = "total_" + lun;
                    db.delete(totalKey.getBytes());
                    // 同时一并删除第一分区的相关信息
                    db.delete(("total_firstPart_" + lun).getBytes());
                    db.delete(("firstPart_" + lun).getBytes());
                    iterator.remove();
                    continue;
                }
                byte[] value = Optional.ofNullable(db.get(("total_" + lun).getBytes())).orElse(new byte[8]);
                totalSize += ECUtils.bytes2long(value);
                value =  Optional.ofNullable(db.get(lun.getBytes())).orElse(new byte[8]);
                size += ECUtils.bytes2long(value);
                // 添加第一分区的总容量和已使用容量
                long firstPartCapacity = ECUtils.bytes2long(Optional.ofNullable(db.get(("total_firstPart_" + lun).getBytes())).orElse(new byte[8]));
                long firstPartUsed = ECUtils.bytes2long(Optional.ofNullable(db.get(("firstPart_" + lun).getBytes())).orElse(new byte[8]));
                totFirstPartCapacity += firstPartCapacity;
                totFirstPartUsedSize += firstPartUsed;
            } catch (Exception e) {

            }
        }

        this.size = size;
        this.totalSize = totalSize;
        this.firstPartCapacity = totFirstPartCapacity;
        this.firstPartUsedSize = totFirstPartUsedSize;

        DISK_SCHEDULER.schedule(this::getSize, 10, TimeUnit.SECONDS);
    }

    public Mono<Map<String, String>> getVnodeInfo(String vnodeNum) {
        return Mono.just(cache.getOrDefault(vnodeNum, new ConcurrentHashMap<>(0)));
    }

    public Mono<String> hgetVnodeInfo(String vnodeNum, String key) {
        if (cache.containsKey(vnodeNum) && cache.get(vnodeNum).containsKey(key)) {
            return Mono.just(cache.get(vnodeNum).get(key));
        }

        return Mono.empty();
    }

    public Flux<KeyValue<String, String>> hmgetVnodeInfo(String vnodeNum, String... keys) {
        KeyValue<String, String>[] keyValues = new KeyValue[keys.length];
        Map<String, String> vnodeInfo = cache.getOrDefault(vnodeNum, new ConcurrentHashMap<>(0));
        for (int i = 0; i < keys.length; i++) {
            String value = vnodeInfo.getOrDefault(keys[i], "");
            keyValues[i] = KeyValue.just(keys[i], value);
        }

        return Flux.fromArray(keyValues);
    }

    public String hget(String vnodeNum, String key) {
        if (cache.containsKey(vnodeNum) && cache.get(vnodeNum).containsKey(key)) {
            return cache.get(vnodeNum).get(key);
        }

        return null;
    }

    public void updateVnodeDisk(String vnodeNum, String disk, String vnode) {
        Map<String, String> map = cache.get(vnodeNum);
        map.put("lun_name", disk);
        lunSet.add(map.get("s_uuid") + '@' + disk);
        pool.removeNodeInfoCache(vnode);
    }

    public void updateVnodeNode(String vnodeNum, String node, String disk, String vnode) {
        Map<String, String> map = cache.get(vnodeNum);
        Map<String, String> newMap = new ConcurrentHashMap<>(map);
        newMap.put("lun_name", disk);
        newMap.put("s_uuid", node);
        cache.put(vnodeNum, newMap);
        lunSet.add(newMap.get("s_uuid") + '@' + disk);
        pool.removeNodeInfoCache(vnode);
    }

    public Map<String, Map<String, String>> getCache() {
        return cache;
    }
}
