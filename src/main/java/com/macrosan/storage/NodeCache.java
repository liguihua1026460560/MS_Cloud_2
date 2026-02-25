package com.macrosan.storage;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.utils.functional.Tuple3;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.SysConstants.HEART_ETH1;
import static com.macrosan.constants.SysConstants.REDIS_NODEINFO_INDEX;

public class NodeCache {
    private static Map<String, Map<String, String>> cache = new ConcurrentHashMap<>();
    private static Map<String, String> iPMap = new ConcurrentHashMap<>();
    private static AtomicBoolean isInit = new AtomicBoolean();

    public synchronized static void init() {
        if (isInit.compareAndSet(false, true)) {
            RedisCommands<String, String> commands = RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX);
            for (String uuid : commands.keys("*")) {
                Map<String, String> map = commands.hgetall(uuid);
                Map<String, String> m = new ConcurrentHashMap<>(map);
                cache.put(uuid, m);
                iPMap.put(map.get(HEART_ETH1), uuid);
            }
        }
    }

    public static String mapIPToNode(String ip) {
        return iPMap.get(ip);
    }

    public static String getIP(String uuid) {
        Map<String, String> map = cache.get(uuid);
        return map.get(HEART_ETH1);
    }

    public static void add(String uuid) {
        if (cache.get(uuid) == null) {
            Map<String, String> map = RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hgetall(uuid);
            Map<String, String> m = new ConcurrentHashMap<>(map);
            cache.put(uuid, m);
            iPMap.put(map.get(HEART_ETH1), uuid);
        }
    }

    public static void remove(String uuid) {
        if (cache.get(uuid) != null) {
            String heartIp = RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(uuid, HEART_ETH1);
            cache.remove(uuid);
            iPMap.remove(heartIp, uuid);
        }
    }

    public static Map<String, Map<String, String>> getCache() {
        return cache;
    }

    /**
     * 获取所有节点eth4 ip
     * @return list
     */
    public static List<Tuple3<String, String, String>> getAllNodeIp() {
        List<Tuple3<String, String, String>> nodeList = new LinkedList<>();
        for (Map.Entry<String, Map<String, String>> entry : cache.entrySet()) {
            Map<String, String> map = entry.getValue();
            nodeList.add(new Tuple3<>(map.get(HEART_ETH1), "", ""));
        }
        return nodeList;
    }
}
