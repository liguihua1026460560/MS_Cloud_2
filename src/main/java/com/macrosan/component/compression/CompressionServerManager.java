package com.macrosan.component.compression;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.redis.RedisSemaphore;
import lombok.*;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.component.ComponentStarter.COMP_TIMER;
import static com.macrosan.constants.SysConstants.REDIS_POOL_INDEX;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;

/**
 * @author zhaoyang
 * @date 2026/02/26
 * @description 压缩服务管理器，负责从Redis同步压缩服务列表，并定时检测服务健康状态
 **/
@Log4j2
public class CompressionServerManager {
    // 单例实例
    private static volatile CompressionServerManager instance;

    private static final RedisConnPool REDIS_POOL = RedisConnPool.getInstance();
    // Redis中存储压缩服务列表的key
    private static final String REDIS_COMPRESSION_SERVERS_KEY = "compression_server_set";

    // 所有服务列表
    private final List<ServerInfo> allServers = new CopyOnWriteArrayList<>();

    // 轮询计数器
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    private ScheduledExecutorService scheduler;

    public static final String MODULE_NAME = "component_media";

    /**
     * 获取单例实例
     */
    public static CompressionServerManager getInstance() {
        if (instance == null) {
            synchronized (CompressionServerManager.class) {
                if (instance == null) {
                    instance = new CompressionServerManager();
                }
            }
        }
        return instance;
    }

    public void init() {
        syncFromRedis();
        startScheduler();
    }

    /**
     * 启动定时任务
     */
    private void startScheduler() {

        scheduler = COMP_TIMER;

        // 定时同步Redis（每30秒）
        scheduler.scheduleAtFixedRate(this::syncFromRedis, 30, 30, TimeUnit.SECONDS);

        // 定时健康检查（每60秒）
        scheduler.scheduleAtFixedRate(this::healthCheck, 60, 60, TimeUnit.SECONDS);

        log.info("CompressionServerManager scheduler started");
    }


    /**
     * 从Redis同步服务列表
     */
    private void syncFromRedis() {
        log.debug("Syncing compression servers from Redis");
        try {
            List<String> serverSet = REDIS_POOL.getShortMasterCommand(REDIS_POOL_INDEX).hkeys(REDIS_COMPRESSION_SERVERS_KEY);
            if (serverSet == null || serverSet.isEmpty()) {
                return;
            }
            Map<String, ServerInfo> redisServers = serverSet.stream()
                    .map(address -> {
                        String[] parts = address.split(":");
                        if (parts.length == 2) {
                            String ip = parts[0];
                            int port = Integer.parseInt(parts[1]);
                            String maxConcurrent = REDIS_POOL.getShortMasterCommand(REDIS_POOL_INDEX).hget(REDIS_COMPRESSION_SERVERS_KEY, address);
                            int concurrent = StringUtils.isEmpty(maxConcurrent) ? 30 : Integer.parseInt(maxConcurrent);
                            return new ServerInfo(ip, port, concurrent);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(ServerInfo::getAddress, server -> server));

            synchronized (this) {
                List<ServerInfo> toAdd = redisServers.values().stream()
                        .filter(redisServer -> !allServers.contains(redisServer))
                        .collect(Collectors.toList());

                List<ServerInfo> toRemove = allServers.stream()
                        .filter(server -> !redisServers.containsKey(server.getAddress()))
                        .collect(Collectors.toList());

                toAdd.forEach(server -> {
                    server.initSemaphore();
                    allServers.add(server);
                    log.info("Added new server from Redis: {} maxConcurrent:{}", server.getAddress(), server.getMaxConcurrent());
                });

                toRemove.forEach(server -> {
                    allServers.remove(server);
                    log.info("Removed server not in Redis: {} maxConcurrent:{}", server.getAddress(), server.getMaxConcurrent());
                });
                // Update maxConcurrent for all servers
                for (ServerInfo server : allServers) {
                    if (server.getMaxConcurrent() != redisServers.get(server.getAddress()).getMaxConcurrent()) {
                        server.updateMaxConcurrent(redisServers.get(server.getAddress()).getMaxConcurrent());
                        log.info("Updated maxConcurrent for {} from {} to {}", server.getAddress(), server.getMaxConcurrent(), redisServers.get(server.getAddress()).getMaxConcurrent());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to sync compression servers from Redis", e);
        }
    }

    /**
     * 获取可用服务（优先本地）
     */
    public AcquiredServer getAvailableServer() {
        // 本地优先
        ServerInfo local = getAvailableLocalServer();
        if (local != null) {
            String permitId = local.tryAcquire();
            if (permitId != null) {
                return new AcquiredServer(local, permitId);
            }
        }
        List<ServerInfo> availableRemotes = allServers.stream()
                .filter(s -> !s.getIp().equals(CURRENT_IP) && s.isAvailable())
                .collect(Collectors.toList());
        for (int i = 0; i < availableRemotes.size(); i++) {
            int index = roundRobinCounter.getAndIncrement() % availableRemotes.size();
            ServerInfo server = availableRemotes.get(index);
            String permitId = server.tryAcquire();
            if (permitId != null) {
                return new AcquiredServer(server, permitId);
            }
        }
        return null;
    }

    /**
     * 获取可用本地服务
     */
    private ServerInfo getAvailableLocalServer() {
        return allServers.stream()
                .filter(s -> s.getIp().equals(CURRENT_IP) && s.isAvailable())
                .findFirst()
                .orElse(null);
    }


    /**
     * 定时健康检查
     */
    private void healthCheck() {
        log.debug("Health check started");
        // 检查所有服务
        allServers.forEach(this::checkServer);
        allServers.forEach(server -> log.debug("Server: {} available: {} maxConcurrent: {}", server.getAddress(), server.isAvailable(), server.getMaxConcurrent()));
    }

    private void checkServer(ServerInfo server) {
        boolean healthy = pingServer(server.getIp(), server.getPort());

        if (healthy) {
            if (!server.isAvailable()) {
                // 服务恢复
                server.markSuccess();
                log.info("Server recovered: {}", server.getAddress());
            } else {
                server.markSuccess();
            }
        } else {
            markServerUnavailable(server);
        }
    }

    private boolean pingServer(String ip, int port) {
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress(ip, port), 3000);
            return true;
        } catch (Exception e) {
            log.debug("Failed to ping server {}:{}", ip, port, e);
            return false;
        }
    }

    /**
     * 标记服务不可用
     */
    public void markServerUnavailable(ServerInfo server) {
        if (server.isAvailable()) {
            server.markFailed();
            if (!server.isAvailable()) {
                // 标记服务不可用
                log.warn("Server marked unavailable: {}", server.getAddress());
            } else {
                // 标记服务异常
                log.warn("Server marked failed: {}", server.getAddress());
            }
        }
    }


    /**
     * 获取所有压缩服务器总并发
     * @return 总并发
     */
    public int getAllServerTotalConcurrent() {
        return allServers.stream().filter(ServerInfo::isAvailable).mapToInt(ServerInfo::getMaxConcurrent).sum();
    }

    /**
     * 服务信息
     */
    @Data
    @AllArgsConstructor
    @EqualsAndHashCode(of = {"ip", "port"})
    public static class ServerInfo {
        private String ip;
        private int port;
        private long lastCheckTime;
        private int failCount;
        private boolean available = true;

        private static final long DEFAULT_LEASE_TIME = 3;

        private RedisSemaphore semaphore;

        private int maxConcurrent;

        public ServerInfo(String ip, int port, int maxConcurrent) {
            this.ip = ip;
            this.port = port;
            this.lastCheckTime = System.currentTimeMillis();
            this.maxConcurrent = maxConcurrent;
        }

        public void initSemaphore() {
            this.semaphore = new RedisSemaphore(getAddress(), maxConcurrent);
        }

        public String tryAcquire() {
            try {
                return semaphore.tryAcquire(DEFAULT_LEASE_TIME, TimeUnit.MINUTES);
            } catch (Exception e) {
                return null;
            }
        }

        public void release(String permitId) {
            if (permitId != null) {
                semaphore.release(permitId);
            }
        }

        public void updateMaxConcurrent(int newMaxConcurrent) {
            semaphore.updateMaxPermits(newMaxConcurrent);
            this.maxConcurrent = newMaxConcurrent;
        }

        public String getAddress() {
            return ip + ":" + port;
        }

        public void markFailed() {
            this.failCount++;
            this.lastCheckTime = System.currentTimeMillis();
            if (failCount >= 3) {
                this.available = false;
            }
        }

        public void markSuccess() {
            this.failCount = 0;
            this.available = true;
            this.lastCheckTime = System.currentTimeMillis();
        }

    }

    @Data
    @AllArgsConstructor
    public static class AcquiredServer {
        private ServerInfo server;
        private String permitId;

        public void release() {
            server.release(permitId);
        }
    }
}
