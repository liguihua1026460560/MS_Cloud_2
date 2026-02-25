package com.macrosan.database.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.log4j.Log4j2;

import java.lang.management.ManagementFactory;
import java.util.concurrent.*;

import static com.macrosan.constants.RedisConstants.*;

/**
 * 本地读写锁，读锁部分
 */
@Log4j2
public class ReadWriteLock {
    public static final String READ_LOCK_PREFIX = "read_lock_key";// 读锁存在多个
    private static final String WRITE_LOCK = "write_lock_key"; // 写锁存在一个
    private static final String READ_LOCK_DELIMITER = "@"; // 读锁Key分隔符
    private static final String PROCESS_ID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0]; // 进程ID
    private static final int LOCK_TIMEOUT = 30; // 锁超时自动释放，单位秒
    private static final int RENEWAL_INTERVAL = 10; // 锁续约间隔（每隔一段时间重置过期时间），单位秒
    public static final RedisClient client = RedisClient.create();
    public static RedisCommands<String, String> sync;
    private static final ConcurrentHashMap<String, ScheduledFuture<?>> RENEWAL_MAP = new ConcurrentHashMap<>(); // 用于锁续约
    private static final ScheduledExecutorService RENEWAL_EXECUTOR = Executors.newScheduledThreadPool(1);

    private static final String RENEWAL_SCRIPT =
                    "if redis.call('GET', KEYS[1]) == '1' then \n" +
                    "   return redis.call('EXPIRE', KEYS[1], ARGV[1]) \n" +
                    "else \n" +
                    "   return 0 \n" +
                    "end";

    // 读锁可多个进程的线程同时获取，需区分
    public static String getReadKey() {
        long threadId = Thread.currentThread().getId();
        return String.join(READ_LOCK_DELIMITER, READ_LOCK_PREFIX, PROCESS_ID, String.valueOf(threadId));
    }

    public static RedisCommands<String, String> getRedis6380() {
        return client.connect(RedisURI.Builder.socket(IAM_UDS_ADDRESS)
                .withHost(LOCALHOST)
                .withPassword(REDIS_PASSWD)
                .withPort(6380)
                .build()).sync();
    }

    public static void unLock(boolean flag) {
        //使用连接池复用连接
        if (flag) {
            sync = Redis6380ConnPool.getInstance().getCommand(0);
        }
        //使用短连接时判断当前连接是否关闭
        if (!flag && !sync.getStatefulConnection().isOpen()) {
            sync = getRedis6380();
        }
        String unlockScript =
                "local lockKey = KEYS[1]\n" +
                        "if redis.call('exists',lockKey) ~= 0 then\n" +
                        "   redis.call('DEL', lockKey)\n" +
                        "end    \n" +
                        "return  1";
        try {
            String result;
            String lockKey = getReadKey();
            stopRenewal(lockKey); // 暂停续约线程
            Object res = sync.eval(unlockScript, ScriptOutputType.INTEGER, lockKey);
            if (res instanceof Long || res instanceof Integer) {
                int counter = ((Number) res).intValue();
                result = String.valueOf(counter);
            } else {
                result = (String) res;
            }
//            log.info("release " + readKey + ",res: " + result);
        } catch (Exception e) {
            log.error("releaseLock Exception:" + e);
        }
    }

    public static boolean readLock(boolean flag) {
        //使用复用连接或者短连接
        if (flag) {
            sync = Redis6380ConnPool.getInstance().getCommand(0);
        } else {
            sync = getRedis6380();
        }
        String readLockScript =
                "local readKey ,writeKey,timeout = KEYS[1],KEYS[2],ARGV[1] \n" +
                        "local wExist = redis.call('EXISTS',writeKey) \n" +
                        "if wExist == 1 then  \n" +
                        "   return 0 \n" +
                        "else    \n" +
                        "   redis.call('SET',readKey,'1','EX',timeout) \n" +
                        "end \n" +
                        "return 1";

        long result;
        boolean isAcquireReadLock = false;
        while (!isAcquireReadLock) {
            try {
                String lockKey = getReadKey();
                result = sync.eval(readLockScript, ScriptOutputType.INTEGER, new String[]{lockKey, WRITE_LOCK}, String.valueOf(LOCK_TIMEOUT));
                if (1L == result) {
                    isAcquireReadLock = true;
                    startRenewal(sync, lockKey); // 开启续约任务
                } else if (0L == result) {
                    log.info("acquire_read_lock wait write_lock release.");
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                log.error("acquireReadLock Exception:" + e);
                break;
            }
        }
        if (!isAcquireReadLock) {
            log.info("***acquireWriteLock failed*****");
        }
        return isAcquireReadLock;
    }

    public static void release() {
        //使用短连接时，解锁后立即释放连接
        try {
            sync.getStatefulConnection().close();
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        } finally {
            sync.getStatefulConnection().close();
        }

    }

    /**
     * 加锁后，开启锁续约
     */
    public static void startRenewal(RedisCommands<String, String> sync, String lockKey) {
//        log.info("renewal: " + lockKey);
        String taskKey = lockKey + ":renewal";
        if (RENEWAL_MAP.containsKey(taskKey)) {
            return; // 已存在续约任务
        }

        // ServerConfig在很开始的地方就需要用读写锁，使用DISK_SCHEDULER时，还未初始化完成
        ScheduledFuture<?> schedule = RENEWAL_EXECUTOR.schedule(
                () -> {
                    try {
                        Integer result = sync.eval(RENEWAL_SCRIPT, ScriptOutputType.INTEGER, new String[]{lockKey}, String.valueOf(LOCK_TIMEOUT));

                        if (result == null || result == 0) {
                            stopRenewal(lockKey); // 自动停止无效续约
                        }
                    } catch (Exception e) {
                        log.error("Read lock renewal failed for key: " + lockKey, e);
                    }
                },
                RENEWAL_INTERVAL,
                TimeUnit.SECONDS
        );

        RENEWAL_MAP.put(taskKey, schedule);
    }

    /**
     * 释放锁前，停止锁续约
     */
    public static void stopRenewal(String lockKey) {
        String taskKey = lockKey + ":renewal";
        ScheduledFuture<?> task = RENEWAL_MAP.remove(taskKey);
        if (task != null) {
            task.cancel(false);
        }
    }
}
