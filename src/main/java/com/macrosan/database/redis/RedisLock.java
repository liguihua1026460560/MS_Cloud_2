package com.macrosan.database.redis;

import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.concurrent.TimeUnit;

/**
 * @author zhaoyang
 * @date 2024/06/25
 **/
public class RedisLock {

    public static final RedisConnPool POOL = RedisConnPool.getInstance();
    // 默认锁过期时间30s
    private static final int DEFAULT_EXPIRE_TIMEOUT = 30 * 1000;

    /**
     * 获取锁--不可重入锁
     *
     * @param dbIndex        数据库索引
     * @param lockKey        锁key
     * @param waitTime       等待时间
     * @param lockExpireTime 锁过期时间
     * @param timeUnit       时间单位
     * @param runnable       执行方法
     * @return 获取锁的结果
     */
    public static boolean tryLock(int dbIndex, String lockKey, long waitTime, long lockExpireTime, TimeUnit timeUnit, Runnable runnable) {
        String uuid = null;
        waitTime = timeUnit.toMillis(waitTime);
        lockExpireTime = timeUnit.toMillis(lockExpireTime);
        try {
            uuid = RandomStringUtils.randomAlphanumeric(8);
            long startTime = System.currentTimeMillis();
            // 至少尝试一次获取锁
            boolean first = true;
            long currentTime;
            while (true) {
                currentTime = System.currentTimeMillis();
                // 超时或时间回拨，则跳出循环
                if (!first && (currentTime - startTime > waitTime || currentTime < startTime)) {
                    return false;
                }
                first = false;
                String res = POOL.getShortMasterCommand(dbIndex).set(lockKey, uuid, new SetArgs().nx().ex(TimeUnit.MILLISECONDS.toSeconds(lockExpireTime)));
                if ("OK".equalsIgnoreCase(res)) {
                    runnable.run();
                    return true;
                }
                Thread.sleep(100);
            }
        } catch (Exception e) {
            if (e instanceof MsException) {
                throw new MsException(((MsException) e).getErrCode(), e.getMessage(), e);
            }
            throw new RuntimeException(e);
        } finally {
            if (uuid != null) {
                release(dbIndex, lockKey, uuid);
            }
        }
    }

    /**
     * 获取锁--获取失败则重试，直至超过waitTime
     * 使用默认锁过期时间30s
     *
     * @param dbIndex  数据库索引
     * @param lockKey  锁key
     * @param waitTime 等待时间--单位毫秒
     * @param runnable 执行方法
     * @return 获取锁的结果
     */
    public static boolean tryLock(int dbIndex, String lockKey, int waitTime, TimeUnit timeUnit, Runnable runnable) {
        return tryLock(dbIndex, lockKey, TimeUnit.MILLISECONDS.convert(waitTime, timeUnit), DEFAULT_EXPIRE_TIMEOUT, TimeUnit.MILLISECONDS, runnable);
    }

    /**
     * 获取锁---获取失败则直接返回false，不重试
     * 使用默认锁过期时间30s
     *
     * @param dbIndex  数据库索引
     * @param lockKey  锁key
     * @param runnable 执行方法
     * @return 获取锁的结果
     */
    public static boolean tryLock(int dbIndex, String lockKey, Runnable runnable) {
        return tryLock(dbIndex, lockKey, 0, DEFAULT_EXPIRE_TIMEOUT, TimeUnit.MILLISECONDS, runnable);
    }

    private static void release(int dbIndex, String lockKey, String uuid) {
        String script = "local result = redis.call('get', KEYS[1]);\n" +
                "if result == ARGV[1] then \n" +
                "  redis.call('del', KEYS[1])\n" +
                "  return 1 \n" +
                "else\n" +
                "  return nil \n" +
                "end";
        String[] keys = new String[]{lockKey};
        POOL.getShortMasterCommand(dbIndex).eval(script, ScriptOutputType.INTEGER, keys, uuid);
    }


}
