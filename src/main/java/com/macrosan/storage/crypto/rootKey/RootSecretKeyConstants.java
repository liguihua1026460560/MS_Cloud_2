package com.macrosan.storage.crypto.rootKey;

import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.extern.log4j.Log4j2;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ThreadFactory;

/**
 * @Description: 根密钥相关常量
 * @Author wanhao
 * @Date 2023/4/6 0006 下午 1:56
 */
@Log4j2
public class RootSecretKeyConstants {
    static final Scheduler CRYPTO_SCHEDULER;
    private final static ThreadFactory CRYPTO_THREAD_FACTORY = new MsThreadFactory("crypto");

    /**
     * 密钥文件的类型
     */
    static final String KEY_STORE_TYPE = "JCEKS";

    /**
     * 生成根密钥使用的算法
     */
    static final String ROOT_KEY_ALGORTHM = "AES256";

    /**
     * redis表2 记录最新根密钥的创建时间，轮换周期，密钥文件的密码等
     */
    static final String REDIS_SECRET_KEY = "encrypt_root_key";

    /**
     * 节点更新根密钥时使用的锁，保证同时只有一个节点在更新根密钥
     */
    static final String ROOT_LOCK = "encrypt_root_key_lock";

    /**
     * 最大轮换周期为730天，最小轮换周期为7天，默认365天
     */
    static final long DEFAULT_ROTATION_INTERVAL = 1000L * 60 * 60 * 24 * 365;
    static final long DEFAULT_MAX_ROTATION_INTERVAL = 1000L * 60 * 60 * 24 * 730;
    static final long DEFAULT_MIN_ROTATION_INTERVAL = 1000L * 60 * 60 * 24 * 7;

    static final String SECRET_KEY_FILE_PARENT_PATH = "/cg_sp0/crypto/";
    static final String SECRET_KEY_FILE_NAME = "root_secret_key.keystore";
    static final String BACKUP_SECRET_KEY_FILE_NAME = "backup_root_secret_key.keystore";
    static final String SECRET_KEY_FILE_PATH = SECRET_KEY_FILE_PARENT_PATH + SECRET_KEY_FILE_NAME;
    static final String BACKUP_SECRET_KEY_FILE_PATH = SECRET_KEY_FILE_PARENT_PATH + BACKUP_SECRET_KEY_FILE_NAME;


    static {
        Scheduler scheduler = null;
        try {
            MsExecutor executor = new MsExecutor(4, 1, CRYPTO_THREAD_FACTORY);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }
        CRYPTO_SCHEDULER = scheduler;
    }
}
