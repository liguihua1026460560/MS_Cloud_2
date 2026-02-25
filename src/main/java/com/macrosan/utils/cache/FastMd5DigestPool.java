package com.macrosan.utils.cache;

import com.macrosan.utils.msutils.md5.Digest;
import com.macrosan.utils.msutils.md5.FastMd5Digest;
import com.macrosan.utils.msutils.md5.Md5Digest;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.concurrent.TimeUnit;

/**
 * @author zhaoyang
 * @date 2025/10/22
 **/
@Log4j2
public class FastMd5DigestPool {
    @Getter
    private final static FastMd5DigestPool instance;

    static {
        instance = new FastMd5DigestPool(512, 256, 128);
    }

    private final GenericObjectPool<Digest> pool;

    public FastMd5DigestPool(int maxTotal) {
        this(maxTotal, maxTotal, 0);
    }

    public FastMd5DigestPool(int maxTotal, int maxIdle, int minIdle) {
        GenericObjectPoolConfig<Digest> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);
        config.setBlockWhenExhausted(false);
        config.setTimeBetweenEvictionRunsMillis(60000);
        config.setMinEvictableIdleTimeMillis(300000);
        config.setNumTestsPerEvictionRun(3);

        this.pool = new GenericObjectPool<>(new FastMd5DigestPool.DigestFactory(), config);

        // 预创建对象
        if (minIdle > 0) {
            try {
                pool.addObjects(minIdle);
            } catch (Exception e) {
                throw new RuntimeException("preCreate digest fail", e);
            }
        }
    }

    /**
     * 借用 byte 数组
     */
    public Digest borrow() throws Exception {
        return pool.borrowObject();
    }

    /**
     * 尝试借用 byte 数组（带超时）
     */
    public Digest borrow(long timeout, TimeUnit unit) throws Exception {
        return pool.borrowObject(unit.toMillis(timeout));
    }

    /**
     * 归还 byte 数组
     */
    public void returnDigest(Digest digest) {
        if (digest != null) {
            try {
                pool.returnObject(digest);
            } catch (Exception e) {
                log.error("returnDigest fail", e);
            }
        }
    }

    /**
     * 使 对象 在池中失效，不会再被其他线程所使用
     */
    public void invalidateDigest(Digest digest) {
        if (digest != null) {
            try {
                pool.invalidateObject(digest);
            } catch (Exception e) {
                log.error("invalidateDigest fail", e);
            }
        }
    }

    private static class DigestFactory extends BasePooledObjectFactory<Digest> {


        @Override
        public Digest create() throws Exception {
            return new FastMd5Digest();
        }

        @Override
        public PooledObject<Digest> wrap(Digest digest) {
            return new DefaultPooledObject<>(digest);
        }

        @Override
        public boolean validateObject(PooledObject<Digest> digestPooledObject) {
            return digestPooledObject != null;
        }

        @Override
        public void passivateObject(PooledObject<Digest> digestPooledObject) throws Exception {
            digestPooledObject.getObject().reset();
        }
    }
}
