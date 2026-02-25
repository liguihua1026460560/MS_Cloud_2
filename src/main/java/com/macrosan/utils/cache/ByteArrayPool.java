package com.macrosan.utils.cache;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author zhaoyang
 * @date 2025/10/22
 **/
@Log4j2
public class ByteArrayPool {
    private final GenericObjectPool<byte[]> pool;
    
    public ByteArrayPool(int arraySize, int maxTotal) {
        this(arraySize, maxTotal, maxTotal, 0);
    }
    
    public ByteArrayPool(int arraySize, int maxTotal, int maxIdle, int minIdle) {
        GenericObjectPoolConfig<byte[]> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);
        config.setBlockWhenExhausted(false);
        config.setTimeBetweenEvictionRunsMillis(60000);
        config.setMinEvictableIdleTimeMillis(300000);
        config.setNumTestsPerEvictionRun(3);
        
        this.pool = new GenericObjectPool<>(new ByteArrayFactory(arraySize), config);
        
        // 预创建对象
        if (minIdle > 0) {
            try {
                pool.addObjects(minIdle);
            } catch (Exception e) {
                throw new RuntimeException("预创建对象失败", e);
            }
        }
    }

    public byte[] borrowArray() throws Exception {
        return pool.borrowObject();
    }
    

    public byte[] borrowArray(long timeout, TimeUnit unit) throws Exception {
        return pool.borrowObject(unit.toMillis(timeout));
    }
    
    /**
     * 归还 byte 数组
     */
    public void returnArray(byte[] array) {
        if (array != null) {
            try {
                pool.returnObject(array);
            } catch (Exception e) {
                log.error("returnDigest fail", e);
            }
        }
    }



    private static class ByteArrayFactory extends BasePooledObjectFactory<byte[]> {
        private final int arraySize;
        
        public ByteArrayFactory(int arraySize) {
            this.arraySize = arraySize;
        }
        
        @Override
        public byte[] create() throws Exception {
            return new byte[arraySize];
        }
        
        @Override
        public PooledObject<byte[]> wrap(byte[] buffer) {
            return new DefaultPooledObject<>(buffer);
        }
        
        @Override
        public boolean validateObject(PooledObject<byte[]> p) {
            byte[] array = p.getObject();
            return array != null && array.length == arraySize;
        }
        
        @Override
        public void passivateObject(PooledObject<byte[]> p) throws Exception {
            // 对象归还到池时的清理操作
            Arrays.fill(p.getObject(), (byte) 0);
        }
    }
}