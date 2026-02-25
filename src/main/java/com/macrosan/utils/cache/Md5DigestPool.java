package com.macrosan.utils.cache;

import org.apache.commons.codec.digest.DigestUtils;

import java.security.MessageDigest;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Md5DigestPool {
    private static ConcurrentLinkedQueue<MessageDigest> pool = new ConcurrentLinkedQueue<MessageDigest>() {
        {
            for (int i = 0; i < 100; i++) {
                this.add(DigestUtils.getMd5Digest());
            }
        }
    };

    public static MessageDigest acquire() {
        MessageDigest res = pool.poll();
        if (res != null) {
            return res;
        } else {
            return DigestUtils.getMd5Digest();
        }
    }

    public static void release(MessageDigest digest) {
        pool.add(digest);
    }

}
