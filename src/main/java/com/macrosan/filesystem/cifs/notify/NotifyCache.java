package com.macrosan.filesystem.cifs.notify;

import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.handler.SMBHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class NotifyCache {
    private static final AtomicLong localAsyncID = new AtomicLong(1);

    private static final Map<Long, Long> msgToAsyncMap = new HashMap<>();
    private static final Map<Long, Long> asyncToMsgMap = new HashMap<>();
    private static final Map<Long, NotifyCache> cache = new HashMap<>();

    public static long acquireAsyncID() {
        return localAsyncID.incrementAndGet();
    }

    public static void addCache(long asyncID, long msgID, String bucket, String key, SMBHandler handler,
                                NotifyLock notify, SMB2Header replyHeader) {
        msgToAsyncMap.put(msgID, asyncID);
        asyncToMsgMap.put(asyncID, msgID);
        cache.put(asyncID, new NotifyCache(bucket, key, handler, notify, replyHeader));
    }

    public static NotifyCache clearByAsync(long asyncID) {
        synchronized (localAsyncID) {
            Long msgID = asyncToMsgMap.remove(asyncID);
            if (msgID != null) {
                msgToAsyncMap.remove(msgID);
            }
            return cache.remove(asyncID);
        }
    }

    public static NotifyCache clearByMsg(long msgID) {
        synchronized (localAsyncID) {
            Long asyncID = msgToAsyncMap.remove(msgID);
            if (asyncID != null) {
                asyncToMsgMap.remove(asyncID);
                return cache.remove(asyncID);
            }

            return null;
        }
    }

    public String bucket;
    public String key;
    public NotifyLock notify;
    public SMBHandler handler;
    public SMB2Header replyHeader;

    NotifyCache(String bucket, String key, SMBHandler handler, NotifyLock notify, SMB2Header replyHeader) {
        this.bucket = bucket;
        this.key = key;
        this.handler = handler;
        this.notify = notify;
        this.replyHeader = replyHeader;
    }
}
