package com.macrosan.filesystem.cifs.notify;

import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.extern.log4j.Log4j2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.filesystem.lock.LockKeeper.KEEP_NAN;

@Log4j2
public class NotifyCache {
    private static final AtomicLong localAsyncID = new AtomicLong(1);

    private static final Map<Long, Long> msgToAsyncMap = new HashMap<>();
    private static final Map<Long, Long> asyncToMsgMap = new HashMap<>();
    private static final Map<Long, NotifyCache> cache = new HashMap<>();
    private static final Map<Long, Long> cancel = new ConcurrentHashMap<>();

    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("NotifyCache-"));

    static {
        executor.submit(NotifyCache::clearCancel);
    }

    public static long acquireAsyncID() {
        return localAsyncID.incrementAndGet();
    }

    public static void addCache(long asyncID, long msgID, String bucket, String key, SMBHandler handler,
                                NotifyLock notify, SMB2Header replyHeader) {
        msgToAsyncMap.put(msgID, asyncID);
        asyncToMsgMap.put(asyncID, msgID);
        cache.put(asyncID, new NotifyCache(bucket, key, handler, notify, replyHeader));
    }

    public static void addCancel(long msgID) {
        cancel.put(msgID, System.nanoTime());
    }

    public static boolean isCancel(long msgID) {
        return cancel.containsKey(msgID);
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

    private static void clearCancel() {
        long start = System.nanoTime();
        try {
            Iterator<Map.Entry<Long, Long>> iterator = cancel.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, Long> entry = iterator.next();
                if (System.nanoTime() - entry.getValue() > 5 * KEEP_NAN) {
                    iterator.remove();
                    cancel.remove(entry.getKey());
                }
            }
        } catch (Exception e) {
            log.error(e);
        }

        long exec = KEEP_NAN - (System.nanoTime() - start);
        if (exec < 0) {
            exec = 0;
        }
        executor.schedule(NotifyCache::clearCancel, exec, TimeUnit.NANOSECONDS);
    }

    public static String info() {
        String str = "";
        try {
            str = "\nmsgToAsyncMap->" + msgToAsyncMap +
                    "\nasyncToMsgMap->" + asyncToMsgMap +
                    "\ncache->" + cache +
                    "\ncancel->" + cancel;
        } catch (Exception e) {
            log.error("CIFS NotifyCache info: ", e);
        }
        return str;
    }
}
