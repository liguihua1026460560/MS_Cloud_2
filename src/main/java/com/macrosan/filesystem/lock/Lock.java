package com.macrosan.filesystem.lock;

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

public abstract class Lock {
    private static final IntObjectHashMap<LockServer<? extends Lock>> LOCK_SERVER_MAP = new IntObjectHashMap<>();
    private static final ObjectIntHashMap<Class<? extends Lock>> LOCK_TYPE_MAP = new ObjectIntHashMap<>();

    public static final int RED_LOCK_TYPE = 1;
    public static final int NOTIFY_TYPE = 2;
    public static final int LEASE_TYPE = 3;
    public static final int SHARE_ACCESS_TYPE = 4;
    public static final int CIFS_LOCK_TYPE = 5;
    public static final int NFS4_LOCK_TYPE = 6;
    public static final int NFS4_SHARE_ACCESS_TYPE = 7;
    public static final int NFS4_DELEGATE_TYPE = 8;

    public static <T extends Lock> void register(int type, LockServer<T> server, Class<T> tClass) {
        LOCK_TYPE_MAP.put(tClass, type);
        LOCK_SERVER_MAP.put(type, server);
    }

    static LockServer<? extends Lock> getServer(int type) {
        return LOCK_SERVER_MAP.get(type);
    }

    static int getType(Class<? extends Lock> tClass) {
        return LOCK_TYPE_MAP.get(tClass);
    }

    long keepNum;

    public abstract boolean needKeep();
}
