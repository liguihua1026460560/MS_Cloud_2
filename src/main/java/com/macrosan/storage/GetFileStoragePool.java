package com.macrosan.storage;

import java.util.LinkedList;

import static com.macrosan.storage.StoragePoolFactory.DEFAULT_DIVISION_SIZE;
import static com.macrosan.storage.StoragePoolFactory.DEFAULT_PACKAGE_SIZE;

public final class GetFileStoragePool extends StoragePool {
    private static final GetFileStoragePool instance = new GetFileStoragePool();

    private GetFileStoragePool() {
        super("GetFile", StoragePoolType.REPLICA, 1, 0, DEFAULT_PACKAGE_SIZE, DEFAULT_DIVISION_SIZE,
                new LinkedList<>(), null, null);
    }

    public static GetFileStoragePool getInstance() {
        return instance;
    }
}
