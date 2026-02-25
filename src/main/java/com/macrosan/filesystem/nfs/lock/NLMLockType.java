package com.macrosan.filesystem.nfs.lock;

public enum NLMLockType {
    //没有写锁的情况下，可以加读锁。加读锁后可以重复加读锁，加写锁会失败
    READ,
    //没有其他锁的情况下，可以加写锁。加写锁后可以其他加锁操作都会失败
    WRITE
}