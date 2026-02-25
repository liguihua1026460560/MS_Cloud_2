package com.macrosan.filesystem.cache;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.Inode;
import reactor.core.publisher.Mono;

import java.util.List;

public class LookupCache {
    private static Cache<String, Long> cache = new Cache<>(100000, ErasureServer.DISK_SCHEDULER);

    private static Mono<Inode> cacheLookup(String cacheKey, String bucket, String objName, ReqInfo reqHeader, boolean caseSensitive, boolean isReName, long dirInode) {
        return FsUtils.lookup(bucket, objName, reqHeader, caseSensitive, isReName, dirInode, null)
                .doOnNext(i -> {
                    if (!InodeUtils.isError(i)) {
                        cache.put(cacheKey, i.getNodeId());
                    }
                });
    }

//    public static Mono<Inode> lookup(String bucket, String objName, ReqInfo reqHeader, boolean caseSensitive, boolean isReName, long dirInode) {
//        String cacheKey = bucket + '/' + objName + '/' + dirInode;
//        Long cachedNodeId = cache.get(cacheKey);
//        if (cachedNodeId != null) {
//            return Node.getInstance().getInode(bucket, cachedNodeId)
//                    .flatMap(inode -> {
//                        if (!InodeUtils.isError(inode) && cachedNodeId == inode.getNodeId()) {
//                            return Mono.just(inode);
//                        } else {
//                            cache.remove(cacheKey);
//                            return cacheLookup(cacheKey, bucket, objName, reqHeader, caseSensitive, isReName, dirInode);
//                        }
//                    });
//        } else {
//            return cacheLookup(cacheKey, bucket, objName, reqHeader, caseSensitive, isReName, dirInode);
//        }
//    }

    public static Mono<Inode> lookup(String bucket, String objName, ReqInfo reqHeader, boolean caseSensitive, boolean isReName, long dirInode, List<Inode.ACE> dirACEs) {
        return FsUtils.lookup(bucket, objName, reqHeader, caseSensitive, isReName, dirInode, dirACEs);
    }
}
