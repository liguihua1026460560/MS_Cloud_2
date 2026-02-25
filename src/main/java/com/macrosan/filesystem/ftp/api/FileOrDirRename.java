package com.macrosan.filesystem.ftp.api;


import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.ftp.Session;
import com.macrosan.filesystem.lock.redlock.LockType;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.ReadDirCache;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.essearch.EsMetaTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.SysConstants.ES_ON;
import static com.macrosan.constants.SysConstants.ES_SWITCH;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.utils.InodeUtils.isError;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;

@Slf4j
public class FileOrDirRename {

    private static final Node nodeInstance = Node.getInstance();

    // 参考 com.macrosan.filesystem.cifs.api.smb2.SetInfo.rename 实现
    public static Mono<Boolean> rename(String bucket, long nodeId, String oldObj, String newObj, boolean canOverWrite, Session session) {
        ReqInfo reqHeader = new ReqInfo();
        reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(bucket);
        reqHeader.bucket = bucket;
        reqHeader.nfsHandler = new NFSHandler();

        return RedLockClient.lock(reqHeader, oldObj, LockType.WRITE, true, true)
                .flatMap(lock -> {
                    session.lock = reqHeader.lock;
                    return nodeInstance.getInode(bucket, nodeId);
                }).flatMap(inode -> {
                    // 如果源文件不存在，则重命名失败
                    if (InodeUtils.isError(inode)) {
                        log.error("rename input/output error: obj: {}, nodeId: {}, linkN: {}", oldObj, nodeId, inode.getLinkN());
                        return Mono.just(false)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }

                    boolean isDir = StringUtils.isNotEmpty(inode.getObjName()) && inode.getObjName().endsWith("/");
                    AtomicBoolean overWrite = new AtomicBoolean(false);

                    String[] oldObjName0 = new String[1];
                    String[] newObjName0 = new String[1];

                    oldObjName0[0] = isDir ? oldObj + '/' : oldObj;
                    newObjName0[0] = isDir ? newObj + '/' : newObj;

                    return FsUtils.lookup(bucket, newObj, reqHeader, true, -1, null)
                            .flatMap(newInode -> {
                                // 目标inode存在
                                if (!InodeUtils.isError(newInode)) {
                                    //源为目录，目标文件存在
                                    if (inode.getObjName().endsWith("/")) {
                                        return Mono.just(false)
                                                .doOnNext(r -> releaseLock(reqHeader, r));
                                    }
                                    //如果源的名称和传入的oldObj不一致，说明是多客户端并发修改同一个源，此时源已重命名成功
                                    if (newInode.getNodeId() == inode.getNodeId() && newInode.getLinkN() == 1) {
                                        log.info("rename not found: obj: {}, nodeId: {}, linkN: {}", oldObj, nodeId, inode.getLinkN());
                                        return Mono.just(true)
                                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                    }
                                    overWrite.set(true);
                                }
                                // 如果源为目录
                                if (isDir) {
                                    // 如果更新后的目录名已经存在，则检查这个新的目录名之下是否还有与旧目录名称相同的dirInode
                                    return Mono.just(overWrite.get())
                                            .flatMap(isOverWrite -> {
                                                return FSQuotaUtils.canRename(bucket, nodeId, newInode)
                                                        .flatMap(can -> {
                                                            if (!can) {
                                                                return Mono.just(false)
                                                                        .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                            }
                                                            if (isOverWrite) {
                                                                String prefix = newInode.getObjName();
                                                                return ReadDirCache.listAndCache(bucket, prefix, 0, 4096, reqHeader.nfsHandler, newInode.getNodeId(), null, newInode.getACEs())
                                                                        .flatMap(list1 -> {
                                                                            if (!list1.isEmpty()) {
                                                                                log.error("directory already exists: {}", newObjName0[0]);
                                                                                return Mono.just(false)
                                                                                        .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                                            }
                                                                            return scanAndReName0(inode, bucket, newObjName0[0], oldObjName0[0], 0, reqHeader);
                                                                        });
                                                            } else {
                                                                // 将oldInode视为dirInode，遍历该目录下的所有子目录与文件
                                                                // 重命名时不需要再get新的inode，直接rename即可
                                                                return scanAndReName0(inode, bucket, newObjName0[0], oldObjName0[0], 0, reqHeader);
                                                            }
                                                        });
                                            });
                                    // 如果源是文件
                                } else {
                                    return renameFile(nodeId, newInode, oldObjName0[0], newObjName0[0], bucket, overWrite.get())
                                            .flatMap(i -> {
                                                if (InodeUtils.isError(i) || i.isDeleteMark()) {
                                                    if (i.getLinkN() == ERROR_INODE.getLinkN()) {
                                                        log.error("rename file nodeId: {}, obj: {} to newObj: {} fail, overwirte: {}, newInode: {}", nodeId, oldObjName0[0], newObjName0[0], overWrite.get(), newInode);
                                                    } else {
                                                    }

                                                    return Mono.just(false)
                                                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                                }

                                                return Flux.just(oldObjName0[0], newObjName0[0])
                                                        .flatMap(obj -> InodeUtils.findDirInode(obj, bucket))
                                                        .collectList()
                                                        .flatMap(dirInodes -> {
                                                            return updateTime(dirInodes, true);
                                                        })
                                                        .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                            });
                                }
                            });
                });
    }

    public static Mono<Boolean> releaseLock(ReqInfo reqHeader, Boolean b) {
        if (!reqHeader.lock.isEmpty()) {
            for (String key : reqHeader.lock.keySet()) {
                String value = reqHeader.lock.get(key);
                RedLockClient.unlock(reqHeader.bucket, key, value, true).subscribe();
            }
        }
        return Mono.just(b);
    }

    public static Mono<Boolean> scanAndReName0(Inode oldInode, String bucket, String newObjName, String oldObjName, long offset, ReqInfo reqHeader) {
        return scanAndReName(oldInode, bucket, newObjName, offset, reqHeader.nfsHandler)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        log.info("rename {}->{} fail: {}", oldInode.getObjName(), newObjName, inode.getLinkN());
                    }
                    return Flux.just(oldObjName, newObjName)
                            .flatMap(obj -> InodeUtils.findDirInode(obj, bucket))
                            .collectList()
                            .flatMap(dirInodes -> {
                                return updateTime(dirInodes, true);
                            })
                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                });
    }

    /**
     * rename完成后更新fromDirFh与toDirFh代表的父级目录的mtime和ctime
     **/
    public static Mono<Boolean> updateTime(List<Inode> inodeList, Boolean b) {
        // 如果两个inode的nodeId相等，只发送一次更改时间的请求
        if (inodeList.size() == 2 && !InodeUtils.isError(inodeList.get(0)) &&
                !InodeUtils.isError(inodeList.get(1)) && inodeList.get(0).getNodeId() == inodeList.get(1).getNodeId()) {
            return nodeInstance.updateInodeTime(inodeList.get(0).getNodeId(), inodeList.get(0).getBucket(), System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), false, true, true)
                    .map(inode -> b)
                    .doOnError(e -> log.info("update time error 1", e))
                    .onErrorReturn(b);
        } else {
            long stamp = System.currentTimeMillis() / 1000;
            int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
            return Flux.fromIterable(inodeList)
                    .flatMap(dirInode -> nodeInstance.updateInodeTime(dirInode.getNodeId(), dirInode.getBucket(), stamp, stampNano, false, true, true))
                    .collectList()
                    .map(list -> b)
                    .doOnError(e -> log.info("update time error 2", e))
                    .onErrorReturn(b);
        }
    }

    /**
     * @param dirInode   当前的目录的完整路径 /test0/fio/old/
     * @param newObjName 当前目录所要更改成的新目录名 /test0/fio/new/
     * @param bucket     桶名
     * @param count      offset
     **/
    public static Mono<Inode> scanAndReName(Inode dirInode, String bucket, String newObjName, long count, NFSHandler nfsHandler) {
        String prefix = dirInode.getObjName();
        AtomicLong offset = new AtomicLong(count);
        return ReadDirCache.listAndCache(bucket, prefix, offset.get(), 1 << 20, nfsHandler, dirInode.getNodeId(), null, dirInode.getACEs())
                .flatMap(list -> {
                    if (list.size() > 0) {
                        offset.set(list.get(list.size() - 1).getCookie());
                    }
                    return Mono.just(list);
                })
                .flatMapMany(Flux::fromIterable)
                .flatMap(inode -> {
                    String curOldName = inode.getObjName();
                    String[] nameArray = inode.getObjName().split("/");
                    String curNewName = newObjName + nameArray[nameArray.length - 1];
                    if ((inode.getMode() & S_IFMT) == S_IFDIR) {
                        // 如果是目录则进行递归
                        if (!curNewName.endsWith("/")) {
                            curNewName = curNewName + "/";
                        }
                        return scanAndReName(inode, bucket, curNewName, 0, nfsHandler);
                    } else {
                        // 如果是文件则进行重命名
                        return renameFile(inode.getNodeId(), null, curOldName, curNewName, bucket, false);
                    }
                })
                .collectList()
                .flatMap(list -> {
                    // 如果是非空的话则继续往下遍历；如果是空的话直接更改当前目录本身
                    return ReadDirCache.listAndCache(bucket, dirInode.getObjName(), offset.get(), 4096, nfsHandler, dirInode.getNodeId(), null, dirInode.getACEs())
                            .flatMap(list0 -> {
                                if (list0.isEmpty()) {
                                    return renameFile(dirInode.getNodeId(), null, dirInode.getObjName(), newObjName, bucket, false);
                                } else {
                                    return scanAndReName(dirInode, bucket, newObjName, offset.get(), nfsHandler);
                                }
                            });
                })
                .onErrorReturn(ERROR_INODE);
    }

    public static Mono<Inode> renameFile(long oldNodeId, Inode newInode, String oldObjName, String newObjName, String bucket, boolean isOverWrite) {
        boolean esSwitch = ES_ON.equals(NFSBucketInfo.getBucketInfo(bucket).get(ES_SWITCH));
        return Mono.just(isOverWrite)
                .flatMap(b -> {
                    if (b) {
                        // mv a.txt b.txt，如果b.txt文件已存在，则需在改名前删除b.txt
                        return nodeInstance.deleteInode(newInode.getNodeId(), newInode.getBucket(), newInode.getObjName())
                                .flatMap(f -> esSwitch && !isError(f) ? EsMetaTask.delEsMeta(f, newInode, bucket, newObjName, newInode.getNodeId(), false).map(v -> f) : Mono.just(f))
                                .flatMap(inode1 -> {
                                    return InodeUtils.updateParentDirTime(newInode.getObjName(), bucket)
                                            .flatMap(r -> {
                                                return Mono.just(inode1);
                                            });
                                })
                                .map(i -> {
                                    if (InodeUtils.isError(i)) {
                                        log.error("rename: delete {} failed: {}", newObjName, i.getLinkN());
                                        return false;
                                    } else {
                                        return true;
                                    }
                                });
                    } else {
                        return Mono.just(true);
                    }
                })
                .flatMap(b0 -> {
                    if (!b0) {
                        return Mono.just(ERROR_INODE);
                    }

                    return nodeInstance.renameFile(oldNodeId, oldObjName, newObjName, bucket)
                            .flatMap(inode -> {
                                if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                    log.info("rename {}->{} fail: {}", oldObjName, newObjName, inode.getLinkN());
                                    return Mono.just(inode);
                                }
                                return Mono.just(inode).flatMap(i -> {
                                    if (esSwitch) {
                                        Inode cInode = inode.clone();
                                        return EsMetaTask.mvEsMeta(cInode, bucket, oldObjName, newObjName, oldNodeId);
                                    }
                                    return Mono.just(true);
                                }).map(a -> inode);
                            });
                });
    }
}
