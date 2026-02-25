package com.macrosan.filesystem.cifs.api.smb1;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cache.WriteCache;
import com.macrosan.filesystem.cifs.SMB1;
import com.macrosan.filesystem.cifs.SMB1.SMB1Reply;
import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.cifs.SMB1Header;
import com.macrosan.filesystem.cifs.call.smb1.*;
import com.macrosan.filesystem.cifs.reply.smb1.EchoReply;
import com.macrosan.filesystem.cifs.reply.smb1.MkDirReply;
import com.macrosan.filesystem.cifs.reply.smb1.ReNameReply;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.lock.redlock.LockType;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.reply.EntryOutReply;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.ReadDirCache;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.functional.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NTStatus.*;
import static com.macrosan.filesystem.cifs.SMB1.SMB1_OPCODE.*;
import static com.macrosan.message.jsonmsg.Inode.*;

@Slf4j
public class SMB1Proc {

    private static final SMB1Proc instance = new SMB1Proc();

    private static final Node nodeInstance = Node.getInstance();

    private SMB1Proc() {
    }

    public static SMB1Proc getInstance() {
        return instance;
    }

    @SMB1.Smb1Opt(value = SMB1.SMB1_OPCODE.SMB_MKDIR)
    public Mono<SMB1Reply> mkdir(SMB1Header header, Session session, MkDirCall call) {
        SMB1Reply reply = new SMB1Reply(header);
        SMB1Header replyHeader = reply.getHeader();
        EntryOutReply entryOutReply = new EntryOutReply(SunRpcHeader.newReplyHeader(header.getMid()));
        SMB1Body mkdirReply = new MkDirReply();
        byte[] dirName = new String(call.dirName).getBytes(StandardCharsets.UTF_8);
        String bucket = NFSBucketInfo.getBucketName(header.tid);

        return NFSBucketInfo.getBucketInfoReactive(bucket)
                .flatMap(bucketInfo -> {
                    ReqInfo reqHeader = new ReqInfo();
                    reqHeader.bucketInfo = bucketInfo;
                    reqHeader.bucket = bucket;
                    long inodeId = 1L;
                    return CheckUtils.cifsWritePermissionCheck(bucket)
                            .flatMap(pass -> {
                                if (!pass) {
                                    replyHeader.status = NTStatus.STATUS_ACCESS_DENIED;
                                    return Mono.just(reply);
                                }
                                return nodeInstance.getInode(bucket, inodeId)
                                        .flatMap(dirInode -> {
                                            return InodeUtils.nfsCreate(reqHeader, dirInode, 0755 | S_IFDIR, dirName, entryOutReply, header.tid, null, null);
                                        })
                                        .map(inode -> {
                                            replyHeader.status = entryOutReply.status;
                                            reply.setBody(mkdirReply);
                                            return reply;
                                        });
                            });
                });

    }

    @SMB1.Smb1Opt(value = SMB1.SMB1_OPCODE.SMB_RMDIR)
    public Mono<SMB1Reply> rmDir(SMB1Header header, Session session, MkDirCall call) {
        SMB1Reply reply = new SMB1Reply(header);
        SMB1Header replyHeader = reply.getHeader();
        SMB1Body rmdirReply = new MkDirReply();
        String objName = new String(call.dirName);
        String bucket = NFSBucketInfo.getBucketName(header.tid);
        boolean[] isRmDirErr = new boolean[1];
        return FsUtils.lookup(bucket, objName, null, false, -1, null)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        return Mono.just(inode);
                    }
                    if (inode.getObjName().endsWith("/")) {
                        String prefix = inode.getObjName();
                        return ReadDirCache.listAndCache(bucket, prefix, 0, 1024, new NFSHandler(null, null), 1L, null, inode.getACEs())
                                .flatMap(list1 -> {
                                    if (!list1.isEmpty()) {
                                        isRmDirErr[0] = true;
                                        return Mono.just(ERROR_INODE);
                                    }
                                    return Mono.just(inode);
                                });
                    }
                    return Mono.just(inode);
                })
                .flatMap(inodeRes -> {
                    if (NOT_FOUND_INODE.equals(inodeRes)) {
                        replyHeader.status = STATUS_OBJECT_NAME_NOT_FOUND;
                        reply.setBody(rmdirReply);
                        return Mono.just(reply);
                    }
                    if (ERROR_INODE.equals(inodeRes)) {
                        log.info("get inode fail.objName:{},bucket:{}", objName, bucket);
                        replyHeader.status = STATUS_DATA_ERROR;
                        if (isRmDirErr[0]) {
                            replyHeader.status = STATUS_DIRECTORY_NOT_EMPTY;
                        }
                        reply.setBody(rmdirReply);
                        return Mono.just(reply);
                    }
                    return CheckUtils.cifsWritePermissionCheck(bucket)
                            .flatMap(pass -> {
                                if (!pass) {
                                    replyHeader.status = NTStatus.STATUS_ACCESS_DENIED;
                                    return Mono.just(reply);
                                }
                                return nodeInstance.deleteInode(inodeRes.getNodeId(), bucket, inodeRes.getObjName())
                                        .flatMap(delRes -> {
                                            replyHeader.status = STATUS_SUCCESS;
                                            if (NOT_FOUND_INODE.getLinkN() == delRes.getLinkN()) {
                                                replyHeader.status = STATUS_OBJECT_NAME_NOT_FOUND;
                                            }
                                            if (ERROR_INODE.getLinkN() == delRes.getLinkN()) {
                                                replyHeader.status = STATUS_DATA_ERROR;
                                            }
                                            reply.setBody(rmdirReply);
                                            return Mono.just(reply);
                                        });
                            });

                });
    }

    @SMB1.Smb1Opt(value = SMB1.SMB1_OPCODE.SMB_FCLOSE)
    public Mono<SMB1Reply> findClose2(SMB1Header header, Session session, FindCloseCall call) {
        SMB1Reply reply = new SMB1Reply(header);
        SMB1Header replyHeader = reply.getHeader();
        SMB1Body body = new SMB1Body();
        reply.setBody(body);
        return Mono.just(reply);
    }

    @SMB1.Smb1Opt(value = SMB1.SMB1_OPCODE.SMB_CLOSE)
    public Mono<SMB1Reply> close(SMB1Header header, Session session, CloseCall call) {
        SMB1Reply reply = new SMB1Reply(header);
        SMB1Header replyHeader = reply.getHeader();
        Inode inode = NT.openedFiles.get(call.fsid);
        SMB1Body body = new SMB1Body();
        reply.setBody(body);
        String bucket = NFSBucketInfo.getBucketName(header.tid);
        if (inode != null) {
            return WriteCache.getCache(bucket, inode.getNodeId(), 0, inode.getStorage(), true)
                    .flatMap(writeCache -> {
                        if (writeCache != null) {
                            return writeCache.nfsCommit(inode, 0, 0)
                                    .flatMap(b -> {
                                        return Mono.just(reply);
                                    });
                        }
                        return Mono.just(reply);
                    });
        }
        return Mono.just(reply);
    }

    @SMB1.Smb1Opt(SMB_MV)
    public Mono<SMB1.SMB1Reply> rename(SMB1Header header, Session session, ReNameCall call) {
        SMB1.SMB1Reply reply = new SMB1.SMB1Reply(header);
        //rename
        SMB1Header replyHeader = reply.getHeader();
        SMB1Body reNameReply = new ReNameReply();
        reply.setBody(reNameReply);
        //all path
        String oldName = new String(call.oldFileName);
        String newName = new String(call.newFileName);
        String bucket = NFSBucketInfo.getBucketName(header.tid);
        AtomicReference<String> oldObjectName = new AtomicReference<>();
        AtomicReference<String> newObjName = new AtomicReference<>();
        AtomicBoolean dir = new AtomicBoolean(false);
        AtomicBoolean overWrite = new AtomicBoolean(false);
        Inode[] dirInodes = new Inode[2];
        ReqInfo reqHeader = new ReqInfo();
        return Flux.just(new Tuple2<>(true, oldName), new Tuple2<>(false, newName))
                .flatMap(t -> {
                    return CheckUtils.cifsWritePermissionCheck(bucket)
                            .flatMap(pass -> {
                                return NFSBucketInfo.getBucketInfoReactive(reqHeader.bucket)
                                        .map(map -> {
                                            reqHeader.bucketInfo = map;
                                            return pass;
                                        });
                            }).flatMap(pass -> {
                                if (!pass) {
                                    return Mono.just(NO_PERMISSION_INODE);
                                }
                                int index = t.var2.lastIndexOf("/");
                                String parName = t.var2.substring(0, index + 1);
                                return NFSBucketInfo.getBucketInfoReactive(bucket)
                                        .doOnNext(bucketInfo -> {
                                            reqHeader.bucketInfo = bucketInfo;
                                            reqHeader.bucket = bucket;
                                        }).flatMap(b -> FsUtils.lookup(bucket, parName, null, false, -1, null));
                            }).flatMap(dirInode -> {
                                if (InodeUtils.isError(dirInode)) {
                                    log.info("get inode fail.bucket:{}, from dir :{},to dir {}:{}", bucket, oldName, newName, dirInode.getLinkN());
                                    return Mono.just(new Tuple2<>(t.var1, dirInode));
                                }
                                if (dirInode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                                    return Mono.just(new Tuple2<>(t.var1, dirInode));
                                }
                                byte[] name = t.var1 ? call.oldFileName : call.newFileName;
                                String objName = dirInode.getObjName() + new String(name, 0, name.length);

                                if (t.var1) {
                                    dirInodes[0] = dirInode;
                                    oldObjectName.set(objName);
                                } else {
                                    dirInodes[1] = dirInode;
                                    newObjName.set(objName);
                                }
                                return RedLockClient.lock(reqHeader, objName, LockType.WRITE, true, t.var1)
                                        .flatMap(lock -> FsUtils.lookup(bucket, objName, reqHeader, true, dirInode.getNodeId(), dirInode.getACEs()))
                                        .map(inode -> {
                                            //mv dir
                                            if (t.var1 && StringUtils.isNotEmpty(inode.getObjName()) && inode.getObjName().endsWith("/")) {
                                                dir.set(true);
                                            }
                                            return new Tuple2<>(t.var1, inode);
                                        });
                            });

                }).collectList()
                .flatMap(list -> {
                    Inode oldInode = ERROR_INODE;
                    Inode newInode = ERROR_INODE;

                    for (Tuple2<Boolean, Inode> tuple2 : list) {
                        if (tuple2.var1) {
                            oldInode = tuple2.var2;
                        } else {
                            newInode = tuple2.var2;
                        }
                    }
//                    if (debug) {
//                        log.info("【rename】 old: {} {}, new: {} {}", oldInode.getObjName(), oldInode.getLinkN(), newInode.getObjName(), newInode.getLinkN());
//                    }

                    if (ERROR_INODE.equals(oldInode) || NOT_FOUND_INODE.equals(oldInode)) {
                        replyHeader.status = STATUS_DATA_ERROR;
                        if (NOT_FOUND_INODE.equals(oldInode)) {
                            replyHeader.status = STATUS_NO_SUCH_FILE;
                        }
                        return Mono.just(reply)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }
                    if (oldInode.getLinkN() == NO_PERMISSION_INODE.getLinkN()) {
                        return Mono.just(reply)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }

                    // 如果新名称的Inode已存在，则应当覆盖已存在的文件；如果inode为retryInode，表示返回的是当前目录下子目录的inode，为客户端长时间未接收到响应重发的rename请求，应当直接返回
                    if (!InodeUtils.isError(newInode)) {
                        if (RETRY_INODE.getLinkN() == newInode.getLinkN() && newInode.getNodeId() == -3) {
                            //正在重命名，返回结果为空
                            return Mono.just(reply)
                                    .doOnNext(r2 -> releaseLock(reqHeader, r2));
                        }
                        overWrite.set(true);
                    }

                    String[] oldObjName0 = new String[1];
                    String[] newObjName0 = new String[1];

                    oldObjName0[0] = dir.get() ? oldObjectName.get() + '/' : oldObjectName.get();
                    newObjName0[0] = dir.get() ? newObjName.get() + '/' : newObjName.get();
                    if (newObjName0[0].getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH) {
                        //to do
                        replyHeader.status = FsConstants.NfsErrorNo.NFS3ERR_NAMETOOLONG;
                        return Mono.just(reply)
                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                    }
                    // 如果更改的为目录
                    if (dir.get()) {
                        // 如果更新后的目录名已经存在，则检查这个新的目录名之下是否还有与旧目录名称相同的dirInode
                        if (overWrite.get()) {
                            String prefix = newInode.getObjName();
                            List<Inode> list1 = ReadDirCache.listAndCache(bucket, prefix, 0, 4096, reqHeader.nfsHandler, newInode.getNodeId(), null, null).block();
                            if (!list1.isEmpty()) {
                                log.error("directory already exists: {}", newObjName0[0]);
                                replyHeader.status = STATUS_OBJECT_NAME_COLLISION;
                                return Mono.just(reply)
                                        .doOnNext(r2 -> releaseLock(reqHeader, r2));
                            }
                        }

                        // 将oldInode视为dirInode，遍历该目录下的所有子目录与文件
                        // 重命名时不需要再get新的inode，直接rename即可
                        return scanAndReName(oldInode, bucket, newObjName0[0], 0, reqHeader.nfsHandler)
                                .flatMap(inode -> {
                                    if (InodeUtils.isError(inode)) {
                                        log.info("rename {}->{} fail: {}", oldObjName0, newObjName0, inode.getLinkN());
                                    }
                                    return updateTime(dirInodes, reply, bucket)
                                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                });
                    } else {
                        // 如果更改的为文件
                        return renameFile(oldInode.getNodeId(), newInode, oldObjName0[0], newObjName0[0], bucket, overWrite.get())
                                .flatMap(inode -> {
                                    if (InodeUtils.isError(inode) || inode.isDeleteMark()) {
                                        if (NOT_FOUND_INODE.getLinkN() == inode.getLinkN() || inode.isDeleteMark()) {
                                            replyHeader.status = STATUS_NO_SUCH_FILE;
                                        }
                                        if (ERROR_INODE.getLinkN() == inode.getLinkN()) {
                                            replyHeader.status = STATUS_DATA_ERROR;
                                        }
                                        return Mono.just(reply)
                                                .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                    }
                                    return Mono.just(inode)
                                            .flatMap(i -> updateTime(dirInodes, reply, bucket))
                                            .doOnNext(r2 -> releaseLock(reqHeader, r2));
                                });

                    }
                });
    }


    @SMB1.Smb1Opt(SMB_ECHO)
    public Mono<SMB1.SMB1Reply> echo(SMB1Header header, Session session0, EchoCall call) {
        SMB1.SMB1Reply reply = new SMB1.SMB1Reply(header);
        EchoReply echoReply = new EchoReply();
        //
        echoReply.seqNum = 1;
        echoReply.echoData = call.echoData;
        reply.setBody(echoReply);
        return Mono.just(reply);
    }


    public Mono<SMB1Reply> releaseLock(ReqInfo reqHeader, SMB1Reply reNameReply) {
        if (!reqHeader.lock.isEmpty()) {
            for (String key : reqHeader.lock.keySet()) {
                String value = reqHeader.lock.get(key);
                RedLockClient.unlock(reqHeader.bucket, key, value, true).subscribe();
            }
        }
        return Mono.just(reNameReply);
    }

    /**
     * rename完成后更新fromDirFh与toDirFh代表的父级目录的mtime和ctime
     **/
    public Mono<SMB1Reply> updateTime(Inode[] dirInodes, SMB1Reply reNameReply, String bucket) {
        List<Inode> inodeList = Arrays.stream(dirInodes).filter(Objects::nonNull).collect(Collectors.toList());

        // 如果两个inode的nodeId相等，只发送一次更改时间的请求
        if (inodeList.size() == 2 && !InodeUtils.isError(dirInodes[0]) &&
                !InodeUtils.isError(dirInodes[1]) && dirInodes[0].getNodeId() == dirInodes[1].getNodeId()) {
            return nodeInstance.updateInodeTime(dirInodes[0].getNodeId(), dirInodes[0].getBucket(), System.currentTimeMillis() / 1000, (int) (System.nanoTime() % ONE_SECOND_NANO), false, true, true)
                    .map(inode -> {
                        dirInodes[0] = inode;
                        dirInodes[1] = inode;
                        return reNameReply;
                    })
                    .doOnError(e -> log.info("update time error 1", e))
                    .onErrorReturn(reNameReply);
        } else {
            long stamp = System.currentTimeMillis() / 1000;
            int stampNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
            return Flux.fromIterable(inodeList)
                    .flatMap(dirInode -> nodeInstance.updateInodeTime(dirInode.getNodeId(), dirInode.getBucket(), stamp, stampNano, false, true, true))
                    .collectList()
                    .map(list -> {
                        if (list.get(0).getNodeId() == dirInodes[0].getNodeId()) {
                            dirInodes[0] = list.get(0);
                            dirInodes[1] = list.get(1);
                        } else {
                            dirInodes[1] = list.get(0);
                            dirInodes[0] = list.get(1);
                        }
                        return reNameReply;
                    })
                    .doOnError(e -> log.info("update time error 2", e))
                    .onErrorReturn(reNameReply);
        }
    }

    public Mono<Inode> renameFile(long oldNodeId, Inode newInode, String oldObjName, String newObjName, String bucket, boolean isOverWrite) {
        return Mono.just(isOverWrite)
                .flatMap(b -> {
                    if (b) {
                        // mv a.txt b.txt，如果b.txt文件已存在，则需在改名前删除b.txt
                        return nodeInstance.deleteInode(newInode.getNodeId(), newInode.getBucket(), newInode.getObjName())
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
                                if (InodeUtils.isError(inode)) {
                                    log.info("rename {}->{} fail: {}", oldObjName, newObjName, inode.getLinkN());
                                    return Mono.just(inode);
                                }
                                return Mono.just(inode);
                            });
                });
    }

    /**
     * @param dirInode   当前的目录的完整路径 /test0/fio/old/
     * @param newObjName 当前目录所要更改成的新目录名 /test0/fio/new/
     * @param bucket     桶名
     * @param count      offset
     **/
    public Mono<Inode> scanAndReName(Inode dirInode, String bucket, String newObjName, long count, NFSHandler nfsHandler) {
        String prefix = dirInode.getObjName();
        AtomicLong offset = new AtomicLong(count);
        return ReadDirCache.listAndCache(bucket, prefix, offset.get(), 1 << 20, nfsHandler, dirInode.getNodeId(), null, null)
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
//                    offset.accumulateAndGet(inode.getCookie(), (oldVal, newVal) -> oldVal > newVal ? oldVal : newVal);
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
                    return ReadDirCache.listAndCache(bucket, dirInode.getObjName(), offset.get(), 4096, nfsHandler, dirInode.getNodeId(), null, null)
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

    @SMB1.Smb1Opt(value = SMB_UNLINK)
    public Mono<SMB1Reply> remove(SMB1Header header, Session session0, RemoveCall removeCall) {
        SMB1Reply reply = new SMB1Reply(header);
        SMB1Header replyHeader = reply.getHeader();
        String bucket = NFSBucketInfo.getBucketName(header.tid);
        String objName = new String(removeCall.fileName);

        return FsUtils.lookup(bucket, objName, null, false, -1, null)
                .flatMap(inode -> {
                    if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                        replyHeader.status = STATUS_DATA_ERROR;
                        log.info("get inode fail.objName:{},bucket:{}", objName, bucket);
                        return Mono.just(reply);
                    }
                    if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                        replyHeader.status = STATUS_OBJECT_NAME_NOT_FOUND;
                        return Mono.just(reply);
                    }
                    return CheckUtils.cifsWritePermissionCheck(bucket)
                            .flatMap(pass -> {
                                if (!pass) {
                                    replyHeader.status = NTStatus.STATUS_ACCESS_DENIED;
                                    return Mono.just(reply);
                                }
                                return nodeInstance.deleteInode(inode.getNodeId(), inode.getBucket(), inode.getObjName())
                                        .flatMap(inode1 -> {
                                            if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                                replyHeader.status = STATUS_DATA_ERROR;
                                                return Mono.just(reply);
                                            }
                                            if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                                replyHeader.status = STATUS_OBJECT_NAME_NOT_FOUND;
                                                return Mono.just(reply);
                                            }
                                            InodeUtils.updateParentDirTime(objName, bucket);
                                            return Mono.just(reply);
                                        });
                            });
                });
    }
}
