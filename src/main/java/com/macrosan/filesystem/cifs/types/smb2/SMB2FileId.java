package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cache.CifsQueryDirCache;
import com.macrosan.filesystem.cifs.rpc.pdu.call.RpcRequestCall;
import com.macrosan.filesystem.cache.WriteCacheClient;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.pipe.BindPduInfo;
import com.macrosan.filesystem.cifs.types.smb2.pipe.RpcPipeType;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.filesystem.FsConstants.ONE_SECOND_NANO;
import static com.macrosan.filesystem.cifs.call.smb2.CreateCall.FILE_DELETE_ON_CLOSE;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/f1d9b40d-e335-45fc-9d0b-199a31ede4c3
 */
@Data
@Accessors(chain = true)
@Log4j2
@EqualsAndHashCode
@ToString
public class SMB2FileId {
    public long persistent;
    public long volatile_;

    public int readStruct(ByteBuf buf, int offset) {
        persistent = buf.getLongLE(offset);
        volatile_ = buf.getLongLE(offset + 8);
        return 16;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLongLE(offset, persistent);
        buf.setLongLE(offset + 8, volatile_);
        return 16;
    }

    //TODO close或者超时后清除缓存
    public static Map<Long, FileInfo> cache = new ConcurrentHashMap<>();
    private static AtomicLong id = new AtomicLong();
    public static Set<Long> needDeleteSet = ConcurrentHashMap.newKeySet();
    public static long CLEAR_DURATION = 5 * 60 * ONE_SECOND_NANO; // 5分钟
    public static long TIMEOUT_DURATION = 15 * 60 * ONE_SECOND_NANO; // 15分钟

    static MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("FileInfoCache-"));

    static {
        executor.submit(SMB2FileId::tryClearCache);
    }

    private static void tryClearCache() {
        for (Long persistent : cache.keySet()) {
            cache.computeIfPresent(persistent, (k, info) -> {
                boolean needClear = false;
                if (info.closeTime > 0) {
                    //fileInfo已经关闭，满足清除时长则清理
                    long closeDuration = System.nanoTime() - info.closeTime;
                    if (closeDuration > CLEAR_DURATION && info.closeTime > info.openTime) {
                        needClear = true;
                    }
                } else {
                    //fileInfo没有关闭，满足超时时长再清理
                    long idleDuration = System.nanoTime() - info.openTime;
                    if (idleDuration > TIMEOUT_DURATION) {
                        needClear = true;
                    }
                }

                //需要进行清理
                if (needClear) {
                    if (info.queryDirCache != null) {
                        info.queryDirCache.clear();
                        info.queryDirCache = null;
                    }
                    WriteCacheClient.clear(persistent, info);
                    return null;
                }

                return info;
            });
        }
        executor.schedule(SMB2FileId::tryClearCache, 200, TimeUnit.MILLISECONDS);
    }

    private static final int LOCAL_NODE = Integer.parseInt(ServerConfig.getInstance().getHostUuid());

    /**
     * 根据 FileId 获取 node
     */
    public static String getNode(long id) {
        int nodeValue = (int) (id % 1024);
        if (nodeValue <= 0) {
            nodeValue += 1024;
        }
        return String.format("%04d", nodeValue);
    }

    /**
     * 计算分布式 FileId。 nodeSize * (id - 1) + LOCAL_NODE
     */
    public static long getId() {
        long id0 = id.incrementAndGet();
        return 1024 * (id0 - 1) + LOCAL_NODE;
    }

    public static SMB2FileId randomFileId(CompoundRequest compoundRequest, String bucket, String obj, long inodeId,
                                          long dirNodeId, int createOptions, Inode openInode, List<Inode.ACE> ACEs) {
        long id0 = getId();
        return randomFileId(compoundRequest, bucket, obj, inodeId, dirNodeId, createOptions, openInode, ACEs, id0);
    }
    public static SMB2FileId randomFileId(CompoundRequest compoundRequest, String bucket, String obj, long inodeId,
                                          long dirNodeId, int createOptions, Inode openInode, List<Inode.ACE> ACEs, long id0) {
        boolean needDelete = (createOptions & FILE_DELETE_ON_CLOSE) != 0;
        if (needDelete) {
            needDeleteSet.add(inodeId);
        }

        FileInfo v = new FileInfo(bucket, obj, inodeId, dirNodeId, openInode, ACEs);
        cache.compute(id0, (k, v0) -> v);

        SMB2FileId fileId = new SMB2FileId()
                .setPersistent(id0)
                .setVolatile_(0);
        compoundRequest.setFileId(fileId);

        return fileId;
    }

    @Data
    @ToString
    public static class FileInfo {
        public String bucket;
        public String obj;
        public long inodeId;
        public CifsQueryDirCache queryDirCache;

        public long dirInode;
        //打开时间，每次各接口从内存getFileInfo会刷新一次openTime
        public long openTime;
        public long closeTime = -1;
        public boolean updateTimeOnClose = false;
        public Inode openInode;
        public List<Inode.ACE> ACEs;
        public AtomicBoolean abandon = new AtomicBoolean(false);
        public BindPduInfo bindPduInfo;
        public RpcRequestCall rpcRequestCall;
        public int[] uidAndGid;
        public String targetName;
        public int isGroupOrUser = 1; // 0: group, 1: user
        public long allocationSize = Long.MAX_VALUE;

        FileInfo(String bucket, String obj, long inodeId, long dirInode, Inode openInode, List<Inode.ACE> ACEs) {
            this.bucket = bucket;
            this.obj = obj;
            this.inodeId = inodeId;
            this.openTime = System.nanoTime();
            this.openInode = openInode;
            this.dirInode = dirInode;
            this.ACEs = ACEs;
        }
    }

    public FileInfo getFileInfo(CompoundRequest compoundRequest) {
        //共用SMB2FileId
        if (compoundRequest != null && compoundRequest.fileId != null) {
            FileInfo fileInfo = cache.get(compoundRequest.fileId.persistent);
            if (null != fileInfo) {
                fileInfo.openTime = System.nanoTime();
            }
            return fileInfo;
        }

        return cache.compute(persistent, (k, v) -> {
            if (v == null) {
                return v;
            } else {
                v.openTime = System.nanoTime();
                return v;
            }
        });
    }

    public FileInfo getFileInfo(SMB2FileId smb2FileId) {
        FileInfo fileInfo = cache.get(smb2FileId.persistent);
        if (null != fileInfo) {
            fileInfo.openTime = System.nanoTime();
        }
        return fileInfo;
    }

    //TODO close不应该从session中删除
    public static void removeFileInfo(Session session) {
//        cache.remove(session.lastUsedFileId.persistent);
    }

    public static boolean isFileNeedDelete(long inodeId) {
        return needDeleteSet.contains(inodeId);
    }

    public static void updateFileInfo(CompoundRequest compoundRequest, SMB2FileId fileId, FileInfo fileInfo) {
        if (fileId == null) {
            return;
        }
        long id = 0L;

        // 多个请求的情况
        if (fileId.persistent == -1 && fileId.volatile_ == -1) {
            id = compoundRequest.fileId.persistent;
        } else {
            // 单个请求的情况用call中的persistent
            id = fileId.persistent;
        }
        cache.compute(id, (k, v) -> {
            if (v == null) {
                return v;
            }
            return fileInfo;
        });
    }

    public static void clearCache(CompoundRequest compoundRequest, SMB2FileId fileId) {
        if (fileId == null) {
            return;
        }

        long id = 0L;

        // 多个请求的情况
        if (fileId.persistent == -1 && fileId.volatile_ == -1) {
            id = compoundRequest.fileId.persistent;
        } else {
            // 单个请求的情况用call中的persistent
            id = fileId.persistent;
        }

        cache.compute(id, (k, v) -> {
            if (v == null) {
                return v;
            }

            // 清除list缓存
            if (v.queryDirCache != null) {
                v.queryDirCache.clear();
                v.queryDirCache = null;
            }

            v.closeTime = System.nanoTime();
            return null;
        });
    }
}
