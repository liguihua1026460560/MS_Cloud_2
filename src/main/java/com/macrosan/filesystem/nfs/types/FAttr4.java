package com.macrosan.filesystem.nfs.types;


import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ErasureClient;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.message.jsonmsg.BucketInfo;
import com.macrosan.message.jsonmsg.DirInfo;
import com.macrosan.message.jsonmsg.FSQuotaConfig;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import io.netty.buffer.ByteBuf;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.nfs.NFSBucketInfo.FSID_BUCKET;
import static com.macrosan.filesystem.nfs.api.NFS4Proc.ROOT_INODE;
import static com.macrosan.filesystem.nfs.call.v4.SetAttrV4Call.NFS4_SET_TO_CLIENT_TIME;
import static com.macrosan.filesystem.nfs.call.v4.SetAttrV4Call.NFS4_SET_TO_SERVER_TIME;
import static com.macrosan.filesystem.quota.FSQuotaConstants.FS_DIR_QUOTA;

@Log4j2
@ToString
public class FAttr4 {
    public static final int supportedAttrs = 1 << 0;
    public static final int type = 1 << 1;
    public static final int expireType = 1 << 2;
    public static final int change = 1 << 3;
    public static final int size = 1 << 4;
    public static final int linkSupport = 1 << 5;
    public static final int symlinkSupport = 1 << 6;
    public static final int nameAttr = 1 << 7;
    public static final int fsId = 1 << 8;
    public static final int uniqueHandle = 1 << 9;
    public static final int leaseTime = 1 << 10;
    public static final int rDAttrError = 1 << 11;
    public static final int acl = 1 << 12;
    public static final int aclSupport = 1 << 13;
    public static final int archive = 1 << 14;
    public static final int canSetTime = 1 << 15;
    public static final int caseInSensitive = 1 << 16;
    public static final int casePreserving = 1 << 17;
    public static final int chownRestricted = 1 << 18;
    public static final int fileHandle = 1 << 19;
    public static final int fileId = 1 << 20;
    public static final int filesAvail = 1 << 21;
    public static final int filesFree = 1 << 22;
    public static final int filesTotal = 1 << 23;
    public static final int fsLocations = 1 << 24;
    public static final int hidden = 1 << 25;
    public static final int homogeneous = 1 << 26;
    public static final int maxFileSize = 1 << 27;
    public static final int maxLink = 1 << 28;
    public static final int maxName = 1 << 29;
    public static final int maxRead = 1 << 30;
    public static final int maxWrite = 1 << 31;

    public static final int mimeType = 1 << 0;
    public static final int mode = 1 << 1;
    public static final int noTrunc = 1 << 2;
    public static final int numLinks = 1 << 3;
    public static final int owner = 1 << 4;
    public static final int ownerGroup = 1 << 5;
    public static final int quotaHard = 1 << 6;
    public static final int quotaSoft = 1 << 7;
    public static final int quotaUsed = 1 << 8;
    public static final int rawDev = 1 << 9;
    public static final int spaceAvail = 1 << 10;
    public static final int spaceFree = 1 << 11;
    public static final int spaceTotal = 1 << 12;
    public static final int spaceUsed = 1 << 13;
    public static final int system = 1 << 14;
    public static final int timeAccess = 1 << 15;
    public static final int timeAccessSet = 1 << 16;
    public static final int backup = 1 << 17;
    public static final int timeCreate = 1 << 18;
    public static final int timeDelta = 1 << 19;
    public static final int timeMetaData = 1 << 20;
    public static final int timeModify = 1 << 21;
    public static final int timeModifySet = 1 << 22;
    public static final int mountedOnFileId = 1 << 23;
    public static final int fsLayoutType = 1 << 30;

    public static final int layoutType = 1 << 0;
    public static final int layoutBlksize = 1 << 1;
    public static final int mdsthreshold = 1 << 4;
    public static final int suppattrExclCreat = 1 << 11;
    public static final int cloneBlksize = 1 << 13;
    public static final int securityLabel = 1 << 16;
    public static final int modeUmask = 1 << 17;

    public interface Mask {
        int getMask();

        int getLen();
    }

    public enum Mask1 implements Mask {
        supportedAttrs(1 << 0, 16),
        type(1 << 1, 4),
        expireType(1 << 2, 4),
        change(1 << 3, 8),
        size(1 << 4, 8),
        linkSupport(1 << 5, 4),
        symlinkSupport(1 << 6, 4),
        nameAttr(1 << 7, 4),
        fsId(1 << 8, 16),
        uniqueHandle(1 << 9, 4),
        leaseTime(1 << 10, 4),
        rDAttrError(1 << 11, 4),
        acl(1 << 12, 4),
        aclSupport(1 << 13, 4),
        canSetTime(1 << 15, 4),
        caseInSensitive(1 << 16, 4),
        casePreserving(1 << 17, 4),
        chownRestricted(1 << 18, 4),
        fileHandle(1 << 19, 20),
        fileId(1 << 20, 8),
        filesAvail(1 << 21, 8),
        filesFree(1 << 22, 8),
        filesTotal(1 << 23, 8),
        fsLocations(1 << 24, 8),
        homogeneous(1 << 26, 4),
        maxFileSize(1 << 27, 8),
        maxLink(1 << 28, 4),
        maxName(1 << 29, 4),
        maxRead(1 << 30, 8),
        maxWrite(1 << 31, 8),
        ;
        public final int mask;
        public final int len;

        Mask1(int mask, int len) {
            this.mask = mask;
            this.len = len;

        }

        @Override
        public int getMask() {
            return mask;
        }

        @Override
        public int getLen() {
            return len;
        }
    }

    public enum Mask2 implements Mask {
        mode(1 << 1, 4),
        noTrunc(1 << 2, 4),
        numLinks(1 << 3, 4),
        owner(1 << 4, 8),
        ownerGroup(1 << 5, 8),
        rawDev(1 << 9, 8),
        spaceAvail(1 << 10, 8),
        spaceFree(1 << 11, 8),
        spaceTotal(1 << 12, 8),
        spaceUsed(1 << 13, 8),
        timeAccess(1 << 15, 12),
        timeAccessSet(1 << 16, 4),
        timeDelta(1 << 19, 12),
        timeMetaData(1 << 20, 12),
        timeModify(1 << 21, 12),
        timeModifySet(1 << 22, 4),
        mountedOnFileId(1 << 23, 8),
        fsLayoutType(1 << 30, 4);
        public int mask;
        public int len;

        Mask2(int mask, int len) {
            this.mask = mask;
            this.len = len;

        }

        @Override
        public int getMask() {
            return mask;
        }

        @Override
        public int getLen() {
            return len;
        }
    }

    public enum Mask3 implements Mask {
        layout_type(1 << 0, 4),
        layoutBlksize(1 << 1, 4),
        suppattrExclCreat(1 << 11, 12),
        modeUmask(1 << 17, 4);
        public int mask;
        public int len;

        Mask3(int mask, int len) {
            this.mask = mask;
            this.len = len;

        }

        @Override
        public int getMask() {
            return mask;
        }

        @Override
        public int getLen() {
            return len;
        }
    }

    public static final int[] SUPPORT_ATTR_4_0 = new int[]{0xfdffbfff, 0x40f9be3e};
    public static final int[] SUPPORT_ATTR_4_1 = new int[]{0xfdffbfff, 0x40f9be3e};
    public static final int[] SUPPORT_ATTR_4_2 = new int[]{0xfdffbfff, 0x40f9be3e, 0x00020000};
    public static final int[][] SUPPORT_ATTR = {SUPPORT_ATTR_4_0, SUPPORT_ATTR_4_1, SUPPORT_ATTR_4_2};

    public Inode inode;
    public FH2 fh2;
    public long fsid;
    public int[] mask;
    public int minorVersion;
    public List<Mask1> mask1s = new ArrayList<>();
    public List<Mask2> mask2s = new ArrayList<>();
    public List<Mask3> mask3s = new ArrayList<>();
    public ObjAttr objAttr;
    public int[] supportAttr;
    public long totalBytes;
    public long freeBytes;
    public long availFreeBytes;
    public Map<Integer, Map<Integer, Object>> maskAttrMap = new HashMap<>();
    public List<Boolean> verifyRes = new ArrayList<>();


    public FAttr4(Inode inode, long fsid, int[] mask, int minorVersion) {
        this.inode = inode;
        this.fsid = fsid;
        this.mask = mask;
        this.fh2 = inode == null ? new FH2() : FH2.mapToFH2(inode, (int) fsid);
        this.minorVersion = minorVersion;
        objAttr = new ObjAttr();
        mapToMask();
    }

    public Mono<Boolean> setSpaceTotal() {
        if (mask.length > 1 && ((mask[1] & spaceAvail) != 0 || (mask[1] & spaceTotal) != 0 || (mask[1] & spaceFree) != 0)) {
            if (inode != null) {
                return FSQuotaUtils.findFinalMinFsQuotaConfig(inode.getBucket(), inode.getNodeId())
                        .flatMap(tuple -> {
                            FSQuotaConfig config = tuple.getT1();
                            long inodeId = tuple.getT2();
                            if (inodeId == 1L) {
                                return ErasureClient.reduceBucketInfo(inode.getBucket())
                                        .filter(BucketInfo::isAvailable)
                                        .map(BucketInfo::getBucketStorage)
                                        .doOnError(e -> log.error("get bucket used capacity error", e))
                                        .defaultIfEmpty("0")
                                        .flatMap(usedCapacityStr -> {
                                            long usedCapacity = Long.parseLong(usedCapacityStr);
                                            long hard = config.getCapacityHardQuota() != 0 ? config.getCapacityHardQuota() : -1;
                                            long totalSize = (hard > 0) ? Math.max(hard, usedCapacity) : 0;
                                            long availableSize = Math.max(0, totalSize - usedCapacity);

                                            this.totalBytes = totalSize;
                                            this.freeBytes = availableSize;
                                            this.availFreeBytes = this.freeBytes;

                                            return Mono.just(true);
                                        });
                            }

                            return FSQuotaRealService.getFsQuotaInfo(inode.getBucket(), inodeId, FS_DIR_QUOTA, 0)
                                    .flatMap(dirInfo -> {
                                        if (DirInfo.isErrorInfo(dirInfo)) {
                                            return Mono.empty();
                                        } else {
                                            long usedCapacity = Long.parseLong(dirInfo.getUsedCap());
                                            long hard = config.getCapacityHardQuota() != 0 ? config.getCapacityHardQuota() : -1;
                                            long totalSize = (hard > 0) ? Math.max(hard, usedCapacity) : 0;
                                            long availableSize = Math.max(0, totalSize - usedCapacity);

                                            this.totalBytes = totalSize;
                                            this.freeBytes = availableSize;
                                            this.availFreeBytes = this.freeBytes;
                                        }

                                        return Mono.just(true);
                                    });

                        })
                        // 只有当目录以及其任意祖先目录都不存在配额时才会进这里
                        .switchIfEmpty(Mono.defer(() -> {
                            List<StoragePool> storagePoolList = StoragePoolFactory.getAvailableStoragesWithCachePool(inode.getBucket());
                            for (StoragePool pool : storagePoolList) {
                                int km = pool.getM() + pool.getK();
                                int k = pool.getK();

                                this.totalBytes += (pool.getCache().totalSize + pool.getCache().firstPartCapacity) / km * k;
                                this.freeBytes += (pool.getCache().totalSize + pool.getCache().firstPartCapacity - pool.getCache().size - pool.getCache().firstPartUsedSize) / km * k;
                            }
                            this.availFreeBytes = this.freeBytes;
                            return Mono.just(true);
                        }));
            } else {
                Set<StoragePool> dataPoolSet = new HashSet<>();
                return RedisConnPool.getInstance().getReactive(REDIS_SYSINFO_INDEX).hgetall(FSID_BUCKET)
                        .flatMap(fsidToBucket -> Flux.fromIterable(fsidToBucket.values()).concatMap(bucket -> RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket))
                                .filter(bucketInfo -> StringUtils.isNotBlank(bucketInfo.get("nfs")) && bucketInfo.get("nfs").equals("1"))
                                .flatMap(bucketInfo -> Mono.just(StoragePoolFactory.getAvailableStoragesWithCachePool(bucketInfo.get("bucket_name"))))
                                .doOnNext(dataPoolSet::addAll)
                                .collectList().map(v -> {
                                    for (StoragePool pool : dataPoolSet) {
                                        int km = pool.getM() + pool.getK();
                                        int k = pool.getK();
                                        this.totalBytes += (pool.getCache().totalSize + pool.getCache().firstPartCapacity) / km * k;
                                        this.freeBytes += (pool.getCache().totalSize + pool.getCache().firstPartCapacity - pool.getCache().size - pool.getCache().firstPartUsedSize) / km * k;
                                    }
                                    this.availFreeBytes = this.freeBytes;
                                    return true;
                                }));
            }
        }
        return Mono.just(true);
    }

    public FAttr4(int[] mask, int minorVersion) {
        this.mask = mask;
        this.minorVersion = minorVersion;
        objAttr = new ObjAttr();
        mapToMask();
    }

    public int getAttrSize() {
        int mask1Len = mask1s.stream().mapToInt(mask1 -> {
            if (mask1.equals(Mask1.fileHandle)) {
                //4字节是fhSize占用
                return fh2.fhSize + 4;
            }
            return mask1.len;
        }).sum();
        int mask2Len = mask2s.stream().mapToInt(mask2 -> mask2.len).sum();
        int mask3Len = mask3s.stream().mapToInt(mask3 -> mask3.len).sum();
        return mask1Len + mask2Len + mask3Len + 8 + 4 * mask.length;
    }

    public void readStruct(ByteBuf buf, int offset) {
        int[] finalOffset = new int[]{offset};
        for (int i = 0; i < mask.length; i++) {
            Map<Integer, Object> attrMap = maskAttrMap.computeIfAbsent(i, k -> new HashMap<>());
            switch (i) {
                case 0:
                    getMask(Mask1.class, mask[0]).forEach(mask1 -> readMask1(buf, finalOffset, mask1.mask, objAttr, attrMap, verifyRes));
                    break;
                case 1:
                    getMask(Mask2.class, mask[1]).forEach(mask2 -> readMask2(buf, finalOffset, mask2.mask, objAttr, attrMap, verifyRes));
                    break;
                case 2:
                    getMask(Mask3.class, mask[2]).forEach(mask3 -> readMask3(buf, finalOffset, mask3.mask, objAttr, attrMap, verifyRes));
                    break;
            }
        }
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, mask.length);
        offset += 4;
        for (int i : mask) {
            buf.setInt(offset, i);
            offset += 4;
        }
        int maskStart = offset + 4;
        int[] finalOffset = new int[]{maskStart};
        mask1s.forEach(mask1 -> writeMask1(buf, finalOffset, mask1.mask));
        mask2s.forEach(mask2 -> writeMask2(buf, finalOffset, mask2.mask));
        mask3s.forEach(mask3 -> writeMask3(buf, finalOffset, mask3.mask));
        buf.setInt(offset, finalOffset[0] - maskStart);
        return finalOffset[0] - start;
    }

    public static void readMask1(ByteBuf buf, int[] finalOffset, int mask, ObjAttr attr, Map<Integer, Object> extraAttrMap, List<Boolean> verifyRes) {
        int offset = finalOffset[0];
        int res = 0;
        switch (mask) {
            case size:
                long size = buf.getLong(offset);
                attr.hasSize = 1;
                attr.size = size;
                offset += 8;
                break;
            case type:
                int type0 = buf.getInt(offset);
                extraAttrMap.put(type, type0);
                offset += 4;
                break;
            case expireType:
            case uniqueHandle:
            case nameAttr:
            case acl:
            case caseInSensitive:
            case chownRestricted:
            case aclSupport:
                res = buf.getInt(offset);
                verifyRes.add(res == 0);
                offset += 4;
                break;
            case change:
                long change0 = buf.getLong(offset);
                extraAttrMap.put(change, change0);
                offset += 8;
                break;
            case linkSupport:
            case symlinkSupport:
            case homogeneous:
            case canSetTime:
            case casePreserving:
                res = buf.getInt(offset);
                verifyRes.add(res == 1);
                offset += 4;
                break;
            case fsId:
                long majorId = buf.getLong(offset);
                long minorId = buf.getLong(offset + 8);
                verifyRes.add(majorId == 0 && minorId == 0);
                offset += 16;
                break;
            case leaseTime:
                int leaseTime0 = buf.getInt(offset);
                verifyRes.add(leaseTime0 == NFS4_LEASE_TIME);
                offset += 4;
                break;
            case filesAvail:
            case filesFree:
            case filesTotal:
                long filesTotal0 = buf.getLong(offset);
                verifyRes.add(filesTotal0 == Long.MAX_VALUE);
                offset += 8;
                break;
            case fsLocations:
                int pathname0 = buf.getInt(offset);
                int serverPathname0 = buf.getInt(offset + 4);
                verifyRes.add(pathname0 == 0 && serverPathname0 == 0);
                offset += 8;
                break;
            case maxFileSize:
                long maxFileSize0 = buf.getLong(offset);
                verifyRes.add(maxFileSize0 == MAX_UPLOAD_TOTAL_SIZE);
                offset += 8;
                break;
            case maxLink:
            case maxName:
                res = buf.getInt(offset);
                verifyRes.add(res == 255);
                offset += 4;
                break;
            case maxRead:
            case maxWrite:
                long maxWrite0 = buf.getLong(offset);
                verifyRes.add(maxWrite0 == 1048576);
                offset += 8;
                break;
            //r w
//            case hidden:

        }
        finalOffset[0] = offset;
    }

    public static void readMask2(ByteBuf buf, int[] finalOffset, int mask, ObjAttr attr, Map<Integer, Object> extraAttrMap, List<Boolean> verifyRes) {
        int offset = finalOffset[0];
        int res = 0;
        switch (mask) {
            case mode:
                int mode = buf.getInt(offset);
                attr.hasMode = 1;
                attr.mode = mode;
                offset += 4;
                break;
            case noTrunc:
                res = buf.getInt(offset);
                verifyRes.add(res == 1);
                break;
            case numLinks:
                int numLink0 = buf.getInt(offset);
                extraAttrMap.put(numLinks, numLink0);
                offset += 4;
                break;
            case owner:
                int ownerLen = buf.getInt(offset);
                offset += 4;
                byte[] owner = new byte[ownerLen];
                buf.getBytes(offset, owner);
                offset += (ownerLen + 3) / 4 * 4;
                attr.hasUid = 1;
                attr.uid = Integer.parseInt(new String(owner));
                break;
            case ownerGroup:
                int groupLen = buf.getInt(offset);
                offset += 4;
                byte[] group = new byte[groupLen];
                buf.getBytes(offset, group);
                offset += (groupLen + 3) / 4 * 4;
                attr.hasGid = 1;
                attr.gid = Integer.parseInt(new String(group));
                break;
            case rawDev:
                int majorDev = buf.getInt(offset);
                int minorDev = buf.getInt(offset + 4);
                extraAttrMap.put(rawDev, new int[]{majorDev, minorDev});
                offset += 8;
                break;
            case spaceAvail:
                long spaceAvail0 = buf.getLong(offset);
                extraAttrMap.put(spaceAvail, spaceAvail0);
                offset += 8;
                break;
            case spaceFree:
                long spaceFree0 = buf.getLong(offset);
                extraAttrMap.put(spaceFree, spaceFree0);
                offset += 8;
                break;
            case spaceTotal:
                long spaceTotal0 = buf.getLong(offset);
                extraAttrMap.put(spaceTotal, spaceTotal0);
                offset += 8;
                break;
            case spaceUsed:
                long spaceUsed0 = buf.getLong(offset);
                extraAttrMap.put(spaceUsed, spaceUsed0);
                offset += 8;
                break;
            // r w
//            case system:
            case timeAccessSet:
                int setAtime = buf.getInt(offset);
                offset += 4;
                switch (setAtime) {
                    case NFS4_SET_TO_SERVER_TIME:
                        attr.hasAtime = 1;
                        break;
                    case NFS4_SET_TO_CLIENT_TIME:
                        attr.hasAtime = 2;
                        attr.atime = (int) buf.getLong(offset);
                        attr.atimeNano = buf.getInt(offset + 8);
                        offset += 12;
                        break;
                }
                break;
            case timeAccess:
                attr.hasAtime = 1;
                attr.atime = (int) buf.getLong(offset);
                attr.atimeNano = buf.getInt(offset + 8);
                offset += 12;
                break;
            case timeDelta:
                //seconds,nSeconds
                long seconds = buf.getLong(offset);
                int nSeconds = buf.getInt(offset + 8);
                verifyRes.add(seconds == 0 && nSeconds == 1000000);
                offset += 12;
                break;
            case timeMetaData:
                attr.ctime = (int) buf.getLong(offset);
                attr.ctimeNano = buf.getInt(offset + 8);
                offset += 12;
                break;
            case timeModify:
                attr.mtime = (int) buf.getLong(offset);
                attr.mtimeNano = buf.getInt(offset + 8);
                offset += 12;
                break;
            //r w
//            case timeBackup:
//            case timeCreate:
            case timeModifySet:
                int setMtime = buf.getInt(offset);
                offset += 4;
                switch (setMtime) {
                    case NFS4_SET_TO_SERVER_TIME:
                        attr.hasMtime = 1;
                        break;
                    case NFS4_SET_TO_CLIENT_TIME:
                        attr.hasMtime = 2;
                        attr.mtime = (int) buf.getLong(offset);
                        attr.mtimeNano = buf.getInt(offset + 8);
                        offset += 12;
                        break;
                }
                break;
            case mountedOnFileId:
                long mountedOnFileId0 = buf.getLong(offset);
                extraAttrMap.put(mountedOnFileId, mountedOnFileId0);
                offset += 8;
                break;
            case fsLayoutType:
                int fsLayoutType0 = buf.getInt(offset);
                verifyRes.add(fsLayoutType0 == 0);
                offset += 4;
                break;
        }
        finalOffset[0] = offset;

    }

    public static void readMask3(ByteBuf buf, int[] finalOffset, int mask, ObjAttr attr, Map<Integer, Object> extraAttrMap, List<Boolean> verifyRes) {
        int offset = finalOffset[0];
        switch (mask) {
            //pnfs
            case layoutType:
            case layoutBlksize:
                offset += 4;
                break;
            case suppattrExclCreat:
              /*  int len = 2;
                buf.setInt(offset, len);
                int[] sMask = {0x00001010, 0x00000032};
                buf.setInt(offset + 4, sMask[0]);
                buf.setInt(offset + 8, sMask[1]);*/
                offset += 12;
                break;
            case modeUmask:
                int mode = buf.getInt(offset);
                int umask = buf.getInt(offset + 4);
                attr.hasMode = 1;
                attr.mode = mode & ~umask;
                offset += 8;
                break;
        }
        finalOffset[0] = offset;
    }

    public void writeMask1(ByteBuf buf, int[] finalOffset, int mask) {
        int offset = finalOffset[0];
        switch (mask) {
            case supportedAttrs:
                buf.setInt(offset, supportAttr.length);
                for (int i = 0; i < supportAttr.length; i++) {
                    buf.setInt(offset + 4 * (i + 1), supportAttr[i]);
                }
                offset += (supportAttr.length + 1) * 4;
                break;
            case type:
                int type = 2;
                if (inode != null) {
                    type = getType(inode);
                }
                buf.setInt(offset, type);
                offset += 4;
                break;
            case expireType:
            case uniqueHandle:
            case nameAttr:
            case acl:
            case rDAttrError:
            case caseInSensitive:
            case chownRestricted:
            case aclSupport:
                //表示acl支持权限的allow,deny
                //可以更改组，所有者
                //文件名区分大小写
                //readDir查属性状态 ok
                //0：句柄永不过期
                //1：句柄在时间点过期
                //2：句柄在时间段内过期
                buf.setInt(offset, 0);
                offset += 4;
                break;
            case change:
                long cTime = inode != null ? inode.getCtime() : System.currentTimeMillis() / 1000;
                buf.setLong(offset, cTime);
                offset += 8;
                break;
            case size:
                long size = inode == null ? 0 : inode.getSize();
                buf.setLong(offset, size);
                offset += 8;
                break;
            case linkSupport:
            case symlinkSupport:
            case homogeneous:
            case canSetTime:
            case casePreserving:
                //保留文件名大小写
                buf.setInt(offset, 1);
                offset += 4;
                break;
            case fsId:
//                long majorId = fsid;
//                long minorId = fsid;
                long majorId = 0;
                long minorId = 0;
                buf.setLong(offset, majorId);
                buf.setLong(offset + 8, minorId);
                offset += 16;
                break;
            case leaseTime:
                //文件系统的租约时间
                buf.setInt(offset, NFS4_LEASE_TIME);
                offset += 4;
                break;
            case fileHandle:
                offset += fh2.writeStruct(buf, offset);
                break;
            case fileId:
                long fileId = inode == null ? 0 : inode.getNodeId();
                buf.setLong(offset, fileId);
                offset += 8;
                break;
            //fileFree=fileTotal-创建对象数
            //fileAvail=fileFree+创建对象中当前未被open数
            //文件句柄可用数
            case filesAvail:
                buf.setLong(offset, Long.MAX_VALUE);
                offset += 8;
                break;
            //文件句柄还未创建数
            case filesFree:
                buf.setLong(offset, Long.MAX_VALUE);
                offset += 8;
                break;
            //文件句柄总可使用数
            case filesTotal:
                buf.setLong(offset, Long.MAX_VALUE);
                offset += 8;
                break;
            case fsLocations:
                //pathname components byte[]
                buf.setInt(offset, 0);
                //byte[]server pathname
                buf.setInt(offset + 4, 0);
                offset += 8;
                break;
            case maxFileSize:
                buf.setLong(offset, MAX_UPLOAD_TOTAL_SIZE);
                offset += 8;
                break;
            case maxLink:
            case maxName:
                buf.setInt(offset, 255);
                offset += 4;
                break;
            case maxRead:
            case maxWrite:
                buf.setLong(offset, (1048576));
                offset += 8;
                break;

        }
        finalOffset[0] = offset;
    }

    public void writeMask2(ByteBuf buf, int[] finalOffset, int mask) {
        int offset = finalOffset[0];
        switch (mask) {
            case mode:
                int mode = inode == null ? 0755 : inode.getMode() & 4095;
                buf.setInt(offset, mode);
                offset += 4;
                break;
            case noTrunc:
                buf.setInt(offset, 1);
                offset += 4;
                break;
            case numLinks:
                int numLink = inode == null ? 1 : inode.getLinkN();
                buf.setInt(offset, numLink);
                offset += 4;
                break;
            case owner:
                int uid = inode == null ? 0 : inode.getUid();
                byte[] owner = String.valueOf(uid).getBytes();
                int ownerLen = owner.length;
                buf.setInt(offset, ownerLen);
                buf.setBytes(offset + 4, owner);
                offset += 4 + (ownerLen + 3) / 4 * 4;
                break;
            case ownerGroup:
                int gid = inode == null ? 0 : inode.getGid();
                byte[] group = String.valueOf(gid).getBytes();
                int groupLen = group.length;
                buf.setInt(offset, groupLen);
                buf.setBytes(offset + 4, group);
                offset += 4 + (groupLen + 3) / 4 * 4;
                break;
            case rawDev:
                buf.setInt(offset, inode == null ? 0 : inode.getMajorDev());
                buf.setInt(offset + 4, inode == null ? 0 : inode.getMinorDev());
                offset += 8;
                break;
            case spaceAvail:
            case spaceFree:
                buf.setLong(offset, this.freeBytes);
                offset += 8;
                break;
            case spaceTotal:
                buf.setLong(offset, this.totalBytes);
                offset += 8;
                break;
            case spaceUsed:
                long spaceUsed = 0;
                if (inode != null && (inode.getMode() & S_IFMT) != S_IFLNK) {
                    spaceUsed = inode.getSize() % 4096 == 0 ? inode.getSize() : (inode.getSize() / 4096 + 1) * 4096;
                }
                buf.setLong(offset, spaceUsed);
                offset += 8;
                break;
            case timeAccess:
                long aTime = inode == null ? System.currentTimeMillis() / 1000 : inode.getAtime();
                int aTimeSec = inode == null ? (int) (System.nanoTime() % ONE_SECOND_NANO) : inode.getAtimensec();
                buf.setLong(offset, aTime);
                buf.setInt(offset + 8, aTimeSec);
                offset += 12;
                break;
            case timeDelta:
                //seconds,nSeconds
                buf.setLong(offset, 0);
                buf.setInt(offset + 8, 1000000);
                offset += 12;
                break;
            case timeMetaData:
                long cTime = inode == null ? System.currentTimeMillis() / 1000 : inode.getCtime();
                int cTimeSec = inode == null ? (int) (System.nanoTime() % ONE_SECOND_NANO) : inode.getCtimensec();
                buf.setLong(offset, cTime);
                buf.setInt(offset + 8, cTimeSec);
                offset += 12;
                break;
            case timeModify:
                long mTime = inode == null ? System.currentTimeMillis() / 1000 : inode.getMtime();
                int mTimeSec = inode == null ? (int) (System.nanoTime() % ONE_SECOND_NANO) : inode.getMtimensec();
                buf.setLong(offset, mTime);
                buf.setInt(offset + 8, mTimeSec);
                offset += 12;
                break;
            case mountedOnFileId:
                long fileId = inode == null ? ROOT_INODE.getNodeId() : fsid;
                buf.setLong(offset, fileId);
                offset += 8;
                break;
            case fsLayoutType:
                buf.setInt(offset, 0);
                offset += 4;
                break;
        }
        finalOffset[0] = offset;

    }

    public void writeMask3(ByteBuf buf, int[] finalOffset, int mask) {
        int offset = finalOffset[0];
        switch (mask) {
//            case layoutType:
//                break;
//            case layoutBlksize:
//                //blkSize 数据块大小
//                buf.setInt(offset, 4096);
//                offset += 4;
//                break;
//            case suppattrExclCreat:
//                //支持acl,size,mode,owner,ownerGroup属性的排除创建
//                int len = 2;
//                buf.setInt(offset, len);
//                int[] sMask = {0x00001010, 0x00000032};
//                buf.setInt(offset + 4, sMask[0]);
//                buf.setInt(offset + 8, sMask[1]);
//                offset += 12;
//                break;
            case modeUmask:
                offset += 4;
                break;
        }
        finalOffset[0] = offset;
    }

    public void mapToMask() {
        supportAttr = SUPPORT_ATTR[minorVersion];
        if (mask.length > supportAttr.length) {
            int[] realMask = new int[supportAttr.length];
            System.arraycopy(mask, 0, realMask, 0, realMask.length);
            mask = realMask;
        }
        for (int i = 0; i < mask.length; i++) {
            mask[i] = mask[i] & supportAttr[i];
        }
        for (int i = 0; i < mask.length; i++) {
            switch (i) {
                case 0:
                    mask1s = getMask(Mask1.class, mask[0]);
                    break;
                case 1:
                    mask2s = getMask(Mask2.class, mask[1]);
                    break;
                case 2:
                    mask3s = getMask(Mask3.class, mask[2]);
                    break;
            }
        }
    }

    public static <T extends Mask> List<T> getMask(Class<T> clazz, int mask) {
        return Arrays.stream(clazz.getEnumConstants()).filter(value -> (value.getMask() & mask) != 0)
                .collect(Collectors.toList());
    }

    public static int getType(Inode inode) {
        switch (inode.getMode() & FsConstants.S_IFMT) {
            case FsConstants.S_IFDIR:
                return FAttr3.fType.NF_DIR.type;
            case FsConstants.S_IFLNK:
                return FAttr3.fType.NF_LINK.type;
            case FsConstants.S_IFREG:
                return FAttr3.fType.NF_REG.type;
            case FsConstants.S_IFBLK:
                return FAttr3.fType.NF_BLK.type;
            case FsConstants.S_IFCHR:
                return FAttr3.fType.NF_CHR.type;
            case FsConstants.S_IFFIFO:
                return FAttr3.fType.NF_FIFO.type;
            default:
                return FAttr3.fType.NF_BAD.type;
        }
    }

    public static boolean verifyAttr(int maskIndex, int mask, Object value, Inode inode, int fsid, FAttr4 fAttr4, boolean same) {
        if (maskIndex == 0){
            switch (mask){
                case type:
                    return getType(inode) == (int) value;
                case change:
                    return inode.getCtime() == (long) value;
                default:
                    return same;
            }

        }else if (maskIndex == 1){
            switch (mask){
                case numLinks:
                    return inode.getLinkN() == (int) value;
                case rawDev:
                    int[] dev = (int[]) value;
                    return inode.getMajorDev() == dev[0] && inode.getMajorDev() == dev[1];
                case spaceAvail:
                    return fAttr4.freeBytes == (long) value;
                case spaceFree:
                    return fAttr4.freeBytes == (long) value;
                case spaceTotal:
                    return fAttr4.totalBytes == (long) value;
                case spaceUsed:
                    long spaceUsed = 0;
                    if (inode != null && (inode.getMode() & S_IFMT) != S_IFLNK) {
                        spaceUsed = inode.getSize() % 4096 == 0 ? inode.getSize() : (inode.getSize() / 4096 + 1) * 4096;
                    }
                    return spaceUsed == (long) value;
                case mountedOnFileId:
                    return fsid == (int) value;
                default:
                    return same;
            }
        }
        return same;
    }


}
