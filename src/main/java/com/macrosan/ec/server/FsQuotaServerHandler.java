package com.macrosan.ec.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.Utils;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.socketmsg.SocketReqMsg;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Mono;

import java.util.Map;

import static com.macrosan.constants.ServerConstants.CONTENT_LENGTH;
import static com.macrosan.ec.ECUtils.bytes2long;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.filesystem.FsConstants.S_IFLNK;
import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.filesystem.quota.FSQuotaConstants.*;
import static com.macrosan.filesystem.quota.FSQuotaRealService.*;
import static com.macrosan.filesystem.quota.FSQuotaScannerTask.*;
import static com.macrosan.filesystem.utils.FSQuotaUtils.getQuotaTypeKey;
import static com.macrosan.filesystem.utils.FSQuotaUtils.quotaConfigRename;
import static com.macrosan.message.jsonmsg.FSQuotaConfig.getCapKey;
import static com.macrosan.message.jsonmsg.FSQuotaConfig.getNumKey;
import static com.macrosan.message.jsonmsg.Inode.INODE_DELETE_MARK;

@Log4j2
public class FsQuotaServerHandler {

    public static Mono<Payload> scanDir(Payload payload) {

        //扫描目录下的文件数，目录数，文件大小
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        FSQuotaConfig config = Json.decodeValue(msg.get("config"), FSQuotaConfig.class);
        String prefix = config.getDirName();
        String bucket = config.getBucket();
        String lun = msg.get("lun");
        String vnode = msg.get("vnode");
        String marker = msg.get("marker");
        int type = config.getQuotaType();
        int uid = config.getUid();
        int gid = config.getGid();
        long stamp = config.getStartTime();
        String scanReferStamp = String.valueOf(stamp + QUOTA_UPDATE_DIR_EXPAND_TIME);


        String realMarker = Utils.getLatestMetaKey(vnode, bucket, marker);
        String realPrefix = Utils.getLatestMetaKey(vnode, bucket, prefix);
        byte[] realSeek;
        if (StringUtils.isNotBlank(marker)) {
            realSeek = realMarker.getBytes();
        } else {
            realSeek = realPrefix.getBytes();
        }
        int count = 0;
        int dirCount = 0;
        long capacity = 0;
        String newMarker = "";
        long scanCount = 0;
        String quotaTypeKey = getQuotaTypeKey(config);
        fsAddQuotaCache(bucket, quotaTypeKey, config);
        fsAddQuotaCheck(bucket, quotaTypeKey);
        String quotaTaskKey = FSQuotaUtils.getQuotaTaskKey(config.bucket, config.nodeId, config.quotaType);
        addScanLocalLock(quotaTaskKey);
        log.debug("扫描目录：" + prefix + "，bucket：" + bucket + "，lun：" + lun + "，vnode：" + vnode + "，type：" + type + "，uid：" + uid + "，gid：" + gid + "，stamp：" + stamp);
        try (MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator()) {
            iterator.seek(realSeek);

            if (iterator.isValid()) {
                if (StringUtils.isNotBlank(marker)) {
                    if (realMarker.equals(new String(iterator.key()))) {
                        iterator.next();
                    }
                } else {
                    //目录对象本身
                    if (realPrefix.equals(new String(iterator.key()))) {
                        iterator.next();
                    }
                }
            }

            while (iterator.isValid() && new String(iterator.key()).startsWith(realPrefix)) {

                Inode inode = null;
                if (MAX_SCAN_COUNT <= scanCount++) {
                    break;
                }
                MetaData metaData = Json.decodeValue(new String(iterator.value()), MetaData.class);
                if (metaData.deleteMark || metaData.deleteMarker) {
                    iterator.next();
                } else {

                    //只扫描设置配额时刻之前的文件以及对象
                    if (metaData.stamp.compareTo(scanReferStamp) >= 0) {
                        iterator.next();
                        continue;
                    }
                    if (metaData.inode > 0) {
                        String inodeKey = Inode.getKey(vnode, bucket, metaData.inode);
                        byte[] inodeBytes = MSRocksDB.getRocksDB(lun).get(inodeKey.getBytes());
                        if (inodeBytes != null) {
                            inode = Json.decodeValue(new String(inodeBytes), Inode.class);
                            //对于过大的文件，根据inodeData的size当做扫描了多次，防止过多的大文件导致扫描超时的情况
                            if (inode.getInodeData().size() > 1) {
                                scanCount += (inode.getInodeData().size() - 1);
                            }
                            if (type == FS_USER_QUOTA && uid != inode.getUid()) {
                                iterator.next();
                                continue;
                            }
                            if (type == FS_GROUP_QUOTA && gid != inode.getGid()) {
                                iterator.next();
                                continue;
                            }
                            //对于设置配额前后两分钟之内有修改的文件进行跳过
                            String quotaStartStampAndSize = null;
                            if (inode.getQuotaStartStampAndSize() != null) {
                                quotaStartStampAndSize = inode.getQuotaStartStampAndSize().get(quotaTypeKey);
                            }
                            //如果文件,metadata.stamp 在配额设置的时间之后,且在扫描的过程中,未进行过修改

                            if (metaData.stamp.compareTo(scanReferStamp) >= 0
                                    && StringUtils.isBlank(quotaStartStampAndSize) /*&& nowStamp.compareTo(scanReferStamp) < 0*/) {
                                iterator.next();
                                continue;
                            }

                            if (inode.getLinkN() != INODE_DELETE_MARK.getLinkN()) {
                                count++;
                                long deltaCap = inode.getSize();
                                if (StringUtils.isNotBlank(quotaStartStampAndSize)) {
                                    //区分多类型，配额扫描同一个文件的情况
                                    String[] split = quotaStartStampAndSize.split("-");
                                    String startStamp = split[0];
                                    //通过对比时间戳，确保是同一个配额记录的。
                                    if (startStamp.equals(String.valueOf(stamp))) {
                                        //如果文件创建时间在实时计数之后，则该处不再次计算
                                        String createS3InodeTime = inode.getXAttrMap().get(CREATE_S3_INODE_TIME);
                                        if ((metaData.stamp.compareTo(scanReferStamp) >= 0 && inode.getLinkN() == 1)
                                                || (StringUtils.isNotBlank(createS3InodeTime) && createS3InodeTime.compareTo(scanReferStamp) >= 0)
                                        ) {
                                            count--;
                                        }
                                        deltaCap = Long.parseLong(split[1]);
                                    }
                                }
                                //硬链接只统计一次容量，软连接不统计容量
                                if ((inode.getLinkN() > 1 && metaData.cookie != inode.getCookie())
                                        || (inode.getMode() & S_IFMT) == S_IFLNK) {
                                    deltaCap = 0;
                                }
                                capacity += deltaCap;
                                if (metaData.key.endsWith("/")) {
                                    dirCount++;
                                }
                            }
                        } else {
                            // 扫描的过程中，文件客户端在访问s3上传的对象，metadata更新成功，inode元数据还未更新成功。
                            if (config.getQuotaType() == FS_DIR_QUOTA || needCount(config, metaData)) {
                                if (metaData.stamp.compareTo(scanReferStamp) < 0) {
                                    count++;
                                    long deltaCap = metaData.endIndex + 1;
                                    if (StringUtils.isNotBlank(metaData.getSysMetaData())) {
                                        //读取content-length字段
                                        Map<String, String> sysMetaMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                                        });
                                        try {
                                            deltaCap = Long.parseLong(sysMetaMap.getOrDefault(CONTENT_LENGTH, "0"));
                                        } catch (NumberFormatException ignored) {

                                        }
                                    }
                                    capacity += deltaCap;
                                    if (metaData.key.endsWith("/")) {
                                        dirCount++;
                                    }
                                }
                            }
                        }
                    } else if (config.getQuotaType() == FS_DIR_QUOTA || needCount(config, metaData)) {
                        if (metaData.stamp.compareTo(scanReferStamp) < 0) {
                            count++;
                            capacity += (metaData.endIndex + 1);
                            if (metaData.key.endsWith("/")) {
                                dirCount++;
                            }
                        }
                    }
                    iterator.next();
                    newMarker = metaData.key;
                }
                if (count >= MAX_SCAN_COUNT) {
                    break;
                }
            }

        } catch (Exception e) {
            log.error("", e);
            removeScanLocalLock(quotaTaskKey);
            return Mono.just(ERROR_PAYLOAD);
        }
        if (StringUtils.isBlank(newMarker)) {
            removeScanLocalLock(quotaTaskKey);
        }
        log.debug("扫描完成：" + count + "个文件，" + capacity + "个字节，" + dirCount + "个目录");
        FSQuotaScanResultVo res = new FSQuotaScanResultVo()
                .setFileCount(count)
                .setFileTotalSize(capacity)
                .setDirCount(dirCount)
                .setMarker(newMarker);
        return Mono.just(DefaultPayload.create(Json.encode(res), SUCCESS.name()));
    }

    public static boolean needCount(FSQuotaConfig fsQuotaConfig, MetaData metaData) {
        if (fsQuotaConfig.getQuotaType() == FS_USER_QUOTA && fsQuotaConfig.getUid() == 0) {
            return true;
        }
        return fsQuotaConfig.getQuotaType() == FS_GROUP_QUOTA && fsQuotaConfig.getGid() == 0;
    }

    public static Mono<Payload> delQuota(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        long dirNodeId = Long.parseLong(msg.get("dirNodeId"));
        String lun = msg.get("lun");
        String vnode = msg.get("vnode");
        int type = Integer.parseInt(msg.get("type"));
        int id = Integer.parseInt(msg.get("id"));
        try {
            if (StringUtils.isNotBlank(vnode)) {
                delQuota(bucket, dirNodeId, lun, vnode, type, id);
            }
            String quotaTypeKey = getQuotaTypeKey(bucket, dirNodeId, type, id);
            fsDeleteQuotaCache(bucket, quotaTypeKey);
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(ERROR_PAYLOAD);
        }

        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static void delQuota(String bucket, long dirNodeId, String lun, String vnode, int type, int id) throws RocksDBException {
        MSRocksDB db = MSRocksDB.getRocksDB(lun);
        String capacityKey = getCapKey(vnode, bucket, dirNodeId, type, id);
        String objNumKey = getNumKey(vnode, bucket, dirNodeId, type, id);
        db.delete(capacityKey.getBytes());
        db.delete(objNumKey.getBytes());

    }

    public static Mono<Payload> getQuotaInfo(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        long dirNodeId = Long.parseLong(msg.get("dirNodeId"));
        int type = Integer.parseInt(msg.get("type"));
        int id = Integer.parseInt(msg.get("id_"));
        String lun = msg.get("lun");
        String vnode = msg.get("vnode");
        DirInfo dirInfo = new DirInfo();
        try {
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            String capacityKey = getCapKey(vnode, bucket, dirNodeId, type, id);
            String objNumKey = getNumKey(vnode, bucket, dirNodeId, type, id);
            byte[] bytes = db.get(capacityKey.getBytes());
            if (bytes != null) {
                dirInfo.setUsedCap(String.valueOf(bytes2long(bytes)));
            } else {
                return Mono.just(DefaultPayload.create(Json.encode(DirInfo.NOT_FOUND_DIR_INFO), NOT_FOUND.name()));
            }
            bytes = db.get(objNumKey.getBytes());
            if (bytes != null) {
                dirInfo.setUsedObjects(String.valueOf(bytes2long(bytes)));
            } else {
                return Mono.just(DefaultPayload.create(Json.encode(DirInfo.NOT_FOUND_DIR_INFO), NOT_FOUND.name()));
            }
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(DefaultPayload.create(Json.encode(DirInfo.ERROR_DIR_INFO), ERROR.name()));
        }
        return Mono.just(DefaultPayload.create(Json.encode(dirInfo), SUCCESS.name()));
    }

    public static Mono<Payload> syncQuotaCache(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String config = msg.get("config");
        String opt = msg.get("opt");
        String dirName = msg.get("dirName");
        String dirNodeId = msg.get("dirNodeId");
        if (config != null) {
            FSQuotaConfig fsQuotaConfig = Json.decodeValue(config, FSQuotaConfig.class);
            fsAddQuotaCache(bucket, FSQuotaUtils.getQuotaTypeKey(fsQuotaConfig), fsQuotaConfig);
        }
        if (QUOTA_CACHE_MODIFY_OPT.equals(opt)) {
            quotaConfigRename(bucket, Long.parseLong(dirNodeId), dirName);
        }
        return Mono.just(SUCCESS_PAYLOAD);
    }
}
