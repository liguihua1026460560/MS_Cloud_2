package com.macrosan.ec.server;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.Utils;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.utils.msutils.MsException;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.SysConstants.ROCKS_LATEST_KEY;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;

@Log4j2
public class FsServerHandler {
    /**
     * 在key后面不区分大小写搜索 file。
     */
    private static String smbLookup(String lun, String vnode, String key, String file) {
        //大小写不影响length
        String realFileName = file.toLowerCase();
        String realDirName = file.toLowerCase() + "/";

        try {
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            if (db == null) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "no rocksdb");
            }

            try (MSRocksIterator iterator = db.newIterator()) {
                iterator.seek(key.getBytes());

                while (iterator.isValid()) {
                    String curKey = new String(iterator.key());
                    if (!curKey.startsWith(key)) {
                        break;
                    }

                    String fileName = curKey.substring(key.length());

                    if (realFileName.equalsIgnoreCase(fileName) || fileName.toLowerCase().startsWith(realDirName)) {
                        return curKey.substring(0, key.length() + file.length());
                    } else {
                        int spIndex;
                        if ((spIndex = fileName.indexOf("/")) >= 0) {
                            String fileNamePrefix = fileName.substring(0, spIndex + 1);
                            byte[] next = (key + fileNamePrefix).getBytes();
                            next[next.length - 1] += 1;
                            iterator.seek(next);
                        } else {
                            iterator.next();
                        }
                    }
                }
            }

            return "";
        } catch (Exception e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "smbLookup fail", e);
        }
    }

    public static Mono<Payload> smbLookupDir(String lun, String vnode, String key, boolean isReName) {
        int vnodeEnd = ROCKS_LATEST_KEY.length() + vnode.length() + 1;
        int bucketEnd = vnodeEnd + key.substring(vnodeEnd).indexOf(File.separator) + 1;
        String keyPrefix = key.substring(0, bucketEnd);

        String object = key.substring(bucketEnd);

        String[] realPaths = object.split("/");
        String realKey = key;
        for (String realPath : realPaths) {
            realKey = smbLookup(lun, vnode, keyPrefix, realPath);
            if (StringUtils.isBlank(realKey)) {
                return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_INODE), NOT_FOUND.name()));
            }

            keyPrefix = realKey + "/";
        }

        return lookup0(lun, realKey, vnode, isReName, new AtomicBoolean());
    }

    //输入的所有dir都是存在的。只有最后一个file需要搜索
    public static Mono<Payload> smbLookupFile(String lun, String vnode, String key, boolean isReName) {
        int spIndex = key.lastIndexOf("/") + 1;
        String realKey = smbLookup(lun, vnode, key.substring(0, spIndex), key.substring(spIndex));
        if (StringUtils.isBlank(realKey)) {
            return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_INODE), NOT_FOUND.name()));
        } else {
            return lookup0(lun, realKey, vnode, isReName, new AtomicBoolean());
        }
    }

    public static Mono<Payload> lookup0(String lun, String key, String vnode, boolean isReName, AtomicBoolean caseRetry) {
        try {
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            if (db == null) {
                return Mono.just(DefaultPayload.create(Json.encode(ERROR_INODE), ERROR.name()));
            }

            byte[] value = null;
            try (MSRocksIterator iterator = db.newIterator()) {
                String realKey = key;
                iterator.seek(realKey.getBytes());
                boolean reSeek = false;
                while (iterator.isValid()) {
                    String seekKey = new String(iterator.key());
                    if (seekKey.equals(realKey) || (seekKey.equals(realKey + "/") && !reSeek)) {
                        MetaData metaData = Json.decodeValue(new String(iterator.value()), MetaData.class);
                        if (metaData.inode > 0) {
                            String inodeKey = Inode.getKey(vnode, metaData.bucket, metaData.inode);
                            value = db.get(inodeKey.getBytes());
                            if (value == null) {
                                value = Json.encode(Inode.notPutInode(metaData)).getBytes();
                            } else {
                                //已被删除
                                Inode i = Json.decodeValue(new String(value), Inode.class);
                                if (i.isDeleteMark()) {
                                    return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_INODE), NOT_FOUND.name()));
                                } else {
                                    i.setCookie(metaData.cookie);
                                    i.setObjName(metaData.key);
                                    Inode.mergeObjAcl(i, metaData);
                                    CifsUtils.setDefaultCifsMode(i);
                                    value = Json.encode(i).getBytes();
                                }
                            }
                        } else {
                            if (metaData.deleteMark || metaData.deleteMarker) {
                                return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_INODE), NOT_FOUND.name()));
                            } else {
                                Inode inode = new Inode()
                                        .setVersionNum(metaData.versionNum)
                                        .setBucket(metaData.getBucket())
                                        .setVersionId(metaData.getVersionId())
                                        .setReference(Utils.metaHash(metaData))
                                        .setObjName(metaData.getKey());
                                Inode.mergeObjAcl(inode, metaData);
                                value = Json.encode(inode).getBytes();
                            }
                        }
                        return Mono.just(DefaultPayload.create(value, SUCCESS.name().getBytes()));
                    } else if (seekKey.startsWith(realKey + "/") && !reSeek) {
                        //s3 dir
                        String metaKey = key.substring(ROCKS_LATEST_KEY.length()) + '/';
                        iterator.seek(metaKey.getBytes());
                        if (iterator.isValid()) {
                            String dirKey = new String(iterator.key());
                            if (dirKey.startsWith(metaKey)) {
                                MetaData metaData = Json.decodeValue(new String(iterator.value()), MetaData.class);
                                String obj = realKey.substring(Utils.getLatestMetaKey(vnode, metaData.bucket, "").length()) + "/";
                                Inode inode = new Inode()
                                        .setVersionNum("")
                                        .setReference(Utils.DEFAULT_META_HASH)
                                        .setBucket(metaData.getBucket())
                                        //多版本会出现重复的versionId，但是对象名不同，暂不处理
                                        .setVersionId(metaData.getVersionId())
                                        .setObjName(obj);
                                if (isReName) {
                                    // rename情况中的lookup用于查找newInode是否存在，不应当走当前分支，此时返回linkN=-3用于区分重发的rename请求
                                    // 设置nodeId=-3是为了避开lookup重新创建inode的分支
                                    inode.setLinkN(-3);
                                    inode.setNodeId(-3);
                                }
                                return Mono.just(DefaultPayload.create(Json.encode(inode).getBytes(), SUCCESS.name().getBytes()));
                            }
                        }

                        return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_INODE).getBytes(), NOT_FOUND.name().getBytes()));
                    } else if (seekKey.startsWith(realKey)) {
                        realKey = realKey + "/";
                        reSeek = true;
                        iterator.seek(realKey.getBytes());
                    } else {
                        break;
                    }
                }
            }

            caseRetry.set(true);
            return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_INODE), NOT_FOUND.name()));
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(DefaultPayload.create(Json.encode(ERROR_INODE), ERROR.name()));
        }
    }

    public static Mono<Payload> checkDir(Payload payload) {
        try {
            SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
            String lun = msg.get("lun");
            String key = msg.get("key");

            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            if (db == null) {
                return Mono.just(ERROR_PAYLOAD);
            }

            try (MSRocksIterator iterator = db.newIterator()) {
                iterator.seek(key.getBytes());
                if (iterator.isValid()) {
                    String seekKey = new String(iterator.key());
                    if (seekKey.startsWith(key)) {
                        return Mono.just(SUCCESS_PAYLOAD);
                    }
                }
            }

            return Mono.just(DefaultPayload.create("", NOT_FOUND.name()));
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(ERROR_PAYLOAD);
        }
    }
}
