package com.macrosan.filesystem.cifs.api.smb2;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ErasureClient;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsConstants.NTStatus;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cifs.SMB2.SMB2Reply;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.call.smb2.GetInfoCall;
import com.macrosan.filesystem.cifs.reply.smb2.ErrorReply;
import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.*;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.filesystem.utils.acl.NFSACL;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.macrosan.constants.ErrorNo.QUOTA_INFO_NOT_EXITS;
import static com.macrosan.constants.SysConstants.REDIS_FS_QUOTA_INFO_INDEX;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.FILE_ATTRIBUTE_ARCHIVE;
import static com.macrosan.filesystem.FsConstants.NFSACLType.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.*;
import static com.macrosan.filesystem.FsConstants.NTStatus.STATUS_NO_MORE_ENTRIES;
import static com.macrosan.filesystem.cifs.call.smb2.GetInfoCall.*;
import static com.macrosan.filesystem.cifs.types.smb2.FsAttrInfo.*;
import static com.macrosan.filesystem.cifs.types.smb2.SecurityInfo.*;
import static com.macrosan.filesystem.quota.FSQuotaConstants.*;
import static com.macrosan.filesystem.utils.FSQuotaUtils.getQuotaBucketKey;
import static com.macrosan.filesystem.utils.FSQuotaUtils.getQuotaTypeKey;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;

@Log4j2
public class GetInfo {
    private static final Node nodeInstance = Node.getInstance();

    private static final Set<String> HAS_SEEK_QUOTA_INFO = new ConcurrentHashSet<>();

    public static Mono<SMB2Reply> getFsInfo(SMB2Reply reply, Session session, GetInfoCall call) {

        GetInfoReply body = new GetInfoReply();
        SMB2Header header = reply.getHeader();
        int fsid = header.getTid();
        String bucketName = NFSBucketInfo.getBucketName(fsid);
        SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(header.getCompoundRequest());

        switch (call.getFileInfoClass()) {
            case FSCC_FS_ATTRIBUTE_INFORMATION:
                FsAttrInfo fsAttrInfo = new FsAttrInfo();
                fsAttrInfo.setFsAttr(FILE_CASE_PRESERVED_NAMES | FILE_CASE_SENSITIVE_SEARCH |
                        FILE_SUPPORTS_OBJECT_IDS | FILE_UNICODE_ON_DISK | FILE_VOLUME_QUOTAS | FILE_PERSISTENT_ACLS);
                body.setInfo(fsAttrInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FS_DEVICE_INFORMATION:
                FsDeviceInfo fsDeviceInfo = new FsDeviceInfo();
                body.setInfo(fsDeviceInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FS_FULL_SIZE_INFORMATION: {
                long dirInodeId = Optional.ofNullable(fileInfo).map(f -> f.inodeId).orElse(1L);

                return FSQuotaUtils.findFinalMinFsQuotaConfig(bucketName, dirInodeId)
                        .flatMap(tuple -> {
                            FSQuotaConfig config = tuple.getT1();
                            long inodeId = tuple.getT2();

                            if (inodeId == 1L) {
                                return ErasureClient.reduceBucketInfo(bucketName)
                                    .filter(BucketInfo::isAvailable)
                                    .map(BucketInfo::getBucketStorage)
                                    .doOnError(e -> log.error("get bucket used capacity error", e))
                                    .defaultIfEmpty("0")
                                    .flatMap(usedCapacityStr -> {
                                        FsFullSizeInfo info = buildFullSizeInfo(config, new DirInfo().setUsedCap(usedCapacityStr));
                                        body.setInfo(info);
                                        reply.setBody(body);

                                        return Mono.just(reply);
                                    });
                            }

                            return FSQuotaRealService.getFsQuotaInfo(bucketName, inodeId, FS_DIR_QUOTA, 0)
                                .flatMap(dirInfo -> {
                                    if (DirInfo.isErrorInfo(dirInfo)) {
                                        return Mono.<SMB2Reply>empty();
                                    } else {
                                        FsFullSizeInfo info = buildFullSizeInfo(config, dirInfo);
                                        body.setInfo(info);
                                        reply.setBody(body);
                                    }
                                    return Mono.just(reply);
                                });

                        })
                        // 只有当目录以及其任意祖先目录都不存在配额时才会进这里
                        .switchIfEmpty(Mono.defer(() -> {
                            FsFullSizeInfo defaultFullSizeInfo = buildDefaultFullSizeInfo(bucketName);
                            body.setInfo(defaultFullSizeInfo);
                            reply.setBody(body);
                            return Mono.just(reply);
                        }));
            }
            case FSCC_FS_OBJECTID_INFORMATION:
                FsObjectIdInfo fsObjectIdInfo = new FsObjectIdInfo();
                fsObjectIdInfo.setObjectId(new IOCTLSubReply.ObjectId1().setFileId(call.getFileId()));
                body.setInfo(fsObjectIdInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FS_VOLUME_INFORMATION:
                //卷信息
                FsVolumeInfo fsVolumeInfo = new FsVolumeInfo();
                fsVolumeInfo.label = bucketName.getBytes(StandardCharsets.UTF_16LE);
                fsVolumeInfo.serialNum = header.getTid();
                body.setInfo(fsVolumeInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FS_SIZE_INFORMATION: {
                long dirInodeId = Optional.ofNullable(fileInfo).map(f -> f.inodeId).orElse(1L);

                return FSQuotaUtils.findFinalMinFsQuotaConfig(bucketName, dirInodeId)
                        .flatMap(tuple -> {
                            FSQuotaConfig config = tuple.getT1();
                            long inodeId = tuple.getT2();

                            if (inodeId == 1L) {
                                return ErasureClient.reduceBucketInfo(bucketName)
                                    .filter(BucketInfo::isAvailable)
                                    .map(BucketInfo::getBucketStorage)
                                    .doOnError(e -> log.error("get bucket used capacity error", e))
                                    .defaultIfEmpty("0")
                                    .flatMap(usedCapacityStr -> {
                                        FsSizeInfo info = buildFsSizeInfo(config, new DirInfo().setUsedCap(usedCapacityStr));
                                        body.setInfo(info);
                                        reply.setBody(body);

                                        return Mono.just(reply);
                                    });
                            }

                            return FSQuotaRealService.getFsQuotaInfo(bucketName, inodeId, FS_DIR_QUOTA, 0)
                                .flatMap(dirInfo -> {
                                    if (DirInfo.isErrorInfo(dirInfo)) {
                                        return Mono.<SMB2Reply>empty();
                                    } else {
                                        FsSizeInfo info = buildFsSizeInfo(config, dirInfo);
                                        body.setInfo(info);
                                        reply.setBody(body);
                                    }
                                    return Mono.just(reply);
                                });
                        })
                        // 只有当目录以及其任意祖先目录都不存在配额时才会进这里
                        .switchIfEmpty(Mono.defer(() -> {
                            FsSizeInfo defaultFsSizeInfoInfo = buildDefaultFsSizeInfo(bucketName);
                            body.setInfo(defaultFsSizeInfoInfo);
                            reply.setBody(body);
                            return Mono.just(reply);
                        }));
            }
            case FSCC_FS_SECTOR_SIZE_INFORMATION:
                FsSectorSizeInfo fsSectorSizeInfo = new FsSectorSizeInfo();
                body.setInfo(fsSectorSizeInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FS_QUOTA_INFORMATION:
                FsQuotaInfo fsQuotaInfo = new FsQuotaInfo();
                if (StringUtils.isNotBlank(fileInfo.obj) && fileInfo.obj.endsWith(SMB_CAP_QUOTA_FILE_NAME)) {
                    String dirName = CifsUtils.getParentDirName(fileInfo.obj);
                    return FsUtils.lookup(bucketName, dirName, null, false, false, -1, null)
                            .flatMap(inode -> {
                                if (InodeUtils.isError(inode)) {
                                    body.setInfo(fsQuotaInfo);
                                    reply.setBody(body);
                                    return Mono.just(reply);
                                }
                                return RedisConnPool.getInstance().getReactive(REDIS_FS_QUOTA_INFO_INDEX)
                                        .hget(getQuotaBucketKey(fileInfo.bucket), getQuotaTypeKey(fileInfo.bucket, inode.getNodeId(), FS_DIR_QUOTA, -1))
                                        .defaultIfEmpty("")
                                        .flatMap(configStr -> {
                                            if (StringUtils.isBlank(configStr) && inode.getNodeId() == 1) {
                                                return FSQuotaRealService.getFsQuotaConfig(fileInfo.bucket, inode.getNodeId(), FS_DIR_QUOTA, -1)
                                                        .flatMap(fsQuotaConfig -> {
                                                            fsQuotaInfo.setDefaultQuotaThreshold(fsQuotaConfig.getCapacitySoftQuota());
                                                            fsQuotaInfo.setDefaultQuotaLimit(fsQuotaConfig.getCapacityHardQuota());
                                                            fsQuotaInfo.setFsControlFlags(fsQuotaConfig.cifsQuotaFlags);
                                                            body.setInfo(fsQuotaInfo);
                                                            reply.setBody(body);
                                                            return Mono.just(reply);
                                                        });
                                            } else if (StringUtils.isNotBlank(configStr)) {
                                                FSQuotaConfig fsQuotaConfig = Json.decodeValue(configStr, FSQuotaConfig.class);
                                                fsQuotaInfo.setDefaultQuotaThreshold(fsQuotaConfig.getCapacitySoftQuota());
                                                fsQuotaInfo.setDefaultQuotaLimit(fsQuotaConfig.getCapacityHardQuota());
                                                fsQuotaInfo.setFsControlFlags(fsQuotaConfig.cifsQuotaFlags);
                                                body.setInfo(fsQuotaInfo);
                                                reply.setBody(body);
                                                return Mono.just(reply);
                                            }
                                            body.setInfo(fsQuotaInfo);
                                            reply.setBody(body);
                                            return Mono.just(reply);
                                        });
                            });

                }
                body.setInfo(fsQuotaInfo);
                reply.setBody(body);
                return Mono.just(reply);
            default:
                break;
        }

        reply.getHeader().setStatus(NTStatus.STATUS_ACCESS_DENIED);
        return Mono.just(reply);
    }

    private static final byte[] NEED_FID = new byte[256];

    static {
        NEED_FID[FSCC_FILE_ALL_INFORMATION & 0xff] = 1;
        NEED_FID[FSCC_FILE_INTERNAL_INFORMATION & 0xff] = 1;
        NEED_FID[FSCC_FILE_NETWORK_OPEN_INFORMATION & 0xff] = 1;
        NEED_FID[FSCC_FILE_BASIC_INFORMATION & 0xff] = 1;
        NEED_FID[FSCC_FILE_STANDARD_INFORMATION & 0xff] = 1;
        NEED_FID[FSCC_FILE_STREAM_INFORMATION & 0xff] = 1;
        NEED_FID[FSCC_FILE_NORMALIZED_NAME_INFORMATION & 0xff] = 1;
        NEED_FID[FSCC_FILE_EA_INFORMATION & 0xff] = 1;
    }


    public static Mono<SMB2Reply> getFileInfo(SMB2Reply reply, SMB2Header header, GetInfoCall call, Session session) {
        GetInfoReply body = new GetInfoReply();
        SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(header.getCompoundRequest());

        //需要fileInfo的接口，并且fileInfo是空的，返回STATUS_FILE_CLOSED
        if (fileInfo == null && NEED_FID[call.getFileInfoClass() & 0xff] == 1) {
            reply.getHeader().setStatus(NTStatus.STATUS_FILE_CLOSED);
            return Mono.just(reply);
        }

        Mono<Inode> getInodeMono;
        //是否需要重新获得inode
        if (true) {
            getInodeMono = Mono.just(fileInfo.openInode);
        } else {
            getInodeMono = nodeInstance.getInode(fileInfo.bucket, fileInfo.inodeId);
        }

        switch (call.getFileInfoClass()) {
            case FSCC_FILE_ALL_INFORMATION:
                return getInodeMono
                        .flatMap(inode -> {
                            if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                reply.getHeader().status = NTStatus.STATUS_OBJECT_NAME_NOT_FOUND;
                                return Mono.just(reply);
                            }
                            if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                reply.getHeader().status = NTStatus.STATUS_IO_DEVICE_ERROR;
                                return Mono.just(reply);
                            }

                            inode.setObjName(fileInfo.obj);
                            String s3Id = session.getTreeAccount(header.getTid());

                            body.setInfo(FileAllInfo.mapToFileAllInfo(inode, s3Id));
                            reply.setFileInfoBody(body, call.getResponseLen());
                            return Mono.just(reply);
                        });

            case FSCC_FILE_BASIC_INFORMATION:
                FileBasicInfo fileBasicInfo = new FileBasicInfo();
                if (fileInfo.inodeId > 0) {
                    return getInodeMono
                            .flatMap(inode -> {
                                if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                    reply.getHeader().status = FsConstants.NTStatus.STATUS_OBJECT_NAME_NOT_FOUND;
                                    return Mono.just(reply);
                                }
                                if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                    reply.getHeader().status = FsConstants.NTStatus.STATUS_IO_DEVICE_ERROR;
                                    return Mono.just(reply);
                                }
                                body.setInfo(FileBasicInfo.mapToFileBasicInfo(inode));
                                reply.setFileInfoBody(body, call.getResponseLen());
                                return Mono.just(reply);
                            });
                }
                fileBasicInfo.setMode(FsConstants.FILE_ATTRIBUTE_DIRECTORY);
                body.setInfo(fileBasicInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FILE_INTERNAL_INFORMATION:
                FileInternalInfo internalInfo = new FileInternalInfo();
                if (fileInfo.inodeId > 0) {
                    return getInodeMono
                            .flatMap(inode -> {
                                if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                    reply.getHeader().status = NTStatus.STATUS_OBJECT_NAME_NOT_FOUND;
                                    return Mono.just(reply);
                                }
                                if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                    reply.getHeader().status = NTStatus.STATUS_IO_DEVICE_ERROR;
                                    return Mono.just(reply);
                                }
                                internalInfo.setNodeId(inode.getNodeId());
                                body.setInfo(internalInfo);
                                reply.setFileInfoBody(body, call.getResponseLen());
                                return Mono.just(reply);
                            });
                }
                internalInfo.setNodeId(1);
                body.setInfo(internalInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FILE_MODE_INFORMATION:
                FileModeInfo fsModeInfo = new FileModeInfo();
                //对应于create操作中createOptions
//                fsModeInfo.mode =
                body.setInfo(fsModeInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FILE_ALIGNMENT_INFORMATION:
                FileAlignmentInfo fsAlignmentInfo = new FileAlignmentInfo();
//                fsAlignmentInfo.alignmentRequirement = FILE_BYTE_ALIGNMENT;
                body.setInfo(fsAlignmentInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FILE_ACCESS_INFORMATION:
                FileAccessInfo fsAccessInfo = new FileAccessInfo();
//                fsAccessInfo.accessFlags =;
                body.setInfo(fsAccessInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FILE_ALTERNATE_NAME_INFORMATION:
                FileAlternateNameInfo fsAlternateNameInfo = new FileAlternateNameInfo();
//                fsAlternateNameInfo.fileName =;
                body.setInfo(fsAlternateNameInfo);
                reply.setBody(body);
                return Mono.just(reply);
            // reparse
            case FSCC_FILE_ATTRIBUTE_TAG_INFORMATION:
                FileAttrTagInfo fsAttrTagInfo = new FileAttrTagInfo();
//                fsAttrTagInfo.fileAttr =;
//                if ()
//                fsAttrTagInfo.reparseTag =
                body.setInfo(fsAttrTagInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FILE_STANDARD_INFORMATION:
                FileStandardInfo standardInfo = new FileStandardInfo();
                if (fileInfo.inodeId > 0) {
                    return getInodeMono
                            .flatMap(inode -> {
                                if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                    reply.getHeader().status = NTStatus.STATUS_OBJECT_NAME_NOT_FOUND;
                                    return Mono.just(reply);
                                }
                                if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                    reply.getHeader().status = NTStatus.STATUS_IO_DEVICE_ERROR;
                                    return Mono.just(reply);
                                }
                                body.setInfo(FileStandardInfo.mapToFileStandardInfo(inode));
                                reply.setFileInfoBody(body, call.getResponseLen());
                                return Mono.just(reply);
                            });
                }

                body.setInfo(standardInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FILE_STREAM_INFORMATION:
                FileStreamInfo fileStreamInfo = new FileStreamInfo();
                //默认数据流
                fileStreamInfo.streamName = "::$DATA".getBytes(StandardCharsets.UTF_16LE);
                if (fileInfo.inodeId > 0) {
                    return getInodeMono
                            .flatMap(inode -> {
                                if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                    reply.getHeader().status = NTStatus.STATUS_OBJECT_NAME_NOT_FOUND;
                                    return Mono.just(reply);
                                }
                                if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                    reply.getHeader().status = NTStatus.STATUS_IO_DEVICE_ERROR;
                                    return Mono.just(reply);
                                }
                                //create操作后size
                                fileStreamInfo.streamSize = inode.getSize();
                                fileStreamInfo.allocationSize = CifsUtils.getAllocationSize(inode.getMode(), inode.getSize());
                                body.setInfo(fileStreamInfo);
                                reply.setFileInfoBody(body, call.getResponseLen());
                                return Mono.just(reply);
                            });
                }
                body.setInfo(fileStreamInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FILE_EA_INFORMATION:
                //extend attr
                FileEAInfo fileEAInfo = new FileEAInfo();
                fileEAInfo.eaSize = 0;
                body.setInfo(fileEAInfo);
                reply.setBody(body);
                return Mono.just(reply);
            case FSCC_FILE_NETWORK_OPEN_INFORMATION:
                FileNetworkOpenInfo fileNetworkOpenInfo = new FileNetworkOpenInfo();
                return getInodeMono
                        .flatMap(inode -> {
                            if (StringUtils.isBlank(inode.getObjName()) || inode.getObjName().endsWith(SMB_CAP_QUOTA_FILE_NAME)) {
                                fileNetworkOpenInfo.mode = FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM | FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_ARCHIVE;
                            } else {
                                long size = inode.getSize();
                                fileNetworkOpenInfo.creationTime = CifsUtils.nttime(inode.getCreateTime() * 1000L);
                                fileNetworkOpenInfo.lastAccessTime = CifsUtils.nttime(inode.getAtime() * 1000L) + inode.getAtimensec() / 100;
                                fileNetworkOpenInfo.lastWriteTime = CifsUtils.nttime(inode.getMtime() * 1000L) + inode.getMtimensec() / 100;
                                fileNetworkOpenInfo.lastChangeTime = CifsUtils.nttime(inode.getCtime() * 1000L) + inode.getCtimensec() / 100;
                                fileNetworkOpenInfo.allocationSize = CifsUtils.getAllocationSize(inode.getMode(), inode.getSize());
                                fileNetworkOpenInfo.endOfFile = size;
                                CifsUtils.setDefaultCifsMode(inode);
                                fileNetworkOpenInfo.mode = CifsUtils.changeToHiddenCifsMode(inode.getObjName(), inode.getCifsMode(), true);
                            }
                            body.setInfo(fileNetworkOpenInfo);
                            reply.setFileInfoBody(body, call.getResponseLen());
                            return Mono.just(reply);
                        });
            case FSCC_FILE_FULL_EA_INFORMATION:
                FileFullEAInfo fullEAInfo = new FileFullEAInfo();
                body.setInfo(fullEAInfo);
                reply.setBody(body);
                return Mono.just(reply);
            //now not supp
            case FSCC_FILE_COMPRESSION_INFORMATION:
                FileCompressionInfo fileCompressionInfo = new FileCompressionInfo();
                body.setInfo(fileCompressionInfo);
                reply.setBody(body);
                return Mono.just(reply);

            case FSCC_FILE_POSITION_INFORMATION:
                FilePositionInfo positionInfo = new FilePositionInfo();
                body.setInfo(positionInfo);
                reply.setBody(body);
                return Mono.just(reply);

            case FSCC_FILE_NORMALIZED_NAME_INFORMATION:
                FileNormalizedNameInfo fileNormalizedNameInfo = new FileNormalizedNameInfo();
                if (fileInfo.inodeId > 0) {
                    return getInodeMono
                            .flatMap(inode -> {
                                if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                    reply.getHeader().status = NTStatus.STATUS_OBJECT_NAME_NOT_FOUND;
                                    return Mono.just(reply);
                                }
                                if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                    reply.getHeader().status = NTStatus.STATUS_IO_DEVICE_ERROR;
                                    return Mono.just(reply);
                                }

                                fileNormalizedNameInfo.fileName = inode.getObjName().replace("/", "\\").getBytes(StandardCharsets.UTF_16LE);
                                body.setInfo(fileNormalizedNameInfo);
                                reply.setFileInfoBody(body, call.getResponseLen());
                                return Mono.just(reply);
                            });
                }
                body.setInfo(fileNormalizedNameInfo);
                reply.setBody(body);
                return Mono.just(reply);

            case FSCC_FILE_PIPE_INFORMATION:
                FilePipeInfo filePipeInfo = new FilePipeInfo();
                body.setInfo(filePipeInfo);
                reply.setBody(body);
                return Mono.just(reply);

            case FSCC_FILE_PIPE_LOCAL_INFORMATION:
                FilePipeLocalInfo filePipeLocalInfo = new FilePipeLocalInfo();
                body.setInfo(filePipeLocalInfo);
                reply.setBody(body);
                return Mono.just(reply);

            case FSCC_FILE_PIPE_REMOTE_INFORMATION:
                FilePipeRemoteInfo filePipeRemoteInfo = new FilePipeRemoteInfo();
                body.setInfo(filePipeRemoteInfo);
                reply.setBody(body);
                return Mono.just(reply);
            default:
                break;
        }


        reply.getHeader().setStatus(NTStatus.STATUS_ACCESS_DENIED);
        return Mono.just(reply);
    }

    public static Mono<SMB2Reply> getQuotaInfo(SMB2Reply reply, SMB2Header header, GetInfoCall call) {
        GetInfoReply replyBody = new GetInfoReply();
        SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(header.getCompoundRequest());
        //需要fileInfo的接口，并且fileInfo是空的，返回STATUS_FILE_CLOSED
        if (fileInfo == null && NEED_FID[call.getFileInfoClass() & 0xff] == 1) {
            reply.getHeader().setStatus(NTStatus.STATUS_FILE_CLOSED);
            return Mono.just(reply);
        }
        String dirName = CifsUtils.getParentDirName(fileInfo.obj);
        if (call.getQuotaInfo().returnSingle == 0) {
            if (HAS_SEEK_QUOTA_INFO.contains(fileInfo.obj)) {
                HAS_SEEK_QUOTA_INFO.remove(fileInfo.obj);
                reply.getHeader().status = STATUS_NO_MORE_ENTRIES;
                return Mono.just(reply);
            } else {
                HAS_SEEK_QUOTA_INFO.add(fileInfo.obj);
            }
        }

        switch (call.getFileInfoClass()) {
            case FSCC_FILE_QUOTA_INFORMATION:
                return FsUtils.lookup(fileInfo.bucket, dirName, null, false, false, -1, null)
                        .flatMap(inode -> {
                            if (InodeUtils.isError(inode)) {
                                reply.getHeader().status = NTStatus.STATUS_IO_DEVICE_ERROR;
                                return Mono.just(reply);
                            }
                            return FSQuotaRealService.getFsQuotaConfig(fileInfo.bucket, inode.getNodeId(), FS_DIR_QUOTA, 0)
                                    .onErrorResume(e -> {
                                        reply.getHeader().status = NTStatus.STATUS_IO_DEVICE_ERROR;
                                        if (e instanceof MsException) {
                                            MsException msException = (MsException) e;
                                            if (msException.getErrCode() == QUOTA_INFO_NOT_EXITS) {
                                                reply.getHeader().status = NTStatus.STATUS_NO_SUCH_FILE;
                                            }
                                        }
                                        return Mono.just(new FSQuotaConfig());
                                    })
                                    .flatMap(fsQuotaConfig -> {
                                        if (StringUtils.isBlank(fsQuotaConfig.getBucket())) {
                                            if (reply.getHeader().status == NTStatus.STATUS_NO_SUCH_FILE && inode.getNodeId() != 1) {
                                                reply.getHeader().status = NTStatus.STATUS_SUCCESS;
                                                FileQuotaInfo body = new FileQuotaInfo(0L, -1L, -1L, inode);
                                                replyBody.setInfo(body);
                                                reply.setBody(replyBody);
                                                return Mono.just(reply);
                                            }
                                            return Mono.just(reply);
                                        }
                                        return FSQuotaRealService.getFsQuotaInfo(fileInfo.bucket, inode.getNodeId(), FS_DIR_QUOTA, 0)
                                                .flatMap(dirInfo -> {
                                                    if (DirInfo.isErrorInfo(dirInfo)) {
                                                        reply.getHeader().status = NTStatus.STATUS_NO_QUOTAS_FOR_ACCOUNT;
                                                        return Mono.just(reply);
                                                    }
                                                    long usedCap = Long.parseLong(dirInfo.getUsedCap());
                                                    long quotaThreshold = fsQuotaConfig.getCapacitySoftQuota() != 0 ? fsQuotaConfig.getCapacitySoftQuota() : -1;
                                                    long quotaLimit = fsQuotaConfig.getCapacityHardQuota() != 0 ? fsQuotaConfig.getCapacityHardQuota() : -1;
                                                    FileQuotaInfo body = new FileQuotaInfo(usedCap, quotaThreshold, quotaLimit, inode);
                                                    replyBody.setInfo(body);
                                                    reply.setBody(replyBody);
                                                    return Mono.just(reply);
                                                });
                                    });
                        });
            default:
                break;
        }
        reply.getHeader().setStatus(NTStatus.STATUS_ACCESS_DENIED);
        return Mono.just(reply);
    }

    public static Mono<SMB2Reply> getSecurityInfo(SMB2Reply reply, SMB2Header header, GetInfoCall call) {
        GetInfoReply body = new GetInfoReply();
        SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(header.getCompoundRequest());

        if (fileInfo == null) {
            reply.getHeader().setStatus(NTStatus.STATUS_FILE_CLOSED);
            log.info("get sceInfo: call: {}", call);
            return Mono.just(reply);
        }

        Inode inode = fileInfo.openInode;
        SecurityInfo secInfo = new SecurityInfo();

        if (CIFSACL.cifsACL) {
            log.info("get sceInfo: obj: {}, ino: {}, call: {}", inode.getObjName(), inode.getNodeId(), call);
        }

        int controlFlag = secInfo.getControl() | SELF_RELATIVE | DACL_PROTECTED;
        boolean isAutoInherit = false;

        for (int i = 0; i < SCE_INFO_ARR.length; i++) {
            int infoClass = SCE_INFO_ARR[i];
            boolean isExist = (call.getAdditionalInfo() & infoClass) != 0;
            if (isExist) {
                switch (infoClass) {
                    //获取文件/目录所有者的SID
                    case OWNER_SECURITY_INFORMATION:
                        //将uid转成sid
                        String userSidStr = FSIdentity.getUserSIDByUid(inode.getUid());
                        SID userSID = SID.generateSID(userSidStr);
                        secInfo.setOwnerSID(userSID);
                        if (CIFSACL.cifsACL) {
                            log.info("get secInfo: obj: {}, ino: {}, ownerSID: {}", inode.getObjName(), inode.getNodeId(), userSidStr);
                        }
                        break;
                    //获取文件/目录所有组的SID
                    case GROUP_SECURITY_INFORMATION:
                        //将gid转成sid
                        String groupSidStr = FSIdentity.getGroupSIDByGid(inode.getGid());
                        SID groupSID = SID.generateSID(groupSidStr);
                        secInfo.setGroupSID(groupSID);
                        if (CIFSACL.cifsACL) {
                            log.info("get secInfo: obj: {}, ino: {}, groupSID: {}", inode.getObjName(), inode.getNodeId(), groupSidStr);
                        }
                        break;
                    //获取文件/目录的ACL列表
                    case DACL_SECURITY_INFORMATION:
                        controlFlag |= DACL_PRESENT;
                        SMB2ACL dACL = new SMB2ACL();

                        //todo revision 2是 NT4，4是AD
                        dACL.setAclRevision((byte) 2);

                        //inode的属主与属组信息
                        String inoOwner = FSIdentity.getUserSIDByUid(inode.getUid());
                        String inoGroup = FSIdentity.getGroupSIDByGid(inode.getGid());

                        //必须设置为0
                        dACL.setSbz1((byte) 0);
                        List<Inode.ACE> aceList = inode.getACEs();
                        List<SMB2ACE> dACLList = new LinkedList<>();
                        short totalAceSize = 0;
                        if (null != aceList && !aceList.isEmpty()) {
                            //判断是否存在cifs ace，如果有cifs ace，则读取cifs ace即可，如果没有，则需要将nfs ace转换
                            if (CIFSACL.isExistCifsACE(aceList)) {

                                //0b100 --> user；0b010 --> group；0b001 --> everyone
                                byte existCifsUGO = 0;

                                for (Inode.ACE ace : aceList) {
                                    if (ace.getNType() == 0 && CIFSACL.isCifsACE(ace)) {
                                        if ((ace.getFlag() & CONTAINER_INHERIT_ACE) != 0 || ((ace.getFlag() & OBJECT_INHERIT_ACE) != 0)) {
                                            isAutoInherit = true;
                                        }

                                        //不是仅用于继承的权限
                                        if ((ace.getFlag() & INHERIT_ONLY_ACE) == 0) {
                                            if (inoOwner.equals(ace.getSid())) {
                                                existCifsUGO |= 0b100;
                                            }

                                            if (inoGroup.equals(ace.getSid())) {
                                                existCifsUGO |= 0b010;
                                            }

                                            if (SID.EVERYONE.getDisplayName().equals(ace.getSid())) {
                                                existCifsUGO |= 0b001;
                                            }
                                        }

                                        SMB2ACE smb2ACE = SMB2ACL.mapInodeCIFSACEsToSMB2ACE(ace);
                                        if (CIFSACL.cifsACL) {
                                            log.info("【smb2ACE1】 {}", smb2ACE);
                                        }
                                        dACLList.add(smb2ACE);
                                        totalAceSize += smb2ACE.size();
                                    }
                                }

                                //检查是否有漏判断ugo权限，如果有遗漏则补充判断
                                if (existCifsUGO < 7) {
                                    int mode = inode.getMode();
                                    if ((existCifsUGO & 0b100) == 0) {
                                        int userRight = NFSACL.parseModeToInt(mode, NFSACL_USER_OBJ);
                                        long userAccess = CIFSACL.ugoToCIFSAccess(userRight);
                                        String aceSid = FSIdentity.getUserSIDByUid(inode.getUid());
                                        SMB2ACE smb2ACE = SMB2ACL.mapInodeCIFSACEsToSMB2ACE((byte) 0, (short) 0, aceSid, userAccess);
                                        dACLList.add(smb2ACE);
                                        totalAceSize += smb2ACE.size();
                                    }

                                    if ((existCifsUGO & 0b010) == 0) {
                                        int groupRight = NFSACL.parseModeToInt(mode, NFSACL_GROUP_OBJ);
                                        long groupAccess = CIFSACL.ugoToCIFSAccess(groupRight);
                                        String aceSid = FSIdentity.getGroupSIDByGid(inode.getGid());
                                        SMB2ACE smb2ACE = SMB2ACL.mapInodeCIFSACEsToSMB2ACE((byte) 0, (short) 0, aceSid, groupAccess);
                                        dACLList.add(smb2ACE);
                                        totalAceSize += smb2ACE.size();
                                    }

                                    if ((existCifsUGO & 0b001) == 0) {
                                        int otherRight = NFSACL.parseModeToInt(mode, NFSACL_OTHER);
                                        long domainAccess = CIFSACL.ugoToCIFSAccess(otherRight);
                                        SMB2ACE smb2ACE = SMB2ACL.mapInodeCIFSACEsToSMB2ACE((byte) 0, (short) 0, SID.EVERYONE.getDisplayName(), domainAccess);
                                        dACLList.add(smb2ACE);
                                        totalAceSize += smb2ACE.size();
                                    }
                                }
                            } else {
                                int nfsEffectMask = ACLUtils.getNFSMask(aceList);

                                //只有nfs ace就需要将nfs ace转换为 cifs ace
                                //todo nfs继承下来的文件或目录返回时需增加 INHERITED_ACE 标志位
                                for (Inode.ACE ace : aceList) {
                                    int nType = ace.getNType();
                                    int id = ace.getId();
                                    int right = ace.getRight();

                                    SMB2ACE.ACEHeader aceHeader = new SMB2ACE.ACEHeader();
                                    SMB2ACE.ACCESS_ALLOWED_ACE allowed_ace = new SMB2ACE.ACCESS_ALLOWED_ACE();
                                    allowed_ace.setHeader(aceHeader);

                                    switch (nType) {
                                        case NFSACL_USER:
                                            if (nfsEffectMask > -1) {
                                                right &= nfsEffectMask;
                                            }

                                            allowed_ace.setSid(SID.generateSID(FSIdentity.getUserSIDByUid(id)));
                                            allowed_ace.setMask(CIFSACL.ugoToCIFSAccess(right));
                                            allowed_ace.getHeader().setAceSize(allowed_ace.size());
                                            dACLList.add(allowed_ace);
                                            if (CIFSACL.cifsACL) {
                                                log.info("【smb2ACE2】 {}", allowed_ace);
                                            }
                                            totalAceSize += allowed_ace.size();
                                            break;
                                        case NFSACL_GROUP:
                                            if (nfsEffectMask > -1) {
                                                right &= nfsEffectMask;
                                            }

                                            allowed_ace.setSid(SID.generateSID(FSIdentity.getGroupSIDByGid(id)));
                                            allowed_ace.setMask(CIFSACL.ugoToCIFSAccess(right));
                                            allowed_ace.getHeader().setAceSize(allowed_ace.size());
                                            dACLList.add(allowed_ace);
                                            if (CIFSACL.cifsACL) {
                                                log.info("【smb2ACE2】 {}", allowed_ace);
                                            }
                                            totalAceSize += allowed_ace.size();
                                            break;
                                        case DEFAULT_NFSACL_USER_OBJ:
                                        case DEFAULT_NFSACL_USER:
                                            allowed_ace.getHeader().setAceFlags((short) (CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE | INHERIT_ONLY_ACE));
                                            allowed_ace.setSid(SID.generateSID(FSIdentity.getUserSIDByUid(id)));
                                            allowed_ace.setMask(CIFSACL.ugoToCIFSAccess(right));
                                            allowed_ace.getHeader().setAceSize(allowed_ace.size());
                                            dACLList.add(allowed_ace);
                                            if (CIFSACL.cifsACL) {
                                                log.info("【smb2ACE2】 {}", allowed_ace);
                                            }
                                            totalAceSize += allowed_ace.size();
                                            isAutoInherit = true;
                                            break;
                                        case DEFAULT_NFSACL_GROUP_OBJ:
                                        case DEFAULT_NFSACL_GROUP:
                                            allowed_ace.getHeader().setAceFlags((short) (CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE | INHERIT_ONLY_ACE));
                                            allowed_ace.setSid(SID.generateSID(FSIdentity.getGroupSIDByGid(id)));
                                            allowed_ace.setMask(CIFSACL.ugoToCIFSAccess(right));
                                            allowed_ace.getHeader().setAceSize(allowed_ace.size());
                                            dACLList.add(allowed_ace);
                                            if (CIFSACL.cifsACL) {
                                                log.info("【smb2ACE2】 {}", allowed_ace);
                                            }
                                            totalAceSize += allowed_ace.size();
                                            isAutoInherit = true;
                                            break;
                                        case DEFAULT_NFSACL_CLASS:
                                            //todo 暂时没有发现需要进行转换
                                            isAutoInherit = true;
                                            break;
                                        case DEFAULT_NFSACL_OTHER:
                                            allowed_ace.getHeader().setAceFlags((short) (CONTAINER_INHERIT_ACE | OBJECT_INHERIT_ACE | INHERIT_ONLY_ACE));
                                            allowed_ace.setSid(SID.EVERYONE.clone());
                                            allowed_ace.setMask(CIFSACL.ugoToCIFSAccess(right));
                                            allowed_ace.getHeader().setAceSize(allowed_ace.size());
                                            if (CIFSACL.cifsACL) {
                                                log.info("【smb2ACE2】 {}", allowed_ace);
                                            }
                                            dACLList.add(allowed_ace);
                                            totalAceSize += allowed_ace.size();
                                            isAutoInherit = true;
                                            break;
                                    }
                                }

                                //将mode权限转化为cifs acl返回
                                List<Inode.ACE> transferList = CIFSACL.parseModeToCIFSACL(inode, nfsEffectMask);
                                for (Inode.ACE ace : transferList) {
                                    SMB2ACE smb2ACE = SMB2ACL.mapInodeCIFSACEsToSMB2ACE(ace);
                                    dACLList.add(smb2ACE);
                                    if (CIFSACL.cifsACL) {
                                        log.info("【smb2ACE2】 {}", smb2ACE);
                                    }
                                    totalAceSize += smb2ACE.size();
                                }
                            }
                        } else {
                            //如果当前ace列表为空，则以mode权限翻译为cifs ace返回
                            List<Inode.ACE> transferList = CIFSACL.parseModeToCIFSACL(inode, -1);
                            for (Inode.ACE ace : transferList) {
                                SMB2ACE smb2ACE = SMB2ACL.mapInodeCIFSACEsToSMB2ACE(ace);
                                if (CIFSACL.cifsACL) {
                                    log.info("【smb2ACE3】 {}", smb2ACE);
                                }
                                dACLList.add(smb2ACE);
                                totalAceSize += smb2ACE.size();
                            }
                        }

                        dACL.setAclSize((short) (8 + totalAceSize));
                        dACL.setAceCount((short) dACLList.size());
                        dACL.setAclList(dACLList);
                        secInfo.setDACL(dACL);
                        if (CIFSACL.cifsACL) {
                            log.info("get secInfo: obj: {}, ino: {}, dACL: {}", inode.getObjName(), inode.getNodeId(), dACL);
                        }
                        break;
                    //获取文件/目录的系统ACL列表
                    case SACL_SECURITY_INFORMATION:
                    case LABEL_SECURITY_INFORMATION:
                    case ATTRIBUTE_SECURITY_INFORMATION:
                    case SCOPE_SECURITY_INFORMATION:
                    case BACKUP_SECURITY_INFORMATION:
                    default:
                        break;
                }
            }
        }

        if (isAutoInherit) {
            controlFlag |= DACL_AUTO_INHERITED;
        }
        secInfo.setControl((short) controlFlag);
        if (CIFSACL.cifsACL) {
            log.info("【secReply】 obj: {}, nodeId: {}, secInfo: {}", inode.getObjName(), inode.getNodeId(), secInfo);
        }

        int offset = 20;
        //设置secInfo中ownerSID、groupSID、sACL、dACL的数值
        if (null != secInfo.getOwnerSID()) {
            secInfo.setOffsetOwner(offset);
            offset += secInfo.getOwnerSID().size();
        }

        if (null != secInfo.getGroupSID()) {
            secInfo.setOffsetGroup(offset);
            offset += secInfo.getGroupSID().size();
        }

        if (null != secInfo.getSACL()) {
            secInfo.setOffsetSacl(offset);
            offset += secInfo.getSACL().size();
        }

        if (null != secInfo.getDACL()) {
            secInfo.setOffsetDacl(offset);
        }

        if (CIFSACL.cifsACL) {
            log.info("【get secInfo】 secInfo: {}, obj: {}", secInfo, inode.getObjName());
        }

        //win7、win8需要返回 STATUS_BUFFER_TOO_SMALL
        if (call.getResponseLen() == 0) {
            ErrorReply errorReply = new ErrorReply();
            int requiredBufSize = secInfo.size();
            if (requiredBufSize > 0) {
                errorReply.setByteCount(4);
                errorReply.setRequiredBufSize(requiredBufSize);
                reply.setBody(errorReply);
                reply.getHeader().setStatus(NTStatus.STATUS_BUFFER_TOO_SMALL);
                return Mono.just(reply);
            }
        }

        body.setInfo(secInfo);
        reply.setBody(body);
        reply.getHeader().setStatus(NTStatus.STATUS_SUCCESS);
        return Mono.just(reply);
    }

    // 计算有配额的情况
    private static FsFullSizeInfo buildFullSizeInfo(FSQuotaConfig config, DirInfo dirInfo) {
        long usedCapacity = Long.parseLong(dirInfo.getUsedCap());
        long hard = config.getCapacityHardQuota() != 0 ? config.getCapacityHardQuota() : -1;
        long totalSize = (hard > 0) ? Math.max(hard, usedCapacity) : 0;
        long availableSize = Math.max(0, totalSize - usedCapacity);

        FsFullSizeInfo info = new FsFullSizeInfo();
        info.setUnits(totalSize / info.getBytesPerSector());
        info.setAvailableUnits(availableSize / info.getBytesPerSector());
        info.setActualAvailableUnits(availableSize / info.getBytesPerSector());
        return info;
    }

    // 计算无配额的情况
    private static FsFullSizeInfo buildDefaultFullSizeInfo(String bucketName) {
        long totalSize = 0;
        long availableSize = 0;
        List<StoragePool> storagePoolList = StoragePoolFactory.getAvailableStoragesWithCachePool(bucketName);
        for (StoragePool pool : storagePoolList) {
            int km = pool.getM() + pool.getK();
            int k = pool.getK();
            totalSize += (pool.getCache().totalSize + pool.getCache().firstPartCapacity) / km * k;
            availableSize += (pool.getCache().totalSize + pool.getCache().firstPartCapacity - pool.getCache().size - pool.getCache().firstPartUsedSize) / km * k;
        }
        FsFullSizeInfo fsFullSizeInfo = new FsFullSizeInfo();
        fsFullSizeInfo.setUnits(totalSize / fsFullSizeInfo.getBytesPerSector());
        fsFullSizeInfo.setAvailableUnits(availableSize / fsFullSizeInfo.getBytesPerSector());
        fsFullSizeInfo.setActualAvailableUnits(availableSize / fsFullSizeInfo.getBytesPerSector());
        return fsFullSizeInfo;
    }

    private static FsSizeInfo buildFsSizeInfo(FSQuotaConfig config, DirInfo dirInfo) {
        long usedCapacity = Long.parseLong(dirInfo.getUsedCap());
        long hard = config.getCapacityHardQuota() != 0 ? config.getCapacityHardQuota() : -1;
        long totalSize = (hard > 0) ? Math.max(hard, usedCapacity) : 0;
        long availableSize = Math.max(0, totalSize - usedCapacity);

        FsSizeInfo info = new FsSizeInfo();
        info.totalAllocate = totalSize / info.bytesPerSector;
        info.actualAllocate = availableSize / info.bytesPerSector;
        return info;
    }

    private static FsSizeInfo buildDefaultFsSizeInfo(String bucketName) {
        FsSizeInfo fsSizeInfo = new FsSizeInfo();
        fsSizeInfo.totalAllocate = 0;
        fsSizeInfo.actualAllocate = 0;
        List<StoragePool> storagePoolList_full = StoragePoolFactory.getAvailableStoragesWithCachePool(bucketName);
        for (StoragePool pool : storagePoolList_full) {
            int km = pool.getM() + pool.getK();
            int k = pool.getK();

            fsSizeInfo.totalAllocate += (pool.getCache().totalSize + pool.getCache().firstPartCapacity) / km * k;
            fsSizeInfo.actualAllocate += (pool.getCache().totalSize + pool.getCache().firstPartCapacity - pool.getCache().size - pool.getCache().firstPartUsedSize) / km * k;
        }
        fsSizeInfo.totalAllocate /= fsSizeInfo.bytesPerSector;
        fsSizeInfo.actualAllocate /= fsSizeInfo.bytesPerSector;
        return fsSizeInfo;
    }
}


