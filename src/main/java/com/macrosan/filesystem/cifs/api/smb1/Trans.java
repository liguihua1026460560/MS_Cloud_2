package com.macrosan.filesystem.cifs.api.smb1;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cifs.SMB1;
import com.macrosan.filesystem.cifs.SMB1.SMB1Reply;
import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.cifs.SMB1Header;
import com.macrosan.filesystem.cifs.call.smb1.Trans2Call;
import com.macrosan.filesystem.cifs.reply.smb1.*;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.reply.ReadDirPlusReply;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.ReadDirCache;
import com.macrosan.message.jsonmsg.Inode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.filesystem.FsConstants.MAX_CIFS_LOOKUP_BUFFER_SIZE;
import static com.macrosan.filesystem.FsConstants.NTStatus;
import static com.macrosan.filesystem.FsConstants.NTStatus.*;
import static com.macrosan.filesystem.FsConstants.SMBSetInfoLevel.*;
import static com.macrosan.filesystem.cifs.SMB1.SMB1_OPCODE.SMB_TRANS2;
import static com.macrosan.filesystem.cifs.call.smb1.Trans2Call.*;
import static com.macrosan.filesystem.cifs.call.smb1.Trans2Call.FindFirstCall.SMB_FIND_FILE_BOTH_DIRECTORY_INFO;
import static com.macrosan.filesystem.cifs.call.smb1.Trans2Call.FindFirstCall.SMB_FIND_FILE_UNIX;
import static com.macrosan.filesystem.cifs.call.smb1.Trans2Call.QueryFSInfoCall.*;
import static com.macrosan.filesystem.cifs.call.smb1.Trans2Call.QueryPathInfoCall.*;
import static com.macrosan.filesystem.cifs.reply.smb1.QueryAttrReply.*;
import static com.macrosan.filesystem.cifs.reply.smb1.QueryCifsUnixReply.*;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;

@Slf4j
public class Trans {
    private static final Trans instance = new Trans();

    private Trans() {
    }

    public static Trans getInstance() {
        return instance;
    }

    private static final Node nodeInstance = Node.getInstance();

    @SMB1.Smb1Opt(value = SMB_TRANS2, buf = MAX_CIFS_LOOKUP_BUFFER_SIZE)
    public Mono<SMB1Reply> transaction2(SMB1Header header, Session session, Trans2Call call) {
        SMB1Reply reply = new SMB1Reply(header);
        SMB1Header replyHeader = reply.getHeader();

        if (call.subCall instanceof QueryFSInfoCall) {
            QueryFSInfoCall queryFSInfoCall = (QueryFSInfoCall) call.subCall;
            SMB1Body body = queryFsInfo(queryFSInfoCall, session, replyHeader);
            if (body != null) {
                reply.setBody(body);
                replyHeader.status = 0;
                return Mono.just(reply);
            }
        } else if (call.subCall instanceof SetFsInfoCall) {

        } else if (call.subCall instanceof QueryPathInfoCall) {
            QueryPathInfoCall queryPathInfoCall = (QueryPathInfoCall) call.subCall;
            return queryPathInfo(queryPathInfoCall, session, replyHeader)
                    .flatMap(res -> {
                        reply.setBody(res);
                        return Mono.just(reply);
                    });
        } else if (call.subCall instanceof FindFirstCall) {
            FindFirstCall findFirstCall = (FindFirstCall) call.subCall;
            return findFirst(findFirstCall, session, replyHeader);
        } else if (call.subCall instanceof SetPathInfoCall) {
            SetPathInfoCall setPathInfoCall = (SetPathInfoCall) call.subCall;
            return setPathInfo(setPathInfoCall, session, replyHeader)
                    .flatMap(b -> {
                        reply.setBody(b);
                        return Mono.just(reply);
                    });
        } else if (call.subCall instanceof Trans2Call.FindNextCall) {
            FindNextCall findNextCall = (Trans2Call.FindNextCall) call.subCall;
            return findNext(findNextCall, session, replyHeader);
        } else if (call.subCall instanceof Trans2Call.SetFileInfoCall) {
            Trans2Call.SetFileInfoCall setFileInfoCall = (Trans2Call.SetFileInfoCall) call.subCall;
            return setFileInfo(setFileInfoCall, session, replyHeader)
                    .flatMap(r -> {
                        return Mono.just(reply);
                    });
        }
        replyHeader.status = NTStatus.STATUS_NOT_IMPLEMENTED;
        return Mono.just(reply);
    }

    private Mono<SMB1Body> queryPathInfo(QueryPathInfoCall queryPathInfoCall, Session session0, SMB1Header replyHeader) {
        String bucket = NFSBucketInfo.getBucketName(replyHeader.getTid());

        switch (queryPathInfoCall.infoLevel) {
            case SMB_QUERY_FILE_ALL_INFO:
                QueryFileAllInfoReply queryFileAllInfoReply = new QueryFileAllInfoReply();
                queryFileAllInfoReply.mode = FsConstants.FILE_ATTRIBUTE_DIRECTORY;

                return Mono.just(queryFileAllInfoReply);
            case QUERY_FILE_UNIX_BASIC:
                String queryFileName = new String(queryPathInfoCall.fileName);
                if (StringUtils.isNotBlank(queryFileName)) {
                    String objName = new String(queryPathInfoCall.fileName).substring(1);
                    return FsUtils.lookup(bucket, objName, null, false, -1, null)
                            .map(inode -> {
                                replyHeader.status = STATUS_SUCCESS;
                                if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                                    replyHeader.status = STATUS_OBJECT_NAME_NOT_FOUND;
                                }
                                if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                    log.info("get inode fail objName:{},bucket:{}", objName, bucket);
                                    replyHeader.status = STATUS_DATA_ERROR;
                                }
                                return QueryFileUnixReply.mapToQueryFileUnixReply(inode);
                            });

                } else {
                    return nodeInstance.getInode(bucket, 1)
                            .map(rootInode -> {
                                if (InodeUtils.isError(rootInode)) {
                                    throw new NFSException(FsConstants.NfsErrorNo.NFS3ERR_I0, "get inode fail,bucket:" + bucket);
                                }
                                replyHeader.status = STATUS_SUCCESS;
                                return QueryFileUnixReply.mapToQueryFileUnixReply(rootInode.setNodeId(replyHeader.tid));
                            });
                }
            case SMB_QFILEINFO_BASIC_INFORMATION:
                QueryFileBasicInfoReply queryFileBasicInfoReply = new QueryFileBasicInfoReply();
                queryFileBasicInfoReply.mode = FsConstants.FILE_ATTRIBUTE_DIRECTORY;
                return Mono.just(queryFileBasicInfoReply);
            case SMB_QFILEINFO_STANDARD_INFORMATION:
                QueryFileStandInfoReply queryFileStandInfoReply = new QueryFileStandInfoReply();
                queryFileStandInfoReply.nlinks = 1;
                queryFileStandInfoReply.directory = 1;
                return Mono.just(queryFileStandInfoReply);
            default:
                break;
        }

        return null;
    }

    private SMB1Body queryFsInfo(QueryFSInfoCall queryFSInfoCall, Session session, SMB1Header replyHeader) {
        String bucket = NFSBucketInfo.getBucketName(replyHeader.getTid());
        Map<String, String> bucketInfo = NFSBucketInfo.getBucketInfo(bucket);

        switch (queryFSInfoCall.infoLevel) {
            case SMB_QUERY_CIFS_UNIX_INFO:
                QueryCifsUnixReply cifsUnixReply = new QueryCifsUnixReply();
                cifsUnixReply.capabilities = CIFS_UNIX_POSIX_PATHNAMES_CAP |
                        CIFS_UNIX_LARGE_READ_CAP |
                        CIFS_UNIX_LARGE_WRITE_CAP;
                return cifsUnixReply;
            case SMB_QUERY_FS_DEVICE_INFO:
                return new QueryFsDeviceReply();
            case SMB_QUERY_FS_ATTRIBUTE_INFO:
                QueryAttrReply attrReply = new QueryAttrReply();
                attrReply.flags = FILE_CASE_PRESERVED_NAMES | FILE_CASE_SENSITIVE_SEARCH |
                        FILE_SUPPORTS_OBJECT_IDS | FILE_UNICODE_ON_DISK | FILE_SUPPORTS_SPARSE_FILES;
                return attrReply;
            case SMB_INFO_ALLOCATION:
                QueryAllocationReply reply = new QueryAllocationReply();
                reply.fsid = replyHeader.tid;
                return reply;
            case SMB_INFO_VOLUME:
                QueryVolInfoReply queryVolInfoReply = new QueryVolInfoReply();
                queryVolInfoReply.VolumeLabel = bucket.toCharArray();
                return queryVolInfoReply;
            case SMB_QUERY_FS_VOLUME_INFO:
                QueryFsVolInfoReply queryFsVolInfoReply = new QueryFsVolInfoReply();
                queryFsVolInfoReply.VolumeLabel = bucket.toCharArray();
                return queryFsVolInfoReply;
            case SMB_QUERY_FS_SIZE_INFO:
                return new QueryFsSizeInfoReply();
            case SMB_QUERY_POSIX_FS_INFO:
                QueryPOSIXFsInfoReply queryPOSIXFsInfoReply = new QueryPOSIXFsInfoReply();
                queryPOSIXFsInfoReply.fsid = replyHeader.tid;
                return queryPOSIXFsInfoReply;
            case SMB_QUERY_POSIX_WHOAMI:
            case SMB_MAC_QUERY_FS_INFO:
            default:
                break;
        }

        return null;
    }

    private Mono<SMB1Reply> findFirst(FindFirstCall findFirstCall, Session session, SMB1Header replyHeader) {
        SMB1Reply reply = new SMB1Reply(replyHeader);
        SMB1Header header = reply.getHeader();
        FindFirstReply body = new FindFirstReply();
        String dirName = new String(findFirstCall.fileName).substring(1).replace("*", "");
        return readdir(findFirstCall.infoLevel, body, session, header, findFirstCall.searchCount, dirName, reply, true);
    }

    public Mono<SMB1Body> setPathInfo(SetPathInfoCall call, Session session0, SMB1Header replyHeader) {
        String bucket = NFSBucketInfo.getBucketName(replyHeader.getTid());

        SetPathInfoReply reply = new SetPathInfoReply();
        switch (call.infoLevel) {
            case SMB_POSIX_OPEN:
                break;
            case SMB_POSIX_UNLINK:
                break;
            case SMB_SET_FILE_UNIX_HLINK:
                break;
            case SMB_SET_FILE_UNIX_LINK:
                break;
            case SMB_SET_FILE_BASIC_INFO:
                break;
            case SMB_SET_FILE_BASIC_INFO2:
                break;
            case SMB_SET_FILE_UNIX_BASIC:
                ObjAttr attr = new ObjAttr();
                UnixBasicInfo unixBasicInfo = (UnixBasicInfo) call.info;
                unixBasicInfo.setObjAttr(attr);
                return FsUtils.lookup(bucket, new String(call.fileName).substring(1), null, false, -1, null)
                        .flatMap(inode -> {
                            if (InodeUtils.isError(inode)) {
                                replyHeader.status = STATUS_DATA_ERROR;
                                return Mono.just(reply);
                            }
                            return nodeInstance.setAttr(inode.getNodeId(), bucket, attr, null)
                                    .flatMap(res -> {
                                        if (InodeUtils.isError(res)) {
                                            replyHeader.status = STATUS_DATA_ERROR;
                                        }
                                        return Mono.just(reply);
                                    });
                        });
            case SMB_SET_FILE_EA:
                break;
            case SMB_SET_POSIX_ACL:
                break;
            case SMB_SET_FILE_END_OF_FILE_INFO2:
                break;
            case SMB_SET_FILE_END_OF_FILE_INFO:
                break;
        }
        return Mono.just(reply);
    }

    public Mono<SMB1Reply> findNext(FindNextCall call, Session session, SMB1Header replyHeader) {
        SMB1Reply reply = new SMB1Reply(replyHeader);
        SMB1Header header = reply.getHeader();
        FindFirstReply body = new FindFirstReply();
        body.isFindNext = true;
        String lastFileName = new String(call.fileName);
        return readdir(call.infoLevel, body, session, header, call.searchCount, lastFileName, reply, false);
    }

    public Mono<SMB1Reply> readdir(int infoLevel, FindFirstReply body, Session session, SMB1Header header, int searchCount, String dirName, SMB1Reply reply, boolean isFindFirst) {
        String bucket = NFSBucketInfo.getBucketName(header.getTid());

        switch (infoLevel) {
            case SMB_FIND_FILE_BOTH_DIRECTORY_INFO:
                break;
            case SMB_FIND_FILE_UNIX:
                // body.searchId = -1; //end
//                body.searchCount = findFirstCall.searchCount;
                //body.endOfSearch = 1;//true
                //body.lastNameOffset = 0;
                body.dataList = new LinkedList<>(); //empty
                AtomicInteger dirBytesLength = new AtomicInteger(0);
                AtomicInteger dirLength = new AtomicInteger(0);
                int maxSize = Math.min(searchCount * FindFirstCall.getInfoSize(infoLevel) + SMB1Header.SIZE, MAX_CIFS_LOOKUP_BUFFER_SIZE);
                ReadDirPlusReply readDirPlusReply = new ReadDirPlusReply(SunRpcHeader.newReplyHeader(header.mid));
                return Mono.just(dirName)
                        .flatMap(dirName0 -> {
                            if (isFindFirst) {
                                if (StringUtils.isBlank(dirName0) || !dirName0.contains("/")) {
                                    return ReadDirCache.listAndCache(bucket, dirName0, 0, maxSize, session.nfsHandler, 1L, readDirPlusReply, null);
                                } else {
                                    String realDirName = dirName0.substring(0, dirName0.lastIndexOf('/'));
                                    dirBytesLength.set(realDirName.getBytes(StandardCharsets.UTF_8).length);
                                    dirLength.set(realDirName.length());
                                    return FsUtils.lookup(bucket, realDirName, null, false, -1, null)
                                            .flatMap(dirInode -> {
                                                if (InodeUtils.isError(dirInode)) {
                                                    header.status = STATUS_DATA_ERROR;
                                                    return Mono.just(new LinkedList<Inode>());
                                                }
                                                return ReadDirCache.listAndCache(bucket, dirInode.getObjName(), 0, maxSize, session.nfsHandler, dirInode.getNodeId(), readDirPlusReply, dirInode.getACEs());
                                            });
                                }
                            } else {
                                return FsUtils.lookup(bucket, dirName0, null, false, -1, null)
                                        .flatMap(lastInode -> {
                                            if (InodeUtils.isError(lastInode)) {
                                                header.status = STATUS_DATA_ERROR;
                                                return Mono.just(new LinkedList<Inode>());
                                            }
                                            String tmpObjName = "/" + lastInode.getObjName();
                                            //父目录为根目录
                                            if (tmpObjName.endsWith("/")) {
                                                tmpObjName = tmpObjName.substring(0, tmpObjName.length() - 1);
                                            }
                                            String realDirName = lastInode.getObjName().substring(0, tmpObjName.lastIndexOf('/'));
                                            if (StringUtils.isBlank(realDirName)) {
                                                return ReadDirCache.listAndCache(bucket, "", lastInode.getCookie(), maxSize, session.nfsHandler, 1L, readDirPlusReply, lastInode.getACEs());
                                            } else {
                                                realDirName = lastInode.getObjName().substring(0, lastInode.getObjName().lastIndexOf('/'));
                                            }
                                            return FsUtils.lookup(bucket, realDirName, null, false, -1, null)
                                                    .flatMap(dirInode0 -> {
                                                        if (InodeUtils.isError(dirInode0)) {
                                                            header.status = STATUS_DATA_ERROR;
                                                            return Mono.just(new LinkedList<Inode>());
                                                        }
                                                        return ReadDirCache.listAndCache(bucket, dirInode0.getObjName(), lastInode.getCookie(), maxSize, session.nfsHandler, dirInode0.getNodeId(), readDirPlusReply, dirInode0.getACEs());
                                                    });
                                        });
                            }
                        })
                        .flatMap(inodeList -> {
                            if (header.status == STATUS_DATA_ERROR) {
                                reply.setBody(body);
                                return Mono.just(reply);
                            } else {
                                int replySize = body.getSize();
                                int lastSize = 108;//first file name offset
                                for (Inode inode : inodeList) {
                                    int limitFileNameLength = 255;
                                    if (inode.getObjName().endsWith("/")) {
                                        limitFileNameLength = 256;
                                    }

                                    if (inode.getObjName().getBytes(StandardCharsets.UTF_8).length - dirBytesLength.get() <= limitFileNameLength) {
                                        FindFirstReply.FILE_UNIX_DIR_INFO entry = FindFirstReply.FILE_UNIX_DIR_INFO.mapToInfo(inode, dirLength.get());
                                        if (replySize + entry.getNextEntryOffset() >= maxSize - body.getSize()) {
                                            break;
                                        }
                                        body.dataList.add(entry);
                                        replySize += entry.getNextEntryOffset();
                                        body.lastNameOffset += lastSize;
                                        lastSize = entry.getNextEntryOffset();
                                    }
                                }
                                body.searchId = session.uid;
                            }
                            body.searchCount = (short) body.dataList.size();
                            if (body.searchCount == 0) {
                                body.endOfSearch = 1;
                            }
                            reply.setBody(body);

                            return Mono.just(reply);
                        });
            default:
                break;
        }
        header.status = STATUS_NOT_IMPLEMENTED;
        return Mono.just(reply);
    }

    public Mono<SMB1Body> setFileInfo(SetFileInfoCall call, Session session, SMB1Header header) {
        SMB1Body body = new SMB1Body();
        return Mono.just(NT.openedFiles.get(call.fsid))
                .flatMap(inode -> {
                    ObjAttr attr = new ObjAttr();
                    UnixBasicInfo unixBasicInfo = (UnixBasicInfo) call.info;
                    unixBasicInfo.setObjAttr(attr);
                    return Node.getInstance().setAttr(inode.getNodeId(), inode.getBucket(), attr, null)
                            .flatMap(res -> {
                                if (InodeUtils.isError(res)) {
                                    header.status = STATUS_DATA_ERROR;
                                }
                                return Mono.just(body);
                            });
                });
    }
}
