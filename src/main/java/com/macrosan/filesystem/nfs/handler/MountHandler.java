package com.macrosan.filesystem.nfs.handler;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.reply.ListExportReply;
import com.macrosan.filesystem.nfs.reply.ListMountRecordReply;
import com.macrosan.filesystem.nfs.reply.MountReply;
import com.macrosan.filesystem.nfs.reply.NullReply;
import com.macrosan.filesystem.nfs.types.FH2;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.FSIPACLUtils;
import com.macrosan.filesystem.utils.IpWhitelistUtils;
import io.netty.buffer.ByteBuf;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.datagram.DatagramSocket;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;

import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;

@Log4j2
public class MountHandler extends RpcHandler {
    NetSocket socket;

    public DatagramSocket udpSocket;
    public SocketAddress address;

    public MountHandler(NetSocket socket) {
        this.socket = socket;
    }

    public MountHandler(DatagramSocket udpSocket, SocketAddress address) {
        this.udpSocket = udpSocket;
        this.address = address;
    }

    @Override
    protected void handleMsg(int offset, RpcCallHeader callHeader, ByteBuf msg) {
        if (NFS.nfsDebug) {
            log.info("Mount call {}", callHeader);
        }

        if (callHeader.rpcVersion == 2 && callHeader.program == 100005 && callHeader.programVersion == 3) {
            SunRpcHeader header = SunRpcHeader.newReplyHeader(callHeader.getHeader().id);
            Mono<RpcReply> reply = Mono.empty();
            int[] bufSize = new int[1];
            bufSize[0] = 4096;
            String[] mountPoint = new String[1];
            switch (callHeader.opt) {
                //NULL
                case 0:
                    reply = Mono.just(new NullReply(header));
                    break;
                case 1:
                    //mount
                    int len = msg.getInt(offset);
                    byte[] bytes = new byte[len];
                    msg.getBytes(offset + 4, bytes);
                    mountPoint[0] = new String(bytes);
                    //TODO check mountpoint and get bucket fsid
                    if (mountPoint[0].lastIndexOf("/") == mountPoint[0].length() - 1) {
                        mountPoint[0] = mountPoint[0].substring(0, mountPoint[0].length() - 1);
                    }
                    //mountPoint=/nfs/bucketName
                    if (mountPoint[0].endsWith("/")) {
                        mountPoint[0] = mountPoint[0].substring(0, mountPoint[0].lastIndexOf("/"));
                    }
                    String[] split = mountPoint[0].split("/");
                    MountReply reply0 = new MountReply(header);
                    if (split.length < 3) {
                        reply0.status = FsConstants.NfsErrorNo.NFS3ERR_NOENT;
                        reply0.rootFH = new FH2();
                        reply0.verf = 0L;
                        reply = Mono.just(reply0);
                        break;
                    }
                    String bucket = split[2];
                    Map<String, String> bucketInfo = RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucket);
                    //bucket fsid exits
                    if (bucketInfo != null && !bucketInfo.isEmpty() && "1".equals(bucketInfo.get("nfs"))) {
                        if (!IpWhitelistUtils.checkNFSWhitelist(bucket, getClientIp())) {
                            reply0.status = FsConstants.NfsErrorNo.NFS3ERR_ACCES;
                            reply0.rootFH = new FH2();
                            reply = Mono.just(reply0);
                            break;
                        }
                        int fsid = Integer.parseInt(bucketInfo.get("fsid"));
                        if (!FSIPACLUtils.hasMountAccess(bucketInfo, getClientIp())
                                ||!CheckUtils.siteCanAccess(bucketInfo)) {
                            reply0.status = FsConstants.NfsErrorNo.NFS3ERR_ACCES;
                            reply0.rootFH = new FH2();
                            reply = Mono.just(reply0);
                            break;
                        }
                        //root inode
                        String verf = bucketInfo.get("verf");
                        if (StringUtils.isBlank(verf)) {
                            //auth type 设置为 AUTH_NULL
                            reply0.verf = 0L;
                        } else {
                            //390003 krb5
                            //390004 krb5i
                            //390005 krb5p
                            reply0.verf = ((1L << 32) | Integer.parseInt(verf));
                        }
                        NFSBucketInfo.FsInfo fsInfo = new NFSBucketInfo.FsInfo();
                        fsInfo.setBucket(bucket);
                        NFSBucketInfo.bucketInfo.put(bucket, bucketInfo);
                        NFSBucketInfo.fsToBucket.put(fsid, fsInfo);
                        if (split.length <= 3) {
                            reply0.rootFH = FH2.mapToFH2(InodeUtils.getAndPutRootInode(bucket), fsid);
                            reply = Mono.just(reply0);
                        } else {
                            String objName = mountPoint[0].substring(mountPoint[0].indexOf(bucket) + bucket.length() + 1);
                            ReqInfo reqHeader = new ReqInfo();
                            reqHeader.bucket = bucket;
                            reply = FsUtils.lookup(bucket, objName, reqHeader, false, 1, null)
                                    .flatMap(inode -> {
                                        if (InodeUtils.isError(inode)) {
                                            reply0.status = FsConstants.NfsErrorNo.NFS3ERR_NOENT;
                                            reply0.rootFH = new FH2();
                                        } else {
                                            reply0.rootFH = FH2.mapToFH2(inode, fsid);
                                        }
                                        return Mono.just(reply0);
                                    });
                        }
                        log.info("mount {},client ip:{}", mountPoint[0],getClientIp());
                    } else {
                        reply0.status = FsConstants.NfsErrorNo.NFS3ERR_NOENT;
                        reply0.rootFH = new FH2();
                        reply0.verf = 0L;
                        reply = Mono.just(reply0);
                    }
                    break;
                case 2:
                    //showmount -d -a
                    Set<String> records = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).smembers("mount_record");
                    ListMountRecordReply listMountRecordReply = new ListMountRecordReply(header);
                    if (records != null && !records.isEmpty()) {
                        String[] recordArr = records.toArray(new String[0]);
                        listMountRecordReply.valueFollow = 1;
                        for (int i = 0; i < records.size(); i++) {
                            String record = recordArr[i];
                            int idx = record.indexOf(":");
                            String hostName = record.substring(0, idx);
                            String dirName = record.substring(idx + 1);
                            int valueFollow = i == records.size() - 1 ? 0 : 1;
                            ListMountRecordReply.MountRecord mountRecord = new ListMountRecordReply.MountRecord(hostName, dirName, valueFollow);
                            listMountRecordReply.mountRecords.add(mountRecord);
                        }
                    } else {
                        listMountRecordReply.valueFollow = 0;
                    }
                    bufSize[0] = listMountRecordReply.getSize() > bufSize[0] ? (listMountRecordReply.getSize() + 512) : bufSize[0];
                    reply = Mono.just(listMountRecordReply);
                    break;
                case 3:
                    //umount
                    int len1 = msg.getInt(offset);
                    byte[] bytes1 = new byte[len1];
                    msg.getBytes(offset + 4, bytes1);
                    mountPoint[0] = new String(bytes1);
                    String mountRecord = getMountRecord(mountPoint[0]);
                    RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).srem("mount_record", mountRecord);
                    log.info("unmount {},client ip:{}", mountPoint[0],getClientIp());
                    reply = Mono.just(new NullReply(header));
                    break;
                case 5:
                    //list export
                    //String exportStr = SshClientUtils.exec("cat /etc/exports|awk '{print $1}'", false, false).var1;
                    //log.info("export info {}", exportStr);
                    ListExportReply listExportReply = new ListExportReply(header);
                    Map<String, String> fsidToBucket = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hgetall("fsid_bucket");
                    if (fsidToBucket != null && fsidToBucket.size() > 0) {
                        int bucketNum = fsidToBucket.size();
                        int i = 0;
                        for (Map.Entry<String, String> entry : fsidToBucket.entrySet()) {
                            int entryFollow = i == bucketNum - 1 ? 0 : 1;
                            Map<String, String> bucketInfo0 = RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hgetall(entry.getValue());
                            String startNfs = bucketInfo0.get("nfs");
                            if ("1".equals(startNfs) && IpWhitelistUtils.checkNFSWhitelist(entry.getValue(), getClientIp())) {
                                String dirName = bucketInfo0.get( "mountPoint");
                                Set<String> ipInfoSet = FSIPACLUtils.getIpInfoSet(bucketInfo0);
                                ListExportReply.ExportEntry entry0 = new ListExportReply.ExportEntry(dirName.length(), dirName.getBytes(), entryFollow, ipInfoSet);
                                listExportReply.exportEntries.add(entry0);
                            }
                            i++;
                        }
                        if (!listExportReply.exportEntries.isEmpty()) {
                            listExportReply.exportEntries.get(listExportReply.exportEntries.size() - 1).entryFollow = 0;
                        } else {
                            listExportReply.valueFollow = 0;
                        }
                    } else {
                        listExportReply.valueFollow = 0;
                    }
                    bufSize[0] = listExportReply.getSize() > bufSize[0] ? (listExportReply.getSize() + 512) : bufSize[0];
                    reply = Mono.just(listExportReply);
                    break;

                default:
            }
            reply.flatMap(r -> {
                if (callHeader.opt == 1 && ((MountReply) r).status == 0) {
                    String val = getMountRecord(mountPoint[0]);
                    RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd("mount_record", val);
                }
                return Mono.just(r);
            }).subscribe(rpcReply -> {
                if (rpcReply != null) {
                    if (NFS.nfsDebug) {
                        log.info("Mount reply {}", rpcReply);
                    }
                    if (socket != null) {
                        socket.write(Buffer.buffer(replyToBuf(rpcReply, bufSize[0])));
                    } else {
                        sendUdpMsg(rpcReply, udpSocket, address, bufSize[0]);
                    }
                }
            });

        }

        //TODO return error
    }

    @Override
    protected void handleRes(int offset, RpcReplyHeader replyHeader, ByteBuf msg) {

    }

    private String getMountRecord(String mountPoint) {
        return getClientIp() + ":" + mountPoint;
    }

    private String getClientIp() {
        if (isUdp) {
            return address.host();
        } else {
            return socket.remoteAddress().host();
        }
    }

    private int getClientPort() {
        if (isUdp) {
            return address.port() ;
        } else {
            return socket.remoteAddress().port() ;
        }
    }
}
