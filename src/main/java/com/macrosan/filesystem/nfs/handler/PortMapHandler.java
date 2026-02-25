package com.macrosan.filesystem.nfs.handler;

import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.reply.NullReply;
import com.macrosan.filesystem.nfs.reply.PortMapGetAddrReply;
import com.macrosan.filesystem.nfs.reply.PortMapReply;
import io.netty.buffer.ByteBuf;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.datagram.DatagramSocket;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Log4j2
public class PortMapHandler extends RpcHandler {

    public static Map<Integer, Integer[]> getPortMap = new ConcurrentHashMap<>(); // xid, [program,version}
    NetSocket socket;
    public DatagramSocket udpSocket;
    public SocketAddress address;

    public PortMapHandler(DatagramSocket socket, SocketAddress address) {
        this.udpSocket = socket;
        this.address = address;
    }

    public PortMapHandler(NetSocket socket) {
        this.socket = socket;
        this.address = socket.remoteAddress();
    }

    @Override
    protected void handleMsg(int offset, RpcCallHeader callHeader, ByteBuf msg) {
        //portmap
        RpcReply reply = null;

        if (NFS.nfsDebug) {
            log.info("PortMap call {}", callHeader);
        }

        if (callHeader.rpcVersion == 2 && callHeader.program == 100000 && callHeader.programVersion == 2
                && callHeader.opt == 3) {
            int status = msg.getInt(offset);
            int program = msg.getInt(offset);
            int programVersion = msg.getInt(offset + 4);
            int proto = msg.getInt(offset + 8);
            int port = msg.getInt(offset + 12);

            int returnPort = -1;

            //NFSV3 tcp
            if (program == 100003 && programVersion == 3 && (proto == 6 || proto == 17)) {
                returnPort = NFS.nfsPort;
            }
            //mount tcp
            else if (program == 100005 && programVersion == 3 && (proto == 6 || proto == 17)) {
                returnPort = NFS.mountPort;
            }
            //NLM tcp
            else if (program == 100021 && programVersion == 4 && (proto == 6 || proto == 17)) {
                returnPort = NFS.nlmPort;
            }
            //NSM tcp
            else if (program == 100024 && programVersion == 1 && (proto == 6 || proto == 17)) {
                returnPort = NFS.nsmPort;
            }

            //RQUOTA udp
            else if (program == 100011 && programVersion == 2 && (proto == 6 || proto == 17)) {
                returnPort = NFS.nfsRQuotaPort;
            }


            if (returnPort != -1) {
                SunRpcHeader header = SunRpcHeader.newReplyHeader(callHeader.getHeader().id);
                reply = new PortMapReply(header);
                ((PortMapReply) reply).port = returnPort;
            }
        } else if (callHeader.rpcVersion == 2 && callHeader.program == 100000 && callHeader.programVersion == 4 && callHeader.opt == 3) {
            int program = msg.getInt(offset);
            int programVersion = msg.getInt(offset + 4);
            //getAddr请求
            int returnPort = -1;

            if (program == 100003 && programVersion == 3) {
                returnPort = NFS.nfsPort;
            }
            //mount tcp
            else if (program == 100005 && programVersion == 3) {
                returnPort = NFS.mountPort;
            }
            //NLM tcp
            else if (program == 100021 && programVersion == 4) {
                returnPort = NFS.nlmPort;
            }
            //NSM tcp
            else if (program == 100024 && programVersion == 1) {
                returnPort = NFS.nsmPort;
            }
            //RQUOTA
            else if (program == 100011 && programVersion == 2) {
                returnPort = NFS.nfsRQuotaPort;
            }

            if (returnPort != -1) {
                int netIdLen = msg.getInt(offset + 8);
                //tcp udp
                byte[] netId = new byte[netIdLen];
                msg.getBytes(offset + 12, netId);
                offset += 12 + (netIdLen + 3) / 4 * 4;
                int addressLen = msg.getInt(offset);
                byte[] address = new byte[addressLen];
                msg.getBytes(offset + 4, address);
                //ip.portA.portB
                String addressStr = new String(address);
                int index = addressStr.lastIndexOf('.');
                String ip;
                if (index != -1) {
                    ip = addressStr.substring(0, index);
                    index = ip.lastIndexOf('.');
                    ip = addressStr.substring(0, index + 1);
                    int first = returnPort >> 8;
                    int secnond = returnPort & 0xff;
                    ip = ip + first + '.' + secnond;
                    SunRpcHeader header = SunRpcHeader.newReplyHeader(callHeader.getHeader().id);
                    reply = new PortMapGetAddrReply(header);
                    ((PortMapGetAddrReply) reply).len = ip.length();
                    ((PortMapGetAddrReply) reply).address = ip.getBytes();
                }
                offset += 4 + (addressLen + 3) / 4 * 4;
                int ownerLen = msg.getInt(offset);
                byte[] owner = new byte[ownerLen];
                msg.getBytes(offset + 4, owner);
            }
        } else if (callHeader.opt == 0) {
            reply = new NullReply(SunRpcHeader.newReplyHeader(callHeader.getHeader().id));
        }

        //program not available
        if (reply == null) {
            SunRpcHeader header = SunRpcHeader.newReplyHeader(callHeader.getHeader().id);
            reply = new PortMapReply(header);
            ((PortMapReply) reply).port = 0;
        }

        if (NFS.nfsDebug) {
            log.info("PortMap reply {}", reply);
        }

        if (socket != null) {

            socket.write(Buffer.buffer(replyToBuf(reply, 4096)));
        } else {
            sendUdpMsg(reply, udpSocket, address, 4096);
        }

    }

    @Override
    protected void handleRes(int offset, RpcReplyHeader replyHeader, ByteBuf msg) {
        int port = msg.getInt(offset);
        if (port != 0 && PortMapHandler.getPortMap.containsKey(replyHeader.getHeader().id)) {
            Integer[] portOwner = PortMapHandler.getPortMap.get(replyHeader.getHeader().id);
            if (portOwner[0] == 100021 && portOwner[1] == 4) {
                NLM4.NLMPortMap.put(address.host(), port);
            } else if (portOwner[0] == 100024 && portOwner[1] == 1) {
                NSM.NSMPortMap.put(address.host(), port);
            }
        }
    }
}
