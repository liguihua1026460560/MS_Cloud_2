package com.macrosan.filesystem.nfs.handler;

import com.macrosan.filesystem.nfs.*;
import com.macrosan.filesystem.nfs.auth.AuthReply;
import com.macrosan.filesystem.nfs.call.v4.CompoundCall;
import com.macrosan.filesystem.nfs.reply.ReadReply;
import com.macrosan.filesystem.nfs.types.FAttr3;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledUnsafeHeapByteBuf;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.datagram.DatagramSocket;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.extern.log4j.Log4j2;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

@Log4j2
public abstract class RpcHandler {

    int msgLen = -1;

    LinkedList<ByteBuf> bufList = new LinkedList<>();
    int writeIndex = 0;

    public boolean isUdp = false;


    protected abstract void handleMsg(int offset, RpcCallHeader callHeader, ByteBuf msg);

    protected abstract void handleRes(int offset, RpcReplyHeader replyHeader, ByteBuf msg);

    protected ByteBuf replyToBuf(RpcReply reply, int bufSize) {
        bufSize = getRealBufSize(reply, bufSize);
        ByteBuf buf = new UnpooledUnsafeHeapByteBuf(UnpooledByteBufAllocator.DEFAULT, bufSize, bufSize);

        int realSize = reply.writeStruct(buf, SunRpcHeader.SIZE);
        reply.getHeader().len = realSize + SunRpcHeader.SIZE - 4;

        reply.getHeader().writeStruct(buf, 0);

        buf.writerIndex(reply.getHeader().len + 4);

        if (NFS.nfsDebug) {
            log.info("nfs reply {}", reply);
        }

        return buf;
    }

    protected ByteBuf replyToBufUdp(RpcReply reply, int bufSize) {
        bufSize = getRealBufSize(reply, bufSize);
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(bufSize, bufSize);
        //udp协议无分片头部
        int realSize = reply.writeStruct(buf, SunRpcHeader.SIZE - 4);
        reply.getHeader().len = realSize + SunRpcHeader.SIZE - 8;

        reply.getHeader().writeStructUdp(buf, 0);

        buf.writerIndex(reply.getHeader().len + 4);

        if (NFS.nfsDebug) {
            log.info("nfs reply {}", reply);
        }

        return buf;
    }

    public int getRealBufSize(RpcReply reply, int bufSize) {
        if (reply instanceof ReadReply) {
            ReadReply readReply = ((ReadReply) reply);
            bufSize = SunRpcHeader.SIZE + RpcReply.SIZE + FAttr3.SIZE + 20 + (readReply.data != null ? (readReply.data.length + 3) / 4 * 4 : 0);
        } else if (reply instanceof AuthReply) {
            RpcReply reply0 = ((AuthReply) reply).reply;
            if (reply0 instanceof ReadReply) {
                ReadReply readReply = ((ReadReply) reply0);
                bufSize = SunRpcHeader.SIZE + RpcReply.SIZE + FAttr3.SIZE + 20 + (readReply.data != null ? (readReply.data.length + 3) / 4 * 4 : 0);
            }
        }
        return bufSize;
    }

    public ByteBuf callToBuf(RpcCall call, int bufSize) {
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(bufSize, bufSize);

        int realSize = call.writeStruct(buf, SunRpcHeader.SIZE);

        call.getHeader().len = realSize + SunRpcHeader.SIZE - 4;
        call.getHeader().writeStruct(buf, 0);

        buf.writerIndex(call.getHeader().len + 4);

        if (NFS.nfsDebug) {
            log.info("nfs call {}", call);
        }

        return buf;
    }


    public ByteBuf callToBuf(CompoundCall call, int bufSize) {
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(bufSize, bufSize);

        int realSize = call.writeStruct0(buf, SunRpcHeader.SIZE);

        call.getCallHeader().header.len = realSize + SunRpcHeader.SIZE - 4;
        call.getCallHeader().header.writeStruct(buf, 0);

        buf.writerIndex(call.getCallHeader().header.len + 4);

        if (NFS.nfsDebug) {
            log.info("nfs call {}", call);
        }

        return buf;
    }


    public ByteBuf callToBufUdp(RpcCall call, int bufSize) {
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(bufSize, bufSize);
        //udp协议无分片头部
        int realSize = call.writeStruct(buf, SunRpcHeader.SIZE - 4);

        call.getHeader().len = realSize + SunRpcHeader.SIZE - 8;
        call.getHeader().writeStructUdp(buf, 0);

        buf.writerIndex(call.getHeader().len + 4);

        if (NFS.nfsDebug) {
            log.info("nfs call {}", call);
        }

        return buf;
    }

    private void clear(ByteBuf msg) {
        int clearLen = msgLen + 4;
        writeIndex -= clearLen;

        ListIterator<ByteBuf> iterator = bufList.listIterator();
        while (iterator.hasNext() && clearLen > 0) {
            ByteBuf buf = iterator.next();
            if (buf.readableBytes() <= clearLen) {
                clearLen -= buf.readableBytes();
//                buf.release();
                iterator.remove();
            } else {
                buf.retain();
                buf.readerIndex(buf.readerIndex() + clearLen);
            }
        }
        msg.release();
        msgLen = -1;
    }

    /**
     * @return isLastMsg
     */
    public void readMsg(ByteBuf msg) {
        SunRpcHeader header = new SunRpcHeader();

        int offset = header.read(msg, 0, isUdp);
        if (offset < 0) {
            log.error("read rpc header fail {}", header);
            return;
        }

        if (header.msgType == 0) {
            RpcCallHeader callHeader = new RpcCallHeader(header);
            int read = callHeader.readStruct(msg, offset);
            if (read < 0) {
                log.error("read nfs header fail {}", callHeader);
            }

            offset += read;
            int auth = callHeader.auth.auth(callHeader, msg, offset);
            if (auth < 0) {
                log.error("nfs auth fail {}", callHeader);
            }
            offset += auth;


            handleMsg(offset, callHeader, msg);
        } else if (header.msgType == 1) {
            RpcReplyHeader replyHeader = new RpcReplyHeader(header);
            int read = replyHeader.readStruct(msg, offset);
            if (read < 0) {
                log.error("read nfs header fail {}", replyHeader);
            }
            if (replyHeader.replyState != 0 || replyHeader.acceptState != 0) {
                log.error("nfs res failed {}", replyHeader);
            }
            offset += read;
            handleRes(offset, replyHeader, msg);
        }
    }

    public void sendUdpMsg(RpcReply reply, DatagramSocket udpSocket, SocketAddress address, int bufSize) {
        Buffer buf = Buffer.buffer(replyToBufUdp(reply, bufSize));
        udpSocket.send(buf, address.port(), address.host(), res -> {
            if (!res.succeeded()) {
                log.error("Listen portmap failed, port : {} cause : {}", address.port(), res.cause());
            }
        });
    }

    int last;

    private void loop() {
        if (msgLen < 0 && writeIndex >= 4) {
            ByteBuf buf = bufList.get(0);

            if (buf.readableBytes() < 4) {
                byte[] bytes = new byte[4];
                int index = 0;
                int n = 0;
                while (n < bytes.length) {
                    int read = Math.min(bufList.get(index).readableBytes(), bytes.length - n);
                    int start = bufList.get(index).readerIndex();
                    for (int k = 0; k < read; k++) {
                        bytes[n + k] = bufList.get(index).getByte(start + k);
                    }
                    index++;
                    n += read;
                }

                msgLen = (bytes[0] & 0xFF) << 24 |
                        (bytes[1] & 0xFF) << 16 |
                        (bytes[2] & 0xFF) << 8 |
                        (bytes[3] & 0xFF);
            } else {
                msgLen = buf.getInt(buf.readerIndex());
            }

            //udp协议无分片头部，此时的msgLen实际上获取的是xid，消息长度为buf.readableBytes() - 4
            if (isUdp) {
                msgLen = buf.readableBytes() - 4;
                last = -1;
            } else {
                last = msgLen & 0xff000000;
                msgLen &= 0x00ffffff;
            }
        }

        if (msgLen >= 0) {
            if (writeIndex < msgLen + 4) {
                return;
            }

            if (last == 0) {
                ByteBuf msg = Unpooled.wrappedBuffer(bufList.toArray(new ByteBuf[bufList.size()]));
                paddingLen += msgLen;
                int clearLen = msgLen + 4;
                writeIndex -= clearLen;
                padding.add(msg.slice(4, msgLen));

                ListIterator<ByteBuf> iterator = bufList.listIterator();
                while (iterator.hasNext() && clearLen > 0) {
                    ByteBuf buf = iterator.next();
                    if (buf.readableBytes() <= clearLen) {
                        clearLen -= buf.readableBytes();
                        iterator.remove();
                    } else {
                        buf.readerIndex(buf.readerIndex() + clearLen);
                    }
                }

                msgLen = -1;
            } else {
                ByteBuf msg;
                if (paddingLen != 0) {
                    ByteBuf[] arr = new ByteBuf[1 + padding.size() + bufList.size()];
                    arr[0] = UnpooledByteBufAllocator.DEFAULT.heapBuffer(4);
                    arr[0].writerIndex(4);
                    arr[0].setInt(0, 0x80000000 | (paddingLen + msgLen));
                    int i = 1;
                    for (ByteBuf pad : padding) {
                        arr[i++] = pad;
                    }
                    for (ByteBuf pad : bufList) {
                        arr[i++] = pad;
                    }

                    msg = Unpooled.wrappedBuffer(arr);

                    paddingLen = 0;
                    for (ByteBuf b : padding) {
//                        b.release();
                    }

                    padding.clear();
                } else {
                    msg = Unpooled.wrappedBuffer(bufList.toArray(new ByteBuf[bufList.size()]));
                }
                readMsg(msg);
                clear(msg);
                loop();
            }
        }

    }

    int paddingLen = 0;
    List<ByteBuf> padding = new LinkedList<>();

    public void handle(ByteBuf buf) {
        buf.retain();
        bufList.add(buf);
        writeIndex += buf.readableBytes();
        loop();
    }
}
