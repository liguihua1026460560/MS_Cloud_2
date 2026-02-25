package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.NFSV4;
import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS4ERR_CLID_INUSE;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS4ERR_DENIED;

@ToString(callSuper = true)
public class CompoundReply extends RpcReply {
    public int opt;
    public int status;
    public byte[] tag = new byte[0];
    public int count;
    private List<RpcReply> replies = new ArrayList<>();

    public CompoundReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        int statusOffset = offset;
        buf.setInt(offset, status);
        buf.setInt(offset + 4, tag.length);
        buf.setBytes(offset + 8, tag);
        offset += 8 + (tag.length + 3) / 4 * 4;
        buf.setInt(offset, replies.size());
        offset += 4;
        for (RpcReply rpcReply : replies) {
            CompoundReply compoundReply = (CompoundReply) rpcReply;
            if (compoundReply.status != 0) {
                if (NFSV4.Opcode.NFS4PROC_LOCK.opcode == compoundReply.opt
                        && NFS4ERR_DENIED == compoundReply.status) {
                    offset += rpcReply.writeStruct(buf, offset);
                } else if (NFSV4.Opcode.NFS4PROC_SETCLIENTID.opcode == compoundReply.opt
                        && NFS4ERR_CLID_INUSE == compoundReply.status) {
                    offset += rpcReply.writeStruct(buf, offset);
                } else if (NFSV4.Opcode.NFS4PROC_SETATTR.opcode == compoundReply.opt) {
                    offset += rpcReply.writeStruct(buf, offset);
                }else {
                    buf.setInt(offset, compoundReply.opt);
                    buf.setInt(offset + 4, compoundReply.status);
                    offset += 8;
                }
                buf.setInt(statusOffset, compoundReply.status);
                break;
            }
            offset += rpcReply.writeStruct(buf, offset);
        }
        return offset - start;
    }


    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        status = buf.getInt(offset);
        int tagLen = buf.getInt(offset + 4);
        tag = new byte[tagLen];
        buf.getBytes(offset + 8, tag);
        offset += 8 + (tag.length + 3) / 4 * 4;
        count = buf.getInt(offset);
        return offset + 4 - start;
    }

    public List<RpcReply> getReplies() {
        return replies;
    }
}
