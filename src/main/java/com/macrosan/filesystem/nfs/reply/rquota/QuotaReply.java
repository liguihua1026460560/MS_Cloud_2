package com.macrosan.filesystem.nfs.reply.rquota;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NFSQuotaStatus.Q_OK;
import static com.macrosan.filesystem.quota.FSQuotaConstants.*;
import static com.macrosan.filesystem.utils.FSQuotaUtils.safeToInt;

@ToString
public class QuotaReply extends RpcReply {

    public int status;
    public int bsize;
    public int quotaActive;
    public int blockHardLimit;
    public int blockSoftLimit;
    public int curBlocks;
    public int filesHardLimit;
    public int filesSoftLimit;
    public int curFiles;
    public int blockTimeLeft;
    public int filesTimeLeft;


    public QuotaReply(SunRpcHeader header) {
        super(header);
    }


    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, this.status);
        buf.setInt(offset + 4, this.bsize);
        buf.setInt(offset + 8, this.quotaActive);
        buf.setInt(offset + 12, this.blockHardLimit);
        buf.setInt(offset + 16, this.blockSoftLimit);
        buf.setInt(offset + 20, this.curBlocks);
        buf.setInt(offset + 24, this.filesHardLimit);
        buf.setInt(offset + 28, this.filesSoftLimit);
        buf.setInt(offset + 32, this.curFiles);
        buf.setInt(offset + 36, this.blockTimeLeft);
        buf.setInt(offset + 40, this.filesTimeLeft);
        return offset + 44 - start;
    }

    public static void buildReply(String blockHardLimit, String blockSoftLimit, String curCap, String filesHardLimit, String filesSoftLimit, String curFiles, QuotaReply reply) {
        reply.status = Q_OK;
        reply.quotaActive = 1;
        reply.bsize = (int )BLOCK_SIZE;
        long capQuotaValue = Long.parseLong(blockHardLimit);
        long capQuotaSoftValue = Long.parseLong(blockSoftLimit);
        long objNumQuotaValue = Long.parseLong(filesHardLimit);
        long objNumQuotaSoftValue = Long.parseLong(filesSoftLimit);
        long totalBytes = Long.parseLong(curCap);
        long totalObjNum = Long.parseLong(curFiles);
        reply.curFiles = safeToInt(totalObjNum);
        reply.blockHardLimit = safeToInt(Math.ceil(capQuotaValue / BLOCK_SIZE));
        reply.blockSoftLimit = safeToInt(Math.ceil(capQuotaSoftValue / BLOCK_SIZE));
        reply.filesHardLimit = safeToInt(objNumQuotaValue);
        reply.filesSoftLimit = safeToInt(objNumQuotaSoftValue);
        reply.curBlocks = safeToInt(Math.ceil(totalBytes / BLOCK_SIZE));
    }
}
