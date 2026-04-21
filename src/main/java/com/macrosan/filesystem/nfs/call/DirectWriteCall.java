package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicBoolean;

@ToString(exclude = "bytes")
public class DirectWriteCall implements ReadStruct {

    public FH2 fh = new FH2();
    public long writeOffset = 0;
    public int count;
    public int sync;
    public int dataLen;
    public ByteBuf bytes;

    private AtomicBoolean closed = new AtomicBoolean(false);

    public int readStruct(ByteBuf buf, int offset) {
        int fhRead = fh.readStruct(buf, offset);
        if (fhRead < 0) {
            return -1;
        }

        offset += fhRead;

        writeOffset = buf.getLong(offset);
        count = buf.getInt(offset + 8);
        sync = buf.getInt(offset + 12);
        dataLen = buf.getInt(offset + 16);
        if (count != dataLen) {
            return -1;
        }

        bytes = PooledByteBufAllocator.DEFAULT.directBuffer(dataLen + 4096, dataLen + 4096);
        long c = (bytes.memoryAddress() + 4095) & ~4095;
        int skip = Math.toIntExact(c - bytes.memoryAddress());
        bytes.writerIndex(skip);
        bytes.readerIndex(skip);

        buf.getBytes(offset + 20, bytes, dataLen);

        return fhRead + 20 + dataLen;
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (bytes != null) {
                bytes.release();
            }
        }
    }

    @Override
    @Deprecated
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
}
