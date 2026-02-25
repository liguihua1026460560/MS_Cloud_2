package com.macrosan.rsocket.server;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;

public class MsPayload implements Payload {

    public ByteBuf meta;
    public ByteBuf data;
    public int loopIndex = -1;

    public MsPayload(ByteBuf meta, ByteBuf data) {
        this.meta = meta;
        this.data = data;
    }

    @Override
    public boolean hasMetadata() {
        return meta != null && meta.readableBytes() > 0;
    }

    @Override
    public ByteBuf sliceMetadata() {
        return meta.slice();
    }

    @Override
    public ByteBuf sliceData() {
        return data.slice();
    }

    @Override
    public ByteBuf data() {
        return data;
    }

    @Override
    public ByteBuf metadata() {
        return meta;
    }

    @Override
    public int refCnt() {
        return 0;
    }

    @Override
    public Payload retain() {
        return this;
    }

    @Override
    public Payload retain(int increment) {
        return this;
    }

    @Override
    public Payload touch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Payload touch(Object hint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean release() {
        return false;
    }

    @Override
    public boolean release(int decrement) {
        return false;
    }
}
