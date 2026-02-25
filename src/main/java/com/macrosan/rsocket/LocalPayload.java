package com.macrosan.rsocket;

import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class LocalPayload<T> implements Payload {
    public static final LocalPayload<Inode> RETRY_INODE_PAYLOAD = new LocalPayload<>(PayloadMetaType.SUCCESS, Inode.RETRY_INODE.clone());
    public static final LocalPayload<String> SUCCESS_PAYLOAD = new LocalPayload<>(PayloadMetaType.SUCCESS, "");
    public static final LocalPayload<String> ERROR_PAYLOAD = new LocalPayload<>(PayloadMetaType.ERROR, "");


    public PayloadMetaType type;
    public T data;

    public LocalPayload(PayloadMetaType type, T data) {
        this.type = type;
        this.data = data;
    }


    @Override
    public boolean hasMetadata() {
        return true;
    }

    @Override
    public ByteBuf sliceMetadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf sliceData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf data() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf metadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int refCnt() {
        return 0;
    }

    @Override
    public Payload retain() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Payload retain(int increment) {
        throw new UnsupportedOperationException();
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

    @Override
    public ByteBuffer getMetadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMetadataUtf8() {
        return type.name();
    }

    @Override
    public String getDataUtf8() {
        return sliceData().toString(StandardCharsets.UTF_8);
    }
}
