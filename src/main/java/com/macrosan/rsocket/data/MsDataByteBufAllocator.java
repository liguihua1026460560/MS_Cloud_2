package com.macrosan.rsocket.data;

import com.macrosan.rsocket.data.DataServer.DataDecoder;
import com.macrosan.utils.msutils.UnsafeUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.log4j.Log4j2;

import java.lang.reflect.Field;

@Log4j2
public class MsDataByteBufAllocator implements ByteBufAllocator {
    public static final ByteBufAllocator POOLED = PooledByteBufAllocator.DEFAULT;
    static long firstOffset;

    static {
        try {
            Field field = ByteToMessageDecoder.class.getDeclaredField("first");
            firstOffset = UnsafeUtils.unsafe.objectFieldOffset(field);
        } catch (Exception e) {
            log.error("get field fail", e);
        }
    }

    DataDecoder decoder;

    public MsDataByteBufAllocator(DataDecoder decoder) {
        this.decoder = decoder;
    }


    @Override
    public ByteBuf buffer() {
        return POOLED.buffer();
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        return POOLED.buffer(initialCapacity);
    }

    @Override
    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        return POOLED.buffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf ioBuffer() {
        return POOLED.ioBuffer();
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity) {
        return POOLED.ioBuffer(initialCapacity);
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
        return POOLED.ioBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf heapBuffer() {
        return POOLED.heapBuffer();
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        return POOLED.heapBuffer(initialCapacity);
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        return POOLED.heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf directBuffer() {
        return POOLED.directBuffer();
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity) {
        return POOLED.directBuffer(initialCapacity);
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        return POOLED.directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        return POOLED.compositeHeapBuffer();
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        return POOLED.compositeHeapBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        return POOLED.compositeHeapBuffer();
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        return POOLED.compositeHeapBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        return POOLED.compositeDirectBuffer();
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        return POOLED.compositeDirectBuffer(maxNumComponents);
    }

    @Override
    public boolean isDirectBufferPooled() {
        return true;
    }

    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        return POOLED.calculateNewCapacity(minNewCapacity, maxCapacity);
    }
}
