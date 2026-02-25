package com.macrosan.utils.msutils.log4j;

import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.apache.logging.log4j.core.layout.ByteBufferDestinationHelper;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Plugin(name = "DeleteObject", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class DelObjRecordLoggerAppender extends AbstractAppender {
    private final Manager manager = new Manager();

    private DelObjRecordLoggerAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions, Property[] properties) {
        super(name, filter, layout, ignoreExceptions, properties);
    }

    @Override
    public void append(LogEvent event) {
        getLayout().encode(event, manager);

        if (event.isEndOfBatch()) {
            manager.flush();
        }
    }

    @PluginBuilderFactory
    public static <B extends DelObjRecordLoggerAppender.Builder<B>> B newBuilder() {
        return new DelObjRecordLoggerAppender.Builder<B>().asBuilder();
    }

    public static class Builder<B extends DelObjRecordLoggerAppender.Builder<B>> extends AbstractAppender.Builder<B>
            implements org.apache.logging.log4j.core.util.Builder<DelObjRecordLoggerAppender> {

        @Override
        public DelObjRecordLoggerAppender build() {
            return new DelObjRecordLoggerAppender(getName(), getFilter(), getLayout(), isIgnoreExceptions(), getPropertyArray());
        }
    }

    private static class Manager implements ByteBufferDestination {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[1 << 20]);

        @Override
        public synchronized ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        @Override
        public synchronized ByteBuffer drain(ByteBuffer buf) {
            if (buf != byteBuffer) {
                throw new UnsupportedOperationException("buf must this byteBuffer");
            }

            flush();
            return buf;
        }

        @Override
        public synchronized void writeBytes(ByteBuffer data) {
            ByteBufferDestinationHelper.writeToUnsynchronized(data, this);
        }

        @Override
        public synchronized void writeBytes(byte[] data, int offset, int length) {
            throw new UnsupportedOperationException("no implement writeBytes");
        }

        public synchronized void flush() {
            byteBuffer.flip();
            if (byteBuffer.remaining() > 0) {
                MaxSizeLogger.write(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
            }
            byteBuffer.clear();
        }
    }
}
