package com.macrosan.rsocket.server;

import io.netty.buffer.*;
import io.rsocket.Payload;
import io.rsocket.frame.*;
import io.rsocket.frame.decoder.PayloadDecoder;

import static com.macrosan.storage.StoragePoolFactory.DEFAULT_PACKAGE_SIZE;

/**
 * @author gaozhiyuan
 */
public class MsPayloadDecoder implements PayloadDecoder {
    public static final MsPayloadDecoder DEFAULT = new MsPayloadDecoder();

    private static final UnpooledByteBufAllocator ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;

    @Override
    public Payload apply(ByteBuf byteBuf) {
        ByteBuf m;
        ByteBuf d;
        FrameType type = FrameHeaderFlyweight.frameType(byteBuf);
        switch (type) {
            case REQUEST_FNF:
                d = RequestFireAndForgetFrameFlyweight.data(byteBuf);
                m = RequestFireAndForgetFrameFlyweight.metadata(byteBuf);
                break;
            case REQUEST_RESPONSE:
                d = RequestResponseFrameFlyweight.data(byteBuf);
                m = RequestResponseFrameFlyweight.metadata(byteBuf);
                break;
            case REQUEST_STREAM:
                d = RequestStreamFrameFlyweight.data(byteBuf);
                m = RequestStreamFrameFlyweight.metadata(byteBuf);
                break;
            case REQUEST_CHANNEL:
                d = RequestChannelFrameFlyweight.data(byteBuf);
                m = RequestChannelFrameFlyweight.metadata(byteBuf);
                break;
            case NEXT:
            case NEXT_COMPLETE:
                d = PayloadFrameFlyweight.data(byteBuf);
                m = PayloadFrameFlyweight.metadata(byteBuf);
                break;
            case METADATA_PUSH:
                d = Unpooled.EMPTY_BUFFER;
                m = MetadataPushFrameFlyweight.metadata(byteBuf);
                break;
            default:
                throw new IllegalArgumentException("unsupported frame type: " + type);
        }

        ByteBuf metadata = new UnpooledUnsafeHeapByteBuf(ALLOCATOR, m.readableBytes(), m.readableBytes());
        ByteBuf data = new UnpooledUnsafeHeapByteBuf(ALLOCATOR, d.readableBytes(), d.readableBytes());

        data.writeBytes(d);
        metadata.writeBytes(m);

        return new MsPayload(metadata, data);
    }
}
