package com.macrosan.filesystem.cifs.types.smb2.pipe;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@Log4j2
public class BindPduInfo extends GetInfoReply.Info {

    private byte version = 5;
    private byte versionMinor = 0;
    private byte pduType;
    private byte pduFlags = 0x03;
    private int dataRep = 0x00000010;
    private short fragLength;
    private short authLength = 0;
    private int callId;
    private short maxXmitFrag = 4280;
    private short maxRecvFrag = 4280;
    private int assocGroup;
    private short numCtxItems;
    private List<ContextItem> contextItems = new ArrayList<>();


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        // 头部至少 28 字节
        if (buf == null || buf.capacity() < offset + 28 || buf.writerIndex() < offset + 28) {
            log.debug("Bind PDU header too short: available={}, required=28, offset={}",
                    buf == null ? 0 : buf.writerIndex() - offset, offset);
            throw new IllegalArgumentException("Incomplete Bind PDU header");
        }
        int start = offset;

        /*if (buf == null || offset < 0 || buf.writerIndex() < offset + 28) {
            log.info("【buf:{}】",buf);
            throw new IndexOutOfBoundsException("Bind PDU header too short");
        }*/

        version = buf.getByte(offset++);
        versionMinor = buf.getByte(offset++);
        pduType = buf.getByte(offset++);
        pduFlags = buf.getByte(offset++);
        dataRep = buf.getIntLE(offset); offset += 4;
        fragLength = buf.getShortLE(offset); offset += 2;
        authLength = buf.getShortLE(offset); offset += 2;
        callId = buf.getIntLE(offset); offset += 4;
        if (fragLength < 28) {
            log.debug("Invalid fragLength: {}", fragLength & 0xFFFF);
            throw new IllegalArgumentException("Invalid Bind PDU fragLength");
        }

        // 检查剩余数据是否足够头部剩余部分 + 上下文
        int remainingAfterHeader = buf.writerIndex() - offset;
        if (remainingAfterHeader < 12) {  // maxXmitFrag ~ numCtxItems + reserved
            log.debug("Bind PDU insufficient for fixed fields: remaining={}, required>=12", remainingAfterHeader);
            throw new IllegalArgumentException("Incomplete Bind PDU fixed fields");
        }

        maxXmitFrag = buf.getShortLE(offset); offset += 2;
        maxRecvFrag = buf.getShortLE(offset); offset += 2;
        assocGroup = buf.getIntLE(offset); offset += 4;
        numCtxItems = buf.getShortLE(offset); offset += 2;
        offset += 2;  // skip reserved 2 bytes

        // 检查剩余长度是否足够所有 context items（每个至少 44 字节）
        int requiredForContexts = numCtxItems * 44;
        int remaining = buf.writerIndex() - offset;
        if (remaining < requiredForContexts) {
            log.error("Bind PDU insufficient for {} contexts: remaining={}, required={}",
                    numCtxItems, remaining, requiredForContexts);
            throw new IllegalArgumentException("Incomplete Bind PDU contexts");
        }

        contextItems.clear();
        for (int i = 0; i < numCtxItems; i++) {
            ContextItem ctx = new ContextItem();

            ctx.setContextId(buf.getShortLE(offset)); offset += 2;
            ctx.setNumTransItems(buf.getByte(offset++) & 0xFF);
            offset++;  // skip reserved byte

            byte[] absUuidBytes = new byte[16];
            buf.getBytes(offset, absUuidBytes);
            ctx.setAbstractSyntaxUuid(uuidLittleEndianToString(absUuidBytes));
            offset += 16;
            ctx.setAbstractSyntaxVersion(buf.getIntLE(offset)); offset += 4;

            byte[] transUuidBytes = new byte[16];
            buf.getBytes(offset, transUuidBytes);
            ctx.setTransferSyntaxRawBytes(transUuidBytes);
            ctx.setTransferSyntaxUuid(uuidLittleEndianToString(transUuidBytes));
            offset += 16;
            ctx.setTransferSyntaxVersion(buf.getIntLE(offset));
            offset += 4;

            contextItems.add(ctx);
        }

        // authLength
        if (authLength > 0) {
            if (buf.writerIndex() < offset + authLength) {
                log.debug("Auth data incomplete: remaining={}, authLength={}",
                        buf.writerIndex() - offset, authLength);
            } else {
                offset += authLength;
            }
        }

        return offset - start;
    }

    @Override
    public int size() {
        return 28 + numCtxItems * 44;
    }

    @Data
    public static class ContextItem {
        private short contextId;
        private int numTransItems;
        private String abstractSyntaxUuid;
        private int abstractSyntaxVersion;
        private String transferSyntaxUuid;
        private int transferSyntaxVersion;
        private byte[] transferSyntaxRawBytes;
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;

        buf.setByte(offset++, version);
        buf.setByte(offset++, versionMinor);
        buf.setByte(offset++, pduType);
        buf.setByte(offset++, pduFlags);
        buf.setIntLE(offset, dataRep);
        offset += 4;
        buf.setShortLE(offset, fragLength);  // 稍后回填
        offset += 2;
        buf.setShortLE(offset, authLength);
        offset += 2;
        buf.setIntLE(offset, callId);
        offset += 4;
        buf.setShortLE(offset, maxXmitFrag);
        offset += 2;
        buf.setShortLE(offset, maxRecvFrag);
        offset += 2;
        buf.setIntLE(offset, assocGroup);
        offset += 4;
        buf.setShortLE(offset, numCtxItems);
        offset += 2;
        buf.setShortLE(offset, 0);
        offset += 2;

        // 写入每个 Context Item
        for (ContextItem ctx : contextItems) {
            buf.setShortLE(offset, ctx.getContextId());
            offset += 2;
            buf.setShortLE(offset, ctx.getNumTransItems());
            offset += 2;

            // Abstract Syntax UUID + Version
            byte[] absUuidBytes = uuidStringToLittleEndian(ctx.getAbstractSyntaxUuid());
            buf.setBytes(offset, absUuidBytes);
            offset += 16;
            buf.setIntLE(offset, ctx.getAbstractSyntaxVersion());
            offset += 4;

            // Transfer Syntax UUID + Version
            byte[] transUuidBytes = uuidStringToLittleEndian(ctx.getTransferSyntaxUuid());
            buf.setBytes(offset, transUuidBytes);
            offset += 16;
            buf.setIntLE(offset, ctx.getTransferSyntaxVersion());
            offset += 4;
        }

        // 回填 Frag Length
        int totalLength = offset - start;
        buf.setShortLE(start + 8, (short) totalLength);  // offset 8-9 是 Frag Length

        return totalLength;
    }

    /**
     * 将 little-endian 的 16 字节 UUID 数组转换为标准字符串格式
     */
    private static String uuidLittleEndianToString(byte[] bytes) {
        if (bytes == null || bytes.length != 16) {
            return "";
        }

        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        // time_low (4 bytes)
        int timeLow = bb.getInt();

        // time_mid (2 bytes)
        short timeMid = bb.getShort();

        // time_hi_and_version (2 bytes)
        short timeHiAndVersion = bb.getShort();

        // clock_seq (2 bytes)
        short clockSeq = bb.getShort();

        // node (6 bytes)
        byte[] node = new byte[6];
        bb.get(node);

        int clockSeqSwapped = ((clockSeq & 0xFF) << 8) | ((clockSeq >> 8) & 0xFF);

        return String.format("%08x-%04x-%04x-%04x-%02x%02x%02x%02x%02x%02x",
                timeLow & 0xFFFFFFFFL,
                timeMid & 0xFFFF,
                timeHiAndVersion & 0xFFFF,
                clockSeqSwapped & 0xFFFF,
                node[0] & 0xFF, node[1] & 0xFF, node[2] & 0xFF,
                node[3] & 0xFF, node[4] & 0xFF, node[5] & 0xFF
        ).toLowerCase();
    }

    private static byte[] uuidStringToLittleEndian(String uuidStr) {
        uuidStr = uuidStr.replace("-", "");
        if (uuidStr.length() != 32) throw new IllegalArgumentException("Invalid UUID");

        ByteBuffer bb = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(Integer.parseUnsignedInt(uuidStr.substring(0, 8), 16));
        bb.putShort(Short.parseShort(uuidStr.substring(8, 12), 16));
        bb.putShort(Short.parseShort(uuidStr.substring(12, 16), 16));
        bb.putShort(Short.parseShort(uuidStr.substring(16, 20), 16));
        bb.putLong(Long.parseUnsignedLong(uuidStr.substring(20), 16));
        return bb.array();
    }

    public static BindPduInfo tryParseBindPdu(byte[] data) {
        if (data == null || data.length < 4) {
            log.warn("Data too short to read pduType: length={}", data == null ? 0 : data.length);
            return null;
        }

        byte pduType = data[2];  // pduType 在偏移 2（第3字节）

        if (pduType != 0x0B && pduType != 0x0E) {
            log.debug("Skipping non-Bind/Alter_context PDU: pduType=0x{:02X}, length={}", pduType, data.length);
            return null;
        }

        // 提前检查 fragLength（从头读取，避免完整解析失败浪费资源）
        if (data.length < 10) {
            log.warn("Data too short to read fragLength: length={}, pduType=0x{:02X}", data.length, pduType);
            return null;
        }
        short fragLength = ByteBuffer.wrap(data, 8, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
        if (data.length < fragLength) {
            log.warn("Incomplete PDU: length={}, fragLength={}, pduType=0x{:02X}",
                    data.length, fragLength & 0xFFFF, pduType);
            return null;
        }

        BindPduInfo bindPdu = new BindPduInfo();
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        int readLen = bindPdu.readStruct(buf, 0);

        if (readLen < 28 || bindPdu.getContextItems().isEmpty()) {
            log.warn("Incomplete {} PDU: readLen={}, contexts={}, pduType=0x{:02X}, fragLength={}",
                    pduType == 0x0B ? "Bind" : "Alter_context", readLen,
                    bindPdu.getContextItems().size(), pduType, fragLength & 0xFFFF);
            return null;
        }

        log.info("{} PDU parsed successfully: contexts={}, callId={}, fragLength={}",
                pduType == 0x0B ? "Bind" : "Alter_context",
                bindPdu.getContextItems().size(), bindPdu.getCallId(), fragLength & 0xFFFF);

        return bindPdu;
    }

    /**
     * 合并 Alter_context 到现有 BindPduInfo
     * - 更新动态字段（如 callId, frag 参数, assoc_group 等）
     * - 根据 context_id 更新或添加新上下文
     */
    public void mergeAlterContext(BindPduInfo alterPdu) {
        if (alterPdu == null || alterPdu.getContextItems().isEmpty()) {
            log.debug("No Alter_context to merge (empty or null)");
            return;
        }

        // 更新动态字段（Alter_context 中的值优先）
        this.setCallId(alterPdu.getCallId());
        this.setPduFlags(alterPdu.getPduFlags());

        // 合并上下文（根据 context_id）
        for (ContextItem newCtx : alterPdu.getContextItems()) {
            boolean found = false;
            for (ContextItem oldCtx : getContextItems()) {
                if (oldCtx.getContextId() == newCtx.getContextId()) {
                    // 更新已有上下文
                    oldCtx.setTransferSyntaxUuid(newCtx.getTransferSyntaxUuid());
                    oldCtx.setTransferSyntaxVersion(newCtx.getTransferSyntaxVersion());
                    oldCtx.setTransferSyntaxRawBytes(newCtx.getTransferSyntaxRawBytes());
                    found = true;
                    log.debug("Updated context id={}, transfer syntax={}",
                            oldCtx.getContextId(), oldCtx.getTransferSyntaxUuid());
                    break;
                }
            }
            if (!found) {
                // 添加新上下文
                getContextItems().add(newCtx);
                log.debug("Added new context id={}, transfer syntax={}",
                        newCtx.getContextId(), newCtx.getTransferSyntaxUuid());
            }
        }

        log.info("Applied Alter_context: new callId={}, contexts now={}",
                getCallId(), getContextItems().size());
    }
}
