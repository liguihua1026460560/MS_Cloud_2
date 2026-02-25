package com.macrosan.message.socketmsg;

import com.dslplatform.json.CompiledJson;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.macrosan.utils.msutils.UnsafeUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import sun.misc.Unsafe;

import java.util.HashMap;
import java.util.Map;

public class SocketDataMsg extends SocketReqMsg {
    private Map<String, Object> objMap = new HashMap<>();
    public byte[] data;

    public SocketDataMsg() {
        super();
    }

    public SocketDataMsg(String msgType, long msgLen) {
        super(msgType, msgLen);
    }

    public SocketDataMsg put(String key, Object value) {
        objMap.put(key, value);
        return this;
    }

    public SocketDataMsg put(String key, String value) {
        objMap.put(key, value);
        return this;
    }

    public SocketDataMsg putData(byte[] data) {
        this.data = data;
        return this;
    }

    public SocketReqMsg replace(String key, String oldValue, String newValue) {
        throw new UnsupportedOperationException("SocketDataMsg not support replace");
    }

    public SocketReqMsg put(Map<String, String> map) {
        throw new UnsupportedOperationException("SocketDataMsg not support put map");
    }

    public String get(String key) {
        return (String) objMap.get(key);
    }

    public int getInt(String key) {
        return (int) objMap.get(key);
    }

    public long getLong(String key) {
        return (long) objMap.get(key);
    }

    public String getAndRemove(String key) {
        throw new UnsupportedOperationException("SocketDataMsg not support getAndRemove");
    }

    /**
     * 不 copy data
     * @return
     */
    public SocketDataMsg copy() {
        SocketDataMsg copy = new SocketDataMsg(msgType, msgLen);
        copy.objMap.putAll(objMap);
        return copy;
    }

    @Override
    public ByteBuf toBytes() {
        int size = objMap.size();
        //objMap 最大长度128
        byte head = (byte) (size & 0x7f);
        int bytesSize = 1;

        head |= 0x80;
        //data.length
        bytesSize += 4;

        byte[][] keys = new byte[size][];
        Object[] values = new Object[size];
        int[] type = new int[size];

        int i = 0;
        //key 最大长度64K,value最大 32K
        for (Map.Entry<String, Object> entry : objMap.entrySet()) {
            byte[] key = entry.getKey().getBytes();
            keys[i] = key;
            bytesSize += 4 + key.length;
            Object value = entry.getValue();

            if (value == null) {
                values[i] = null;
                type[i] = 0;
            } else if (value instanceof String) {
                type[i] = 1;
                values[i] = ((String) value).getBytes();
                bytesSize += ((byte[]) values[i]).length;
            } else if (value instanceof Integer) {
                type[i] = 2;
                values[i] = value;
                bytesSize += 4;
            } else if (value instanceof Long) {
                type[i] = 3;
                values[i] = value;
                bytesSize += 8;
            } else {
                throw new UnsupportedOperationException("SocketDataMsg not support " + value.getClass());
            }

            i++;
        }

        byte[] bytes = new byte[bytesSize];
        ByteBuf buf = data == null ? Unpooled.wrappedBuffer(bytes) : Unpooled.wrappedBuffer(bytes, data);
        buf.writerIndex(0);
        buf.writeByte(head);

        for (i = 0; i < keys.length; i++) {
            byte[] key = keys[i];
            buf.writeChar(key.length);
            buf.writeBytes(key);

            Object value = values[i];

            switch (type[i]) {
                case 0:
                    buf.writeChar(0xffff);
                    break;
                case 1:
                    byte[] valueBytes = (byte[]) value;
                    buf.writeChar(valueBytes.length);
                    buf.writeBytes(valueBytes);
                    break;
                case 2:
                    buf.writeChar(0xfffe);
                    buf.writeInt((Integer) value);
                    break;
                case 3:
                    buf.writeChar(0xfffd);
                    buf.writeLong((Long) value);
                    break;
                default:
                    throw new UnsupportedOperationException("SocketDataMsg not support " + value.getClass());
            }
        }

        if (data != null && data.length > 0) {
            buf.writeInt(data.length);
            buf.writerIndex(buf.writerIndex() + data.length);
        } else {
            buf.writeInt(-1);
        }

        return buf;
    }

    public static SocketDataMsg toSocketReqMsg(ByteBuf buf) {
        int size = buf.readByte() & 0x7f;
        SocketDataMsg msg = new SocketDataMsg();

        for (int i = 0; i < size; i++) {
            int keyLen = buf.readChar();
            byte[] key = new byte[keyLen];
            buf.readBytes(key);

            int valueLen = buf.readChar();
            switch (valueLen) {
                case 0xffff:
                    msg.put(new String(key), null);
                    break;
                case 0xfffe:
                    msg.put(new String(key), buf.readInt());
                    break;
                case 0xfffd:
                    msg.put(new String(key), buf.readLong());
                    break;
                default:
                    byte[] value = new byte[valueLen];
                    buf.readBytes(value);
                    msg.put(new String(key), new String(value));
                    break;
            }
        }

        int dataLen = buf.readInt();
        if (dataLen != -1) {
            msg.data = new byte[dataLen];
            buf.readBytes(msg.data);
        }

        return msg;
    }

}
