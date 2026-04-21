package com.macrosan.message.socketmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

/**
 * SocketReqMsg
 *
 * @author liyixin
 * @date 2018/12/3
 */
@CompiledJson(onUnknown = CompiledJson.Behavior.DEFAULT)
@Data
@Accessors(chain = true)
public class SocketReqMsg {

    @JsonAttribute(nullable = false)
    public String version = "1.0";

    @JsonAttribute(nullable = false)
    public String msgType;

    @JsonAttribute(nullable = false)
    public long msgLen = 0;

    @JsonAttribute(nullable = false)
    public Map<String, String> dataMap;

    public SocketReqMsg() {
    }

    public SocketReqMsg(String msgType, long msgLen) {
        this.msgType = msgType;
        this.msgLen = msgLen;
        dataMap = new HashMap<>(16);
    }

    public SocketReqMsg put(String key, String value) {
        dataMap.put(key, value);
        return this;
    }

    public SocketReqMsg replace(String key, String oldValue, String newValue) {
        dataMap.replace(key, oldValue, newValue);
        return this;
    }

    public SocketReqMsg put(Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            dataMap.put(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public String get(String key) {
        return dataMap.get(key);
    }

    public String getAndRemove(String key) {
        return dataMap.remove(key);
    }

    public SocketReqMsg copy() {
        SocketReqMsg copy = new SocketReqMsg(msgType, msgLen);
        copy.dataMap.putAll(dataMap);
        return copy;
    }

    public ByteBuf toDirectBytes() {
        int size = dataMap.size();
        //不能超过4096
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(4096);
        buf.writeInt(size);

        for (Map.Entry<String, String> entry : dataMap.entrySet()) {
            byte[] key = entry.getKey().getBytes();
            buf.writeInt(key.length);
            buf.writeBytes(key);

            if (entry.getValue() == null) {
                buf.writeInt(-1);
            } else {
                byte[] value = entry.getValue().getBytes();
                buf.writeInt(value.length);
                buf.writeBytes(value);
            }
        }

        return buf;
    }

    public ByteBuf toBytes() {
        int size = dataMap.size();
        int bytesSize = 4;
        byte[][] keys = new byte[size][];
        byte[][] values = new byte[size][];

        int i = 0;
        for (Map.Entry<String, String> entry : dataMap.entrySet()) {
            byte[] key = entry.getKey().getBytes();

            byte[] value = entry.getValue() == null ? null : entry.getValue().getBytes();
            keys[i] = key;
            values[i] = value;
            i++;
            bytesSize += 8 + key.length + (value == null ? 0 : value.length);
        }

        byte[] bytes = new byte[bytesSize];
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        buf.writerIndex(0);
        buf.writeInt(size);

        for (i = 0; i < keys.length; i++) {
            byte[] key = keys[i];

            buf.writeInt(key.length);
            buf.writeBytes(key);


            byte[] value = values[i];
            if (value == null) {
                buf.writeInt(-1);
            } else {
                buf.writeInt(value.length);
                buf.writeBytes(value);
            }
        }

        return buf;
    }

    public static SocketReqMsg toSocketReqMsg(ByteBuf buf) {
        byte head = buf.getByte(buf.readerIndex());
        if ((head & 0x80) != 0) {
            return SocketDataMsg.toSocketReqMsg(buf);
        }

        int size = buf.readInt();
        SocketReqMsg msg = new SocketReqMsg();
        msg.dataMap = new HashMap<>(size);


        for (int i = 0; i < size; i++) {
            int keyLen = buf.readInt();
            byte[] key = new byte[keyLen];
            buf.readBytes(key);

            int valueLen = buf.readInt();
            if (valueLen == -1) {
                msg.dataMap.put(new String(key), null);
            } else {
                byte[] value = new byte[valueLen];
                buf.readBytes(value);
                msg.dataMap.put(new String(key), new String(value));
            }
        }

        return msg;
    }
}
