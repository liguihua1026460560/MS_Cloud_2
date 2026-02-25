package com.macrosan.filesystem.cifs.rpc.pdu;

import com.macrosan.filesystem.cifs.rpc.RPCConstants;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.*;

@Log4j2
@Data
@NoArgsConstructor
public class ResultList {
    public byte numResults;
    public byte reserved;
    public short reserved2;
    public Result[] results;

    public ResultList(ContextList contextList) {
        numResults = contextList.numContextItem;
        reserved = contextList.reserved;
        reserved2 = contextList.reserved2;

        results = new Result[contextList.contextItems.length];

    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setByte(start, numResults);
        buf.setByte(start + 1, reserved);
        buf.setShortLE(start + 2, reserved2);

        start = start + 4;
        for (int i = 0; i < results.length; i++) {
            buf.setShortLE(start, results[i].result);
            buf.setShortLE(start + 2, results[i].reason);
            buf.setBytes(start + 4, results[i].syntax.interfaceUuid);
            buf.setIntLE(start + 20, results[i].syntax.interfaceVer);

            start = start + 24;
        }

        return start - offset;
    }

    @Data
    public static class Result {
        public short result;
        public short reason;
        public ContextList.Syntax syntax = new ContextList.Syntax();
    }
}
