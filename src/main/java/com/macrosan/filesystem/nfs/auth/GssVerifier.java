package com.macrosan.filesystem.nfs.auth;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

import static com.macrosan.constants.ServerConstants.DEFAULT_DATA;

@Log4j2
@Data
@ToString(exclude = "bytes")
public class GssVerifier implements ReadStruct {
    public int flavor;
    public int len;
    public byte[] bytes;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        flavor = buf.getInt(offset);
        len = buf.getInt(offset + 4);
        if (len > 0) {
            bytes = new byte[len];
            buf.getBytes(offset + 8, bytes);
        } else {
            bytes = DEFAULT_DATA;
        }

        return 8 + bytes.length;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, flavor);
        buf.setInt(offset + 4, len);

        if (len > 0) {
            buf.setBytes(offset + 8, bytes);
        }

        return 8 + len;
    }

    public static final GssVerifier NULL_VERIFIER = new GssVerifier() {
        @Override
        public int readStruct(ByteBuf buf, int offset) {
            bytes = DEFAULT_DATA;
            return 8;
        }

        public int writeStruct(ByteBuf buf, int offset) {
            buf.setLong(offset, 0L);
            return 8;
        }
    };

//
//    //gss_verify_mic_v2
//    public static class Krb5V2Verifier extends Verifier {
//        char id;
//
//        @Override
//        public int readStruct(ByteBuf buf, int offset) {
//            offset += super.readStruct(buf, offset);
//            id = buf.getChar(offset);
//            //id == KG2_TOK_MIC 0x0404
//            if (id != 0x0404) {
//                return -1;
//            }
//
//            return 0;
//        }
//    }
}
