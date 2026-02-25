package com.macrosan.filesystem.cifs.api.smb1;

import com.macrosan.filesystem.cifs.*;
import com.macrosan.filesystem.cifs.SMB1.SMB1Reply;
import com.macrosan.filesystem.cifs.call.smb1.NegprotCall;
import com.macrosan.filesystem.cifs.reply.smb1.BodyErrorReply;
import com.macrosan.filesystem.cifs.reply.smb1.NegprotReply;
import com.macrosan.filesystem.cifs.types.Session;
import lombok.AllArgsConstructor;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static com.macrosan.filesystem.cifs.SMB1.SMB1_OPCODE.SMB_NEGPROT;
import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.SMB2_NEGPROT;
import static com.macrosan.filesystem.cifs.SMB2Header.EMPTY_SIGN;
import static com.macrosan.filesystem.cifs.reply.smb1.NegprotReply.CAP_EXTENDED_SECURITY;

public class Negprot {
    private static final Negprot instance = new Negprot();

    private Negprot() {
    }

    public static Negprot getInstance() {
        return instance;
    }

    @AllArgsConstructor
    private static class NegprotInfo {
        final int priority;
        final BiFunction<Integer, Short, NegprotReply> replyFunction;
    }

    private static final NegprotInfo NTLMSSP_INFO = new NegprotInfo(5, NegprotReply::ntlmsspNegprotReply);

    private static final Map<String, NegprotInfo> supportDialectMap = new HashMap<String, NegprotInfo>() {
        {
            this.put("NT LM 0.12", NTLMSSP_INFO);
        }
    };

    /**
     * 初始化，需要从客户端发送的协议中选择一个支持的。
     * 可能出现的协议 https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/80850595-e301-4464-9745-58e4945eb99b
     * 当前仅支持 NT LM 0.12
     */
    @SMB1.Smb1Opt(value = SMB_NEGPROT, allowNoSession = true)
    public Mono<SMBHeader.SMBReply> negprot(SMB1Header header, Session session, NegprotCall call) {
        int choice = -1;

        SMB1Reply reply = new SMB1Reply(header);

        for (int i = 0; i < call.dialects.length; i++) {
            String dialect = call.dialects[i];
            //NTLMSSP协议
            if ("NT LM 0.12".equals(dialect) && (header.flags2 & CAP_EXTENDED_SECURITY) != 0) {
                choice = i;
            }
            //特殊处理可能需要返回SMB2
            if ("SMB 2.002".equals(dialect) || "SMB 2.???".equals(dialect)) {
                SMBHeader.SMBReply smb2Reply = new SMBHeader.SMBReply();
                SMB2Header smb2Header = new SMB2Header()
                        .setMagic(SMB2Header.MAGIC)
                        .setHeaderLen((short) SMB2Header.SIZE)
                        .setCreditCharge((short) 0)
                        .setOpcode(SMB2_NEGPROT.opcode)
                        .setCreditRequested((short) 1)
                        .setFlags(1)
                        .setNextCmd(0)
                        .setMessageId(0)
                        .setPid(0)
                        .setTid(0)
                        .setSessionId(0)
                        .setSign(EMPTY_SIGN);
                smb2Reply.setHeader(smb2Header);
                smb2Reply.setBody(com.macrosan.filesystem.cifs.reply.smb2.NegprotReply.smb2XX());
                return Mono.just(smb2Reply);
            }
        }

        if (choice == -1) {
            reply.setBody(BodyErrorReply.errorReply(-1));
            return Mono.just(reply);
        }

        SMB1Body replyBody = supportDialectMap.get(call.dialects[choice])
                .replyFunction.apply(choice, header.flags2);
        reply.setBody(replyBody);
        return Mono.just(reply);
    }
}
