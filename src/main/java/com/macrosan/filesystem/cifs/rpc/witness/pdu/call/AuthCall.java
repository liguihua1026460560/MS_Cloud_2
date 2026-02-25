package com.macrosan.filesystem.cifs.rpc.witness.pdu.call;

import com.macrosan.filesystem.cifs.rpc.pdu.RPCHeader;
import com.macrosan.filesystem.cifs.rpc.witness.ntlmssp.Authenticate;
import com.macrosan.filesystem.cifs.rpc.witness.ntlmssp.Fields;
import io.netty.buffer.ByteBuf;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class AuthCall extends RPCHeader {
    public int pad;
    public long authID;
    public Authenticate authenticate;

    public int readStruct(ByteBuf buf, int offset) {
        int start = offset + super.readStruct(buf, offset);
        pad = buf.getIntLE(start);
        authID = buf.getLongLE(start + 4);

        start += 12;

        authenticate = new Authenticate();
        buf.getBytes(start, authenticate.signature);
        authenticate.messageType = buf.getIntLE(start + 8);

        authenticate.lmChallengeResponse = new Fields();
        authenticate.lmChallengeResponse.readStruct(buf, start + 12);
        authenticate.ntlmChallengeResponse = new Fields();
        authenticate.ntlmChallengeResponse.readStruct(buf, start + 20);
        authenticate.domainName = new Fields();
        authenticate.domainName.readStruct(buf, start + 28);
        authenticate.userName = new Fields();
        authenticate.userName.readStruct(buf, start + 36);
        authenticate.workstation = new Fields();
        authenticate.workstation.readStruct(buf, start + 44);
        authenticate.encryptedRandomSessionKey = new Fields();
        authenticate.encryptedRandomSessionKey.readStruct(buf, start + 52);

        authenticate.negotiateFlags = buf.getIntLE(start + 60);
        authenticate.version = buf.getLongLE(start + 64);
        buf.getBytes(start + 72, authenticate.mic);


        int len = authenticate.lmChallengeResponse.len;
        authenticate.lmChallengeResponsePayload = new byte[len];
        buf.getBytes(start + authenticate.lmChallengeResponse.bufferOffset, authenticate.lmChallengeResponsePayload);

        len = authenticate.ntlmChallengeResponse.len;
        authenticate.ntlmChallengeResponsePayload = new byte[len];
        buf.getBytes(start + authenticate.ntlmChallengeResponse.bufferOffset, authenticate.ntlmChallengeResponsePayload);

        len = authenticate.domainName.len;
        authenticate.domainNamePayload = new byte[len];
        buf.getBytes(start + authenticate.domainName.bufferOffset, authenticate.domainNamePayload);

        len = authenticate.userName.len;
        authenticate.userNamePayload = new byte[len];
        buf.getBytes(start + authenticate.userName.bufferOffset, authenticate.userNamePayload);

        len = authenticate.workstation.len;
        authenticate.workstationPayload = new byte[len];
        buf.getBytes(start + authenticate.workstation.bufferOffset, authenticate.workstationPayload);

        len = authenticate.encryptedRandomSessionKey.len;
        authenticate.encryptedRandomSessionKeyPayload = new byte[len];
        buf.getBytes(start + authenticate.encryptedRandomSessionKey.bufferOffset, authenticate.encryptedRandomSessionKeyPayload);
        return fragLength;
    }
}
