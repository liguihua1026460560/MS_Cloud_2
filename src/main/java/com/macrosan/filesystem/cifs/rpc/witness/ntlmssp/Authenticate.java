package com.macrosan.filesystem.cifs.rpc.witness.ntlmssp;

public class Authenticate {
    public byte[] signature = new byte[8];
    public int messageType;  // 0x03

    public Fields lmChallengeResponse;
    public Fields ntlmChallengeResponse;
    public Fields domainName;
    public Fields userName;
    public Fields workstation;
    public Fields encryptedRandomSessionKey;

    public int negotiateFlags;

    public long version;
    public byte[] mic = new byte[16];

    public byte[] lmChallengeResponsePayload;
    public byte[] ntlmChallengeResponsePayload;
    public byte[] domainNamePayload;
    public byte[] userNamePayload;
    public byte[] workstationPayload;
    public byte[] encryptedRandomSessionKeyPayload;

}
