package com.macrosan.filesystem.cifs.rpc.witness.ntlmssp;

public class NTLM {
    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/99d90ff4-957f-4c8a-80e4-5bfe5a9a9832
    public static final int NTLMSSP_NEGOTIATE_UNICODE = 0x00000001;
    public static final int NTLMSSP_NEGOTIATE_OEM = 0x00000002;
    public static final int NTLMSSP_REQUEST_TARGET = 0x00000004;
    public static final int NTLMSSP_NEGOTIATE_SIGN = 0x00000010;
    public static final int NTLMSSP_NEGOTIATE_SEAL = 0x00000020;
    public static final int NTLMSSP_NEGOTIATE_DATAGRAM = 0x00000040;
    public static final int NTLMSSP_NEGOTIATE_LM_KEY = 0x00000080;
    public static final int NTLMSSP_NEGOTIATE_NETWARE = 0x00000100;
    public static final int NTLMSSP_NEGOTIATE_NTLM = 0x00000200;
    public static final int NTLMSSP_NEGOTIATE_NT_ONLY = 0x00000400;
    public static final int NTLMSSP_ANONYMOUS = 0x00000800;
    public static final int NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED = 0x00001000;
    public static final int NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED = 0x00002000;
    public static final int NTLMSSP_NEGOTIATE_THIS_IS_LOCAL_CALL = 0x00004000;
    public static final int NTLMSSP_NEGOTIATE_ALWAYS_SIGN = 0x00008000;
    public static final int NTLMSSP_TARGET_TYPE_DOMAIN = 0x00010000;
    public static final int NTLMSSP_TARGET_TYPE_SERVER = 0x00020000;
    public static final int NTLMSSP_TARGET_TYPE_SHARE = 0x00040000;
    public static final int NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY = 0x00080000;
    public static final int NTLMSSP_NEGOTIATE_IDENTIFY = 0x00100000;
    public static final int NTLMSSP_REQUEST_NON_NT_SESSION_KEY = 0x00400000;
    public static final int NTLMSSP_NEGOTIATE_TARGET_INFO = 0x00800000;
    public static final int NTLMSSP_NEGOTIATE_VERSION = 0x02000000;
    public static final int NTLMSSP_NEGOTIATE_128 = 0x20000000;
    public static final int NTLMSSP_NEGOTIATE_KEY_EXCH = 0x40000000;
    public static final int NTLMSSP_NEGOTIATE_56 = 0x80000000;
}
