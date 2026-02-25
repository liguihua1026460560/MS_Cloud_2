package com.macrosan.filesystem.cifs.rpc;


public class RPCConstants {
    // dcerpc请求类型
    public static final byte REQUEST = 0x00;
    public static final byte PING = 0x01;
    public static final byte RESPONSE = 0x02;
    public static final byte FAULT = 0x03;
    public static final byte WORKING = 0x04;
    public static final byte NOCALL = 0x05;
    public static final byte REJECT = 0x06;
    public static final byte ACK = 0x07;
    public static final byte CL_CANCEL = 0x08;
    public static final byte FACK = 0x09;
    public static final byte CANCEL_ACK = 0x0a;
    public static final byte BIND = 0x0b;
    public static final byte BIND_ACK = 0x0c;
    public static final byte BIND_NAK = 0x0d;
    public static final byte ALTER_CONTEXT = 0x0e;
    public static final byte ALTER_CONTEXT_RESP = 0x0f;
    public static final byte AUTH = 0x10;
    public static final byte SHUTDOWN = 0x11;
    public static final byte CO_CANCEL = 0x12;
    public static final byte ORPHANED = 0x13;

    public static final String[] PACKET_TYPE_NAMES = {
            "REQUEST",
            "PING",
            "RESPONSE",
            "FAULT",
            "WORKING",
            "NOCALL",
            "REJECT",
            "ACK",
            "CL_CANCEL",
            "FACK",
            "CANCEL_ACK",
            "BIND",
            "BIND_ACK",
            "BIND_NAK",
            "ALTER_CONTEXT",
            "ALTER_CONTEXT_RESP",
            "AUTH",
            "SHUTDOWN",
            "CO_CANCEL",
            "ORPHANED"
    };


    // idl 接口协议UUID
    public static final String WITNESS_V1_1 = "ccd8c074-d0e5-4a40-92b4-d074faa6ba28";
    public static final String EPMAPPER_V3_0 = "e1af8308-5d1f-11c9-91a4-08002b14a0fa";

    // contextDefResult
    public static final short ACCEPTANCE = 0;
    public static final short USER_REJECTION = 1;
    public static final short PROVIDER_REJECTION = 2;
    public static final short NEGOTIATE_ACK = 3;


    // ProviderReason
    public static final short REASON_NOT_SPECIFIED = 0;
    public static final short ABSTRACT_SYNTAX_NOT_SUPPORTED = 0;
    public static final short PROPOSED_TRANSFER_SYNTAXES_NOT_SUPPORTED = 0;


    // Flags 定义
    public static int PFC_LAST_FRAG = 0x01;
    public static int PFC_PENDING_CANCEL = 0x04;
    public static int PFC_RESERVED_1 = 0x08;
    public static int PFC_CONC_MPX = 0x10;
    public static int PFC_DID_NOT_EXECUTE = 0x20;
    public static int PFC_MAYBE = 0x40;
    public static int PFC_OBJECT_UUID = 0x80;


    // 映射协议
    public static final byte EPM_PROTOCOL_DNET_NSP = 0x04;
    public static final byte EPM_PROTOCOL_OSI_TP4 = 0x05;
    public static final byte EPM_PROTOCOL_OSI_CLNS = 0x06;
    public static final byte EPM_PROTOCOL_TCP = 0x07;
    public static final byte EPM_PROTOCOL_UDP = 0x08;
    public static final byte EPM_PROTOCOL_IP = 0x09;
    public static final byte EPM_PROTOCOL_NCADG = 0x0a;       // Connectionless RPC
    public static final byte EPM_PROTOCOL_NCACN = 0x0b;
    public static final byte EPM_PROTOCOL_NCALRPC = 0x0c;     // Local RPC
    public static final byte EPM_PROTOCOL_UUID = 0x0d;
    public static final byte EPM_PROTOCOL_IPX = 0x0e;
    public static final byte EPM_PROTOCOL_SMB = 0x0f;
    public static final byte EPM_PROTOCOL_NAMED_PIPE = 0x10;
    public static final byte EPM_PROTOCOL_NETBIOS = 0x11;
    public static final byte EPM_PROTOCOL_NETBEUI = 0x12;
    public static final byte EPM_PROTOCOL_SPX = 0x13;
    public static final byte EPM_PROTOCOL_NB_IPX = 0x14;      // NetBIOS over IPX
    public static final byte EPM_PROTOCOL_DSP = 0x16;         // AppleTalk Data Stream Protocol
    public static final byte EPM_PROTOCOL_DDP = 0x17;         // AppleTalk Data Datagram Protocol
    public static final byte EPM_PROTOCOL_APPLETALK = 0x18;   // AppleTalk
    public static final byte EPM_PROTOCOL_VINES_SPP = 0x1a;
    public static final byte EPM_PROTOCOL_VINES_IPC = 0x1b;   // Inter Process Communication
    public static final byte EPM_PROTOCOL_STREETTALK = 0x1c;  // Vines Streettalk
    public static final byte EPM_PROTOCOL_HTTP = 0x1f;
    public static final byte EPM_PROTOCOL_UNIX_DS = 0x20;     // Unix domain socket
    public static final byte EPM_PROTOCOL_NULL = 0x21;


    // witness function
    public static final short WITNESS_GETINTERFACELIST = 0x00;
    public static final short WITNESS_REGISTER = 0x01;
    public static final short WITNESS_UNREGISTER = 0x02;
    public static final short WITNESS_ASYNCNOTIFY = 0x03;
    public static final short WITNESS_REGISTEREX = 0x04;
    public static final String[] WITNESS_OP_NAMES = {
            "WITNESS_GETINTERFACELIST",
            "WITNESS_REGISTER",
            "WITNESS_UNREGISTER",
            "WITNESS_ASYNCNOTIFY",
            "WITNESS_REGISTEREX"
    };

    // witness_notifyResponse_type
    public static final int WITNESS_NOTIFY_RESOURCE_CHANGE = 1;
    public static final int WITNESS_NOTIFY_CLIENT_MOVE = 2;
    public static final int WITNESS_NOTIFY_SHARE_MOVE = 3;
    public static final int WITNESS_NOTIFY_IP_CHANGE = 4;

    // witness_ResourceChange_type
    public static final int WITNESS_RESOURCE_STATE_UNKNOWN = 0x00;
    public static final int WITNESS_RESOURCE_STATE_AVAILABLE = 0x01;
    public static final int WITNESS_RESOURCE_STATE_UNAVAILABLE = 0xff;


    // witness interfacesInfo state
    public static final short WITNESS_STATE_UNKNOWN = 0x00;
    public static final short WITNESS_STATE_AVAILABLE = 0x01;
    public static final short WITNESS_STATE_UNAVAILABLE = 0xff;

    // witness version
    public static final int WITNESS_V1 = 0x00010001;
    public static final int WITNESS_V2 = 0x00020000;
    public static final int WITNESS_UNSPECIFIED_VERSION = 0xFFFFFFFF;

    public static final int WITNESS_INFO_IPv4_VALID = 0x01;
    public static final int WITNESS_INFO_IPv6_VALID = 0x02;
    public static final int WITNESS_INFO_WITNESS_IF = 0x04;
}
