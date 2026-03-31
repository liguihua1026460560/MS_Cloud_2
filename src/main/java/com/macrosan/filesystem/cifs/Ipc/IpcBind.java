package com.macrosan.filesystem.cifs.Ipc;

import com.macrosan.filesystem.cifs.types.smb2.SID;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.log4j.Log4j2;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.filesystem.FsConstants.SMB2ACCESS_MASK.SET_FILE_ALL_RIGHTS;

@Log4j2
public class IpcBind {

    private static final AtomicLong handleCounter = new AtomicLong(1);

    public static byte[] createDsRoleResponsePDU(int callId, short contextId, int infoLevel) {
        ByteBuf buf = Unpooled.buffer(104);
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            buf.writeShortLE(104);                   // frag length
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            buf.writeIntLE(80);                      // alloc hint = 80
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            // Stub Data (必须正好 80 字节)
            if (infoLevel == 1 || infoLevel == 0) {
                // Stub Data
                buf.writeIntLE(0x00020000);                       // MachineRole = DS_ROLE_STANDALONE_SERVER (2)
                buf.writeIntLE(0x00000001);                       // Flags = 0
                buf.writeIntLE(0x00000002);              // Pointer to Info (第一个 referent)
                buf.writeShortLE(0x0000);                // Unknown short (常见值)
                buf.writeShortLE(0x0000);                // Unknown short
                buf.writeIntLE(0x00020004);              // Referent ID for Domain Name (关键！目标是 0x00020004)
                buf.writeIntLE(0);                     // Pointer to Dns Domain (NULL)
                buf.writeIntLE(0);                     // Pointer to Forest (NULL)
                buf.writeZero(16);                       // Domain GUID all zero

                // UNICODE_STRING for Domain Name "M0SS-162"
                String domain = "MOSS-162";
                byte[] domainBytes = domain.getBytes(StandardCharsets.UTF_16LE);  // 18 字节

                buf.writeIntLE(9);               // MaximumCount = 9
                buf.writeIntLE(0);                       // Offset = 0
                buf.writeIntLE(9);               // ActualCount = 9
                buf.writeBytes(domainBytes);             // "M0SS-162" UTF-16LE (18 字节)
                buf.writeZero(2);                        // Alignment padding to 4-byte boundary
                buf.writeIntLE(0);                       // Windows Error = WERR_OK (0x00000000)

                // 精确填充到正好 80 字节 stub
                int currentStubBytes = buf.writerIndex() - 24;
                int remaining = 80 - currentStubBytes;
                if (remaining > 0) {
                    buf.writeZero(remaining);
                } else if (remaining < 0) {
                    log.warn("DsRole stub overflow by {} bytes", -remaining);
                }
            } else {
                buf.writeZero(80);
            }

            // 最终验证长度必须是 104
            if (buf.writerIndex() != 104) {
                log.error("DsRole Response PDU length mismatch: expected 104, actual {}", buf.writerIndex());
                // 强制补齐
                if (buf.writerIndex() < 104) {
                    buf.writeZero(104 - buf.writerIndex());
                }
            }

            byte[] response = new byte[104];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }

    public static byte[] createWkstaGetInfoResponse(int callId, short contextId, int level) {

        // Level 100 stub 固定 152 字节，总 PDU 176 字节
        int stubLen = 152;
        int totalLen = 24 + stubLen;

        ByteBuf buf = Unpooled.buffer(totalLen);
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);
            buf.writeByte(0);
            buf.writeByte(2);                    // Response
            buf.writeByte(0x03);
            buf.writeIntLE(0x10);
            buf.writeShortLE(totalLen);          // frag length = 176
            buf.writeShortLE(0);
            buf.writeIntLE(callId);
            buf.writeIntLE(stubLen);             // alloc hint = 152
            buf.writeShortLE(contextId);
            buf.writeShortLE(0);

            // Stub Data (152 字节，Level 100)
            if (level == 100 || level == 0) {
                buf.writeIntLE(100);
                buf.writeIntLE(0);
                buf.writeLongLE(0x0000000000020000);           // Pointer to Computer Name (Referent ID)
                buf.writeIntLE(500);                        // Platform ID: Unknown (500)
                buf.writeIntLE(0);
                buf.writeLongLE(0x0000000000020000);
                buf.writeLongLE(0x0000000000020000);

                buf.writeIntLE(10);                  // Major Version
                buf.writeIntLE(0);                   // Minor Version

                String computerName = "DESKTOP-OVA1GDI";
                byte[] compBytes = computerName.getBytes(StandardCharsets.UTF_16LE);

                buf.writeIntLE(16);           // MaximumCount
                buf.writeIntLE(0);                   // Offset
                buf.writeLongLE(0);
                buf.writeIntLE(16);           // ActualCount
                buf.writeIntLE(0);
                buf.writeBytes(compBytes);
                buf.writeZero(4 - (compBytes.length % 4));


                String lanGroup = "WORKGROUP";
                byte[] groupBytes = lanGroup.getBytes(StandardCharsets.UTF_16LE);

                buf.writeIntLE(10);
                buf.writeIntLE(0);
                buf.writeLongLE(0);
                buf.writeLongLE(10);
                buf.writeBytes(groupBytes);
                buf.writeZero(4 - (groupBytes.length % 4));

                int stubWritten = buf.writerIndex() - 24;
                int remaining = stubLen - stubWritten;
                if (remaining > 0) {
                    buf.writeZero(remaining);
                } else if (remaining < 0) {
                    log.warn("Wksta Level 100 stub overflow by {} bytes", -remaining);
                }
            } else {
                buf.writeZero(stubLen);
            }

            byte[] response = new byte[totalLen];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }

    private static String getLocalComputerName() {
        try {
            return InetAddress.getLocalHost().getHostName().toUpperCase();  // Windows 通常大写
        } catch (UnknownHostException e) {
            // 备选方案
            String envName = System.getenv("COMPUTERNAME");
            if (envName != null && !envName.isEmpty()) {
                return envName.toUpperCase();
            }
            // 最终 fallback
            return "UNKNOWN-HOST";
        }
    }

    public static byte[] createLsaQueryInfoPolicyResponse(int callId, short contextId, int level) {

        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            // Stub Data (152 字节，Level 100)
            if (level == 5) {
                buf.writeIntLE(0x00020000);                       // Pointer to PolicyInfo (Referent ID)
                buf.writeIntLE(0x00000005);                       // Info

                // UNICODE_STRING for Domain Name "M0SS-162"
                String domain = "MOSS-162";
                byte[] domainBytes = domain.getBytes(StandardCharsets.UTF_16LE);  // 18 字节
                buf.writeShortLE(domainBytes.length);                  // Domain Name Length
                buf.writeShortLE(18);                               // Domain Name Size
                buf.writeIntLE(0x00020004);              // Referent ID for Domain Name
                buf.writeIntLE(0x00020008);
                buf.writeIntLE(9);                      // Pointer to Domain Name Max count
                buf.writeIntLE(0);
                buf.writeIntLE(8);
                buf.writeBytes(domainBytes);             // "M0SS-162"
                buf.writeIntLE(1);                      // Pointer to Sid count

                SID sid = SID.generateSID("S-1-5-1");
                //sid.writeStruct(buf, stubStart + 8);
                buf.writeByte(sid.getRevision());
                buf.writeByte(sid.getNumSubAuth());
                buf.writeZero(2);
                buf.writeIntLE(0x05000000);
                if (sid.getSubAuthority().getSubAutList() != null){
                    for (Long subAuth : sid.getSubAuthority().getSubAutList()){
                        buf.writeIntLE(subAuth.intValue());
                    }
                }
            }

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] lsaLookupSids2Response(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer(248);
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);
            buf.writeByte(0);
            buf.writeByte(2);
            buf.writeByte(0x03);
            buf.writeIntLE(0x10);
            buf.writeShortLE(272);
            buf.writeShortLE(0);
            buf.writeIntLE(callId);
            buf.writeIntLE(248);
            buf.writeShortLE(contextId);
            buf.writeShortLE(0);


            // Pointers
            buf.writeIntLE(0x0002000c);          // Pointer to Domains Referent ID(回填)
            buf.writeIntLE(0x00000002);          // Count
            buf.writeIntLE(0x00020010);          //pointer to Domains Referent ID(回填)
            buf.writeIntLE(0x00000020);          //Max Size

            // ReferencedDomainList
            buf.writeIntLE(2);

            String domain = "MOSS-162";
            byte[] domainBytes = domain.getBytes(StandardCharsets.UTF_16LE);
            buf.writeShortLE(domainBytes.length);  // Length
            buf.writeShortLE(domainBytes.length + 2);          //size
            buf.writeIntLE(0x00020014);    //MOSS-162 Referent ID(回填)
            buf.writeIntLE(0x00020004);    //S-1-5-0 Referent ID(回填)


            String name1 = "Unix Group";
            byte[] name1Bytes = name1.getBytes(StandardCharsets.UTF_16LE);
            buf.writeShortLE(name1Bytes.length);   //Length
            buf.writeShortLE(name1Bytes.length + 2);    //size
            buf.writeIntLE(0x0002001c);         //Unix Group Referent ID(回填)
            buf.writeIntLE(0x00020008);         //S-1-6-0 Referent ID(回填)


            buf.writeIntLE(9);
            buf.writeIntLE(0);
            buf.writeIntLE(8);
            buf.writeBytes(domainBytes);

            //S-1-5-0
            buf.writeIntLE(1);
            buf.writeByte(1);
            buf.writeByte(1);
            buf.writeShortLE(0);
            buf.writeIntLE(0x05000000);
            buf.writeIntLE(0);

            buf.writeIntLE(11);  //Domains Unix Group max Count
            buf.writeIntLE(0);   //Domains Unix Group Offset
            buf.writeIntLE(10);  //Domains Unix Group Actual Count
            buf.writeBytes(name1Bytes);   // "Unix Group"

            //S-1-6-0
            buf.writeIntLE(1);
            buf.writeByte(1);
            buf.writeByte(1);
            buf.writeShortLE(0);
            buf.writeIntLE(0x06000000);
            buf.writeIntLE(0);

            buf.writeIntLE(2);      //Pointers to Names Count
            buf.writeIntLE(0x00020024);     //Pointers to Names Referent ID(回填)
            buf.writeIntLE(2);         //Max Count

            //Pointers to String  Sid Index == 0
            String user1 = "zzb1";
            byte[] user1Bytes = user1.getBytes(StandardCharsets.UTF_16LE);
            buf.writeShortLE(1);     //Sid Type(Sid_NAME_USER)
            buf.writeShortLE(0);
            buf.writeShortLE(8);     //Pointer to Names Length
            buf.writeShortLE(8);     //Pointer to Names Size
            buf.writeIntLE(0x00020028);        // Pointers to String Referent ID(回填)
            buf.writeIntLE(0);                 // Pointer to String Sid Index
            buf.writeIntLE(0);                 // Pointer to String Unknown

            //Pointers to String  Sid Index == 1
            String user2 = "zzb1";
            byte[] user2Bytes = name1.getBytes(StandardCharsets.UTF_16LE);
            buf.writeShortLE(2);     //Sid Type(SID_NAME_DOM_GRP)
            buf.writeShortLE(0);
            buf.writeShortLE(8);     //Pointer to Names Length
            buf.writeShortLE(8);     //Pointer to Names Size
            buf.writeIntLE(0x0002002c);        // Pointers to String Referent ID(回填)
            buf.writeIntLE(1);                 // Pointer to String Sid Index
            buf.writeIntLE(0);                 // Pointer to String Unknown

            //Pointers to String(index == 0)
            buf.writeIntLE(4);   //Max Count
            buf.writeIntLE(0);   //Offset
            buf.writeIntLE(4);   //Actual Count
            buf.writeBytes(user1Bytes);

            //Pointers to String(index == 1)
            buf.writeIntLE(4);   //Max Count
            buf.writeIntLE(0);   //Offset
            buf.writeIntLE(4);   //Actual Count
            buf.writeBytes(user2Bytes);

            //Pointers to Count
            buf.writeIntLE(2);   //Count


            buf.writeIntLE(0);   //Nt Error:STATUS_SUCCESS

            byte[] response = new byte[buf.writerIndex()];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] connect5ResponsePDU(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            buf.writeShortLE(64);                   // frag length
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            buf.writeIntLE(40);                      // alloc hint
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            buf.writeIntLE(1);    //Level Out
            buf.writeIntLE(1);    //Info Out
            buf.writeIntLE(3);    //Client Version
            buf.writeIntLE(0);
            byte[] handle = generatePolicyHandle();
            buf.writeBytes(handle);  // 20 字节 ConnectHandle
            buf.writeZero(4);

            byte[] response = new byte[64];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] lsaLookupNames3ResponsePDU(int callId, short contextId,int isGroupOrUser,int[] uidAndGid) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            buf.writeIntLE(0x00020004);   //Pointer to Domains Referent ID
            buf.writeIntLE(1);      //Domains Count
            buf.writeIntLE(0x00020008);   //Pointer to Domains Referent ID
            buf.writeIntLE(32);           //Max size
            buf.writeIntLE(1);         //Max count

            buf.writeShortLE(16);     //Name len
            buf.writeShortLE(18);     //Name size

            buf.writeIntLE(0x0002000c);
            buf.writeIntLE(0x00020010);
            buf.writeIntLE(9);
            buf.writeIntLE(0);
            buf.writeIntLE(8);
            buf.writeBytes("MOSS-162".getBytes(StandardCharsets.UTF_16LE));

            buf.writeIntLE(1);
            buf.writeByte(1);
            buf.writeByte(1);
            buf.writeShortLE(0);

            if (isGroupOrUser == 0){
                buf.writeIntLE(0x06000000);
            } else if (isGroupOrUser == 1) {
                buf.writeIntLE(0x05000000);
            }
            buf.writeIntLE(0);

            buf.writeIntLE(1);
            buf.writeIntLE(0x00020014);
            buf.writeIntLE(1);

            if (isGroupOrUser == 0){
                buf.writeShortLE(2);    //Sid Type
            } else if (isGroupOrUser == 1) {
                buf.writeShortLE(1);    //Sid Type
            }
            buf.writeShortLE(0);

            buf.writeIntLE(0x00020018);    //Pointer to Sid Referent ID
            buf.writeIntLE(0);             //Sid Index
            buf.writeIntLE(0);             //Flags
            buf.writeIntLE(1);             //Count
            buf.writeByte(1);   //revision
            buf.writeByte(1);   //Num auth
            buf.writeShortLE(0);
            if (isGroupOrUser == 0){
                buf.writeIntLE(0x06000000);
                buf.writeIntLE(uidAndGid[1]);
            } else if (isGroupOrUser == 1) {
                buf.writeIntLE(0x05000000);
                buf.writeIntLE(uidAndGid[0]);
            }

            buf.writeIntLE(1);

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] lookupDomainResponsePDU(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            buf.writeIntLE(0x00020004);    //Pointer to Sid Referent ID
            buf.writeIntLE(1);    //Pointer to Sid Count

            SID sid = SID.generateSID("S-1-5-0");
            //sid.writeStruct(buf, stubStart + 8);
            buf.writeByte(sid.getRevision());
            buf.writeByte(sid.getNumSubAuth());
            buf.writeZero(2);
            buf.writeIntLE(0x05000000);
            if (sid.getSubAuthority().getSubAutList() != null){
                for (Long subAuth : sid.getSubAuthority().getSubAutList()){
                    buf.writeIntLE(subAuth.intValue());
                }
            }

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] enumDomainsResponsePDU(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                   // frag length
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                      // alloc hint
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            buf.writeIntLE(0);    //Resume Handle
            buf.writeIntLE(0x00020000);    //Pointer to Sam Referent ID
            buf.writeIntLE(1);    //Sam Count
            buf.writeIntLE(0x00020004);    //Pointer to Entries Referent ID
            buf.writeIntLE(1);    //Pointer to Entries Max Count

            buf.writeIntLE(0);    //Pointer to Entries Idx(0)
            buf.writeShortLE(16); //Pointer to Entries Name Length
            buf.writeShortLE(16); //Pointer to Entries Name Size
            buf.writeIntLE(0x00020008);  // Pointer to Entries Name Referent ID

            /*buf.writeIntLE(1);    //Pointer to Entries Idx(1)
            buf.writeShortLE(14); //Pointer to Entries Name Length
            buf.writeShortLE(14); //Pointer to Entries Name Size
            buf.writeIntLE(0x0002000c);  // Pointer to Entries Name Referent ID*/

            buf.writeIntLE(8);    //Name(0) Max Count
            buf.writeIntLE(0);    //Name Offset
            buf.writeIntLE(8);    //Name Actual Count
            buf.writeBytes("MOSS-162".getBytes(StandardCharsets.UTF_16LE));

            /*buf.writeIntLE(7);    //Name(1) Max Count
            buf.writeIntLE(0);    //Name Offset
            buf.writeIntLE(7);    //Name Actual Count
            buf.writeBytes("Builtin".getBytes(StandardCharsets.UTF_16LE));
            buf.writeZero(2);*/

            buf.writeIntLE(1);    //Num Entries
            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint

            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] openDomainResponsePDU(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            byte[] handle = generatePolicyHandle();
            buf.writeBytes(handle);  // 20 字节 ConnectHandle

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }

    public static byte[] lsaCloseResponsePDU(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            byte[] handle = new byte[20];

            buf.writeBytes(handle);  // 20 字节 ConnectHandle

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] lookupNamesResponsePDU(int callId, short contextId,int rid) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            buf.writeIntLE(1);  //Count
            buf.writeIntLE(0x00020004);   //Pointer to RIds Referent ID
            buf.writeIntLE(1);            //Ids Max Count
            buf.writeIntLE(rid);         //RID

            buf.writeIntLE(1);            //Pointer to Types Count
            buf.writeIntLE(0x00020008);   //Pointer to Ids Referent ID
            buf.writeIntLE(1);            //Ids Max Count
            buf.writeIntLE(1);

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] openUserResponsePDU(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            byte[] handle = generatePolicyHandle();
            buf.writeBytes(handle);  // 20 字节 ConnectHandle

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] closeResponsePDU(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            byte[] handle = generatePolicyHandle();
            buf.writeBytes(handle);  // 20 字节 ConnectHandle

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] querySecurityResponsePDU(int callId, short contextId,int[] uidAndGid,int isGroupOrUser) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            buf.writeIntLE(0x00020000);    //Pointer to Sdduf Referent ID
            buf.writeIntLE(120);           //Sec Desc Buf Len (回填)
            buf.writeIntLE(0x00020004);    //SAM SECURITY_DESCRIPTOR data Referent ID
            buf.writeIntLE(120);           //Sec Desc Buf Len (回填)

            buf.writeShortLE(1);           //NT Security Descriptor Revision
            buf.writeShortLE(0x8004);      //NT Security Descriptor Type
            buf.writeIntLE(0);             //Offset to Owner SID
            buf.writeIntLE(0);             //Offset to Group SID
            buf.writeIntLE(0);             //Offset to Sacl
            buf.writeIntLE(20);            //Offset to Dacl

            buf.writeShortLE(2);           //NT User ACL Revision
            buf.writeShortLE(100);         //NT User ACL Size
            buf.writeIntLE(4);             //NT User ACL Num ACEs

            //S-1-1-0
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeShortLE(20);
            buf.writeIntLE(0x0000035b);
            buf.writeByte(1);
            buf.writeByte(1);
            buf.writeShortLE(0);
            buf.writeIntLE(0x01000000);
            buf.writeIntLE(0);

            //S-1-5-32-544
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeShortLE(24);
            buf.writeIntLE(0x000f07ff);
            buf.writeByte(1);
            buf.writeByte(2);
            buf.writeShortLE(0);
            buf.writeIntLE(0x05000000);
            buf.writeIntLE(32);
            buf.writeIntLE(544);

            //S-1-5-32-548
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeShortLE(24);
            buf.writeIntLE(0x000f07ff);
            buf.writeByte(1);
            buf.writeByte(2);
            buf.writeShortLE(0);
            buf.writeIntLE(0x05000000);
            buf.writeIntLE(32);
            buf.writeIntLE(548);

            //自定义主体
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeShortLE(24);
            buf.writeIntLE((int) SET_FILE_ALL_RIGHTS);
            buf.writeByte(1);   //revision
            buf.writeByte(1);   //Num auth
            buf.writeShortLE(0);
            if (isGroupOrUser == 0){
                buf.writeIntLE(0x06000000);
                buf.writeIntLE(uidAndGid[1]);
            } else if (isGroupOrUser == 1) {
                buf.writeIntLE(0x05000000);
                buf.writeIntLE(uidAndGid[0]);
            }
            buf.writeIntLE(1007);

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] queryUserInfoResponsePDU(int callId, short contextId,String targetName, int[] uidAndGid) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            buf.writeIntLE(0x00020000);   //Pointer to UserInfo Referent ID
            buf.writeShortLE(21);   //samr_UserInfo Info
            buf.writeZero(2);

            // Last Logon: 0
            buf.writeLongLE(0L);
            // Last Logoff: Feb 6, 2036 23:06:39 CST
            buf.writeLongLE(137303967990000000L);
            // Last Password Change
            buf.writeLongLE(134066937700000000L);
            // Acct Expiry
            buf.writeLongLE(137303967990000000L);
            // Allow Password Change
            buf.writeLongLE(134066937700000000L);
            // Force Password Change
            buf.writeLongLE(0x7FFFFFFFFFFFFFFFL);

            String homeDirectory = "\\\\moss-162\\" + targetName;
            String profilePath = "\\\\moss-162\\"+ targetName +"\\profile";

            buf.writeShortLE(targetName.length() * 2);   //Account Name len
            buf.writeShortLE(targetName.length() * 2);   //Account Name size
            buf.writeIntLE(0x00020004);  //Account Name Referent ID
            buf.writeShortLE(0);   //Full Name len
            buf.writeShortLE(0);   //Full Name size
            buf.writeIntLE(0x00020008);  //Full Name Referent ID
            buf.writeShortLE(homeDirectory.length() * 2);   //Home Directory Name len
            buf.writeShortLE(homeDirectory.length() * 2);   //Home Directory Name size
            buf.writeIntLE(0x0002000c);  //Home Directory Referent ID
            buf.writeShortLE(0);   //Home Drive Name len
            buf.writeShortLE(0);   //Home Drive Name size
            buf.writeIntLE(0x00020010);  //Home Drive Referent ID
            buf.writeShortLE(0);   //Logon Script Name len
            buf.writeShortLE(0);   //Logon Script Name size
            buf.writeIntLE(0x00020014);  //Home Drive Referent ID
            buf.writeShortLE(profilePath.length() * 2);   //Profile Path Name len
            buf.writeShortLE(profilePath.length() * 2);   //Profile Path Name size
            buf.writeIntLE(0x00020018);  //Profile Path Referent ID
            buf.writeShortLE(0);   //Description Name len
            buf.writeShortLE(0);   //Description Name size
            buf.writeIntLE(0x0002001c);  //Description Referent ID
            buf.writeShortLE(0);   //Workstations Name len
            buf.writeShortLE(0);   //Workstations Name size
            buf.writeIntLE(0x00020020);  //Workstations Referent ID
            buf.writeShortLE(0);   //Comment Name len
            buf.writeShortLE(0);   //Comment Name size
            buf.writeIntLE(0x00020024);  //Comment Referent ID
            buf.writeShortLE(0);   //Parameters len
            buf.writeShortLE(0);   //Parameters size
            buf.writeIntLE(0x00020028);  //Parameters Referent ID
            buf.writeShortLE(0);   //Lm Owf Password len
            buf.writeShortLE(0);   //Lm Owf Password size
            buf.writeIntLE(0);  //Lm Owf Password Referent ID
            buf.writeShortLE(0);   //Nt Owf Password len
            buf.writeShortLE(0);   //Nt Owf Password size
            buf.writeIntLE(0);  //Nt Owf Password Referent ID
            buf.writeShortLE(0);   //Private Data Password len
            buf.writeShortLE(0);   //Private Data Password size
            buf.writeIntLE(0);
            buf.writeIntLE(0);   //Buf Count
            buf.writeIntLE(0);
            buf.writeIntLE(1007);  //RID
            buf.writeIntLE(uidAndGid[1]);   //GID
            buf.writeIntLE(0x00000010);  //Acct Flags
            buf.writeIntLE(0x00ffffff);  //Fields Present
            buf.writeShortLE(168);   //Logon Hours Units Per Week
            buf.writeZero(2);
            buf.writeIntLE(0x0002002c);  //Logon Hours Referent ID
            buf.writeShortLE(0);
            buf.writeShortLE(0);
            buf.writeShortLE(0);
            buf.writeShortLE(0);
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeByte(0);
            buf.writeIntLE(targetName.length());
            buf.writeIntLE(0);
            buf.writeIntLE(targetName.length());   //Account Name Actual Count
            writeUnicodeStringData(buf, targetName);
            //buf.writeBytes(accountNameBytes);
            buf.writeIntLE(0);   //Full Name
            buf.writeIntLE(0);
            buf.writeIntLE(0);
            buf.writeIntLE(homeDirectory.length());   //Home Directory
            buf.writeIntLE(0);
            buf.writeIntLE(homeDirectory.length());
            byte[] homeDirectoryBytes = homeDirectory.getBytes(StandardCharsets.UTF_16LE);
            writeUnicodeStringData(buf, homeDirectory);
            //buf.writeBytes(homeDirectoryBytes);
            buf.writeIntLE(0);   //Home Drive
            buf.writeIntLE(0);
            buf.writeIntLE(0);
            buf.writeIntLE(0);   //Logon Script
            buf.writeIntLE(0);
            buf.writeIntLE(0);
            buf.writeIntLE(profilePath.length());   //Profile Path
            buf.writeIntLE(0);
            buf.writeIntLE(profilePath.length());

            writeUnicodeStringData(buf, profilePath);
            //buf.writeBytes(profilePathBytes);
            buf.writeIntLE(0);   //Description
            buf.writeIntLE(0);
            buf.writeIntLE(0);
            buf.writeIntLE(0);   //Workstations
            buf.writeIntLE(0);
            buf.writeIntLE(0);
            buf.writeIntLE(0);   //Comment
            buf.writeIntLE(0);
            buf.writeIntLE(0);
            buf.writeIntLE(0);   //Parameters
            buf.writeIntLE(0);
            buf.writeIntLE(0);
            buf.writeIntLE(1260);   //Logon Hours
            buf.writeIntLE(0);
            buf.writeIntLE(21);

            for (int i = 0; i < 21; i++){
                buf.writeByte(255);
            }
            buf.writeZero(3);

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }

    public static byte[] getGroupForUserResponsePDU(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                     // frag length 占位
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                       // alloc hint 占位
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            int stubStart = buf.writerIndex();       // stub 数据开始位置

            buf.writeIntLE(0x00020000);   //Pointer to Rids Referent ID
            buf.writeIntLE(1);      //Rids Count
            buf.writeIntLE(0x00020004);   //Pointer to Rids Referent ID
            buf.writeIntLE(1);           //Max Count
            buf.writeIntLE(513);         //RID
            buf.writeIntLE(0x00000007);

            buf.writeIntLE(0);    //NT Error

            int totalSize = buf.writerIndex();       // 总长度（header + stub）
            int stubSize = totalSize - 24;           // stub 长度 = total - header

            buf.setShortLE(fragLengthPos, totalSize);   // 回填 frag length
            buf.setIntLE(allocHintPos, stubSize);       // 回填 alloc hint


            byte[] response = new byte[totalSize];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] LsaOpenPolicy3Response(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(3);                        // packet type: Response
            buf.writeByte(0x23);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            buf.writeShortLE(32);                   // frag length
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            buf.writeIntLE(0);                      // alloc hint = 80
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count
            buf.writeIntLE(0x1c010002);
            buf.writeIntLE(0);

            byte[] response = new byte[32];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] LsaOpenPolicy2Response(int callId, short contextId) {
        ByteBuf buf = Unpooled.buffer();
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                        // version
            buf.writeByte(0);                        // minor version
            buf.writeByte(2);                        // packet type: Response
            buf.writeByte(0x03);                     // flags: first + last
            buf.writeIntLE(0x10);                    // data representation (little-endian)
            buf.writeShortLE(48);                   // frag length
            buf.writeShortLE(0);                     // auth length
            buf.writeIntLE(callId);                  // call id
            buf.writeIntLE(24);
            buf.writeShortLE(contextId);             // context id
            buf.writeShortLE(0);                     // cancel count

            byte[] handle = generatePolicyHandle();
            buf.writeBytes(handle);  // 20 字节 PolicyHandle
            buf.writeZero(4);

            byte[] response = new byte[48];
            buf.readBytes(response);
            return response;
        } finally {
            buf.release();
        }
    }
    public static byte[] createFaultResponse(int callId, short contextId, int ntStatus) {
        ByteBuf buf = Unpooled.buffer(64);
        try {
            // DCE/RPC Header (24 字节)
            buf.writeByte(5);                    // version
            buf.writeByte(0);                    // minor version
            buf.writeByte(3);                    // packet type: Fault (3)
            buf.writeByte(0x03);                 // flags: first + last
            buf.writeIntLE(0x10);                // data representation (little-endian)
            int fragLengthPos = buf.writerIndex();
            buf.writeShortLE(0);                 // frag length 占位
            //buf.writeShortLE(40);                // frag length = 40
            buf.writeShortLE(0);                 // auth length = 0
            buf.writeIntLE(callId);              // call id
            int allocHintPos = buf.writerIndex();
            buf.writeIntLE(0);                   // alloc hint 占位
            //buf.writeIntLE(16);                  // alloc hint = stub 长度 16
            buf.writeShortLE(contextId);         // context id
            buf.writeShortLE(0);                 // cancel count

            // Fault Stub (16 字节)
            buf.writeIntLE(ntStatus);            // NTSTATUS 错误码
            buf.writeIntLE(0);                   // reserved
            buf.writeZero(8);                    // fault stub data (全 0)

            int totalSize = buf.writerIndex();
            int stubSize = totalSize - 24;       // 应为 16

            buf.setShortLE(fragLengthPos, totalSize);
            buf.setIntLE(allocHintPos, stubSize);

            byte[] fault = new byte[totalSize];
            buf.readerIndex(0);
            buf.readBytes(fault);
            return fault;
        } finally {
            buf.release();
        }
    }
    private static byte[] generatePolicyHandle() {
        long counter = handleCounter.getAndIncrement();

        ByteBuf buf = Unpooled.buffer(20);
        try {
            // 前4 字节
            buf.writeIntLE(0x00000000);

            //  4 字节: 版本或标志
            buf.writeIntLE(0x04000000);

            //  8 字节
            long randomPart = ThreadLocalRandom.current().nextLong();
            long uniquePart = randomPart | 0x8000000000000000L;
            buf.writeLongLE(uniquePart);

            //  4 字节: 递增 ID
            buf.writeIntLE((int) counter);

            byte[] handle = new byte[20];
            buf.readBytes(handle);
            return handle;
        } finally {
            buf.release();
        }
    }

    // 解析 SamrLookupNamesInDomain 请求中的账户名
    public static String parseSamrLookupName(byte[] stubData) {
        if (stubData == null || stubData.length < 32) {
            return null;
        }

        ByteBuffer bb = ByteBuffer.wrap(stubData).order(ByteOrder.LITTLE_ENDIAN);

        // 跳过 DomainHandle (20 字节)
        bb.position(20);

        int count = bb.getInt(); // Count
        if (count != 1) {        // 强制要求 count == 1
            return null;
        }

        // 跳过 array conformant: MaxCount, Offset, ActualCount
        bb.getInt();    // MaxCount
        bb.getInt();    // Offset
        bb.getInt();    // ActualCount

        // 只解析第一个（也是唯一一个）名称
        short length = bb.getShort();       // Length (字节数，不含 null)
        short maxLength = bb.getShort();    // MaximumLength
        bb.getInt();                        // Buffer pointer (referent ID)

        if (length <= 0 || length > 512 || length % 2 != 0) {
            return null;
        }

        int strMaxCount = bb.getInt();      // MaxCount (wchar count)
        bb.getInt();                        // Offset
        int strActualCount = bb.getInt();   // ActualCount (wchar count)

        if (strActualCount <= 0 || strActualCount > strMaxCount) {
            return null;
        }

        byte[] strBytes = new byte[length];
        bb.get(strBytes);

        String name = new String(strBytes, StandardCharsets.UTF_16LE).trim();
        if (name.isEmpty()) {
            return null;
        }
        // 返回原始名称（带后缀）
        return name;
    }

    /**
     * 解析 LsarLookupNames3 请求中的账户名（预期只有一个）。
     * - 强制要求 count == 1
     * - 返回原始名称（带后缀），解析失败或格式无效返回 null
     */
    public static String parseLsaLookupNames3Name(byte[] stubData) {
        if (stubData == null || stubData.length < 64) {  // LsarLookupNames3 头部通常更大
            return null;
        }

        ByteBuffer bb = ByteBuffer.wrap(stubData).order(ByteOrder.LITTLE_ENDIAN);

        bb.position(20);  // 保守跳过 PolicyHandle + 前置字段（实际可精确计算）

        int numNames = bb.getInt();  // Num Names (uint32)
        if (numNames != 1) {         // 强制只支持一个名称
            return null;
        }
        int maxCount = bb.getInt();

        short length = bb.getShort();       // Length
        short size = bb.getShort();    // size

        // Pointer to Names (lsa_String 数组)
        bb.getInt();  // Pointer Referent ID

        // 跳过 conformant array header
        bb.getInt();  // MaxCount
        bb.getInt();  // Offset
        bb.getInt();  // ActualCount

        if (length <= 0 || length > 512 || length % 2 != 0) {
            return null;
        }

        byte[] strBytes = new byte[length];
        bb.get(strBytes);

        String name = new String(strBytes, StandardCharsets.UTF_16LE).trim();
        if (name.isEmpty()) {
            return null;
        }

        return name;
    }

    private static void writeUnicodeStringData(ByteBuf buf, String str) {

        byte[] bytes = str.getBytes(StandardCharsets.UTF_16LE);

        buf.writeBytes(bytes);

        // 计算当前字符串写入的字节数（就是 bytes.length）
        int strByteLen = bytes.length;

        // 如果不是 4 的倍数，补齐到下一个 4 字节边界
        int remainder = strByteLen % 4;
        if (remainder != 0) {
            int paddingNeeded = 4 - remainder;
            buf.writeZero(paddingNeeded);
        }
    }
}