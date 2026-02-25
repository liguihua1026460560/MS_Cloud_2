package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.types.smb2.IOCTLSubCall;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;
import io.netty.util.collection.IntObjectHashMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;

import java.lang.reflect.Constructor;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/5c03c9d6-15de-48a2-9835-8fb37f8a79d8
 **/
@EqualsAndHashCode(callSuper = true)
@Data
@Log4j2
public class IOCTLCall extends SMB2Body {
    private static final IntObjectHashMap<Constructor<? extends IOCTLSubCall>> subCallMap = new IntObjectHashMap<>();
    short reserved;     // structSize=2
    int ctlCode;
    SMB2FileId fileId;

    int inOff;
    int inLen;
    int maxInResponseSize;

    int outOff;
    int outLen;
    int maxOutResponseSize;

    //SMB2_0_IOCTL_IS_FSCTL
    int flags;
    int reserved2;
    IOCTLSubCall subCall;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = super.readStruct(buf, offset) + offset;
        ctlCode = buf.getIntLE(start + 2);
        fileId = new SMB2FileId();
        fileId.readStruct(buf, start + 6);
        inOff = buf.getIntLE(start + 22);
        inLen = buf.getIntLE(start + 26);
        maxInResponseSize = buf.getIntLE(start + 30);
        //output off and len 0
        maxOutResponseSize = buf.getIntLE(start + 42);
        flags = buf.getIntLE(start + 46);
        if (flags != 1) {
            return -1;
        }

        int subCallSize = 0;
        Constructor<? extends IOCTLSubCall> constructor = subCallMap.get(ctlCode);
        if (constructor == null) {
            subCall = new IOCTLSubCall.EmptyCall();
        } else {
            try {
                subCall = constructor.newInstance();
                subCallSize = subCall.readStruct(buf, inOff + 4);
            } catch (Exception e) {
                log.error("read sub call {} fail", ctlCode, e);
                return -1;
            }
        }

        return 56 + subCallSize;
    }

    public static final int FSCTL_DFS = 0x00060000;
    public static final int FSCTL_FILESYSTEM = 0x00090000;
    public static final int FSCTL_NAMED_PIPE = 0x00110000;
    public static final int FSCTL_PIPE_WAIT = 0x00110018;
    public static final int FSCTL_NETWORK_FILESYSTEM = 0x00140000;
    public static final int FSCTL_SMBTORTURE = 0x83840000;

    public static final int FSCTL_ACCESS_ANY = 0x00000000;
    public static final int FSCTL_ACCESS_READ = 0x00004000;
    public static final int FSCTL_ACCESS_WRITE = 0x00008000;

    public static final int FSCTL_METHOD_BUFFERED = 0x00000000;
    public static final int FSCTL_METHOD_IN_DIRECT = 0x00000001;
    public static final int FSCTL_METHOD_OUT_DIRECT = 0x00000002;
    public static final int FSCTL_METHOD_NEITHER = 0x00000003;

    //FSCTL_DFS
    public static final int FSCTL_DFS_GET_REFERRALS = (FSCTL_DFS | FSCTL_ACCESS_ANY | 0x0194 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_DFS_GET_REFERRALS_EX = (FSCTL_DFS | FSCTL_ACCESS_ANY | 0x01B0 | FSCTL_METHOD_BUFFERED);

    //FSCTL_NETWORK_FILESYSTEM
    public static final int FSCTL_GET_SHADOW_COPY_DATA = (FSCTL_NETWORK_FILESYSTEM | FSCTL_ACCESS_READ | 0x0064 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_SRV_ENUM_SNAPS = FSCTL_GET_SHADOW_COPY_DATA;
    public static final int FSCTL_SRV_REQUEST_RESUME_KEY = (FSCTL_NETWORK_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0078 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_SRV_COPYCHUNK = (FSCTL_NETWORK_FILESYSTEM | FSCTL_ACCESS_READ | 0x00F0 | FSCTL_METHOD_OUT_DIRECT);
    public static final int FSCTL_SRV_COPYCHUNK_WRITE = (FSCTL_NETWORK_FILESYSTEM | FSCTL_ACCESS_WRITE | 0x00F0 | FSCTL_METHOD_OUT_DIRECT);
    public static final int FSCTL_SRV_READ_HASH = (FSCTL_NETWORK_FILESYSTEM | FSCTL_ACCESS_READ | 0x01B8 | FSCTL_METHOD_NEITHER);
    public static final int FSCTL_LMR_REQ_RESILIENCY = (FSCTL_NETWORK_FILESYSTEM | FSCTL_ACCESS_ANY | 0x01D4 | FSCTL_METHOD_BUFFERED);

    public static final int FSCTL_LMR_SET_LINK_TRACKING_INFORMATION = (FSCTL_NETWORK_FILESYSTEM | FSCTL_ACCESS_ANY | 0x00EC | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_QUERY_NETWORK_INTERFACE_INFO =
            (FSCTL_NETWORK_FILESYSTEM | FSCTL_ACCESS_ANY | 0x01FC | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_VALIDATE_NEGOTIATE_INFO_224 =
            (FSCTL_NETWORK_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0200 | FSCTL_METHOD_BUFFERED);

    public static final int FSCTL_VALIDATE_NEGOTIATE_INFO = FSCTL_NETWORK_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0204 | FSCTL_METHOD_BUFFERED;

    //FSCTL_FILESYSTEM
    public static final int FSCTL_REQUEST_OPLOCK_LEVEL_1 = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0000 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_REQUEST_OPLOCK_LEVEL_2 = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0004 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_REQUEST_BATCH_OPLOCK = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0008 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_OPLOCK_BREAK_ACKNOWLEDGE = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x000C | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_OPBATCH_ACK_CLOSE_PENDING = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0010 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_OPLOCK_BREAK_NOTIFY = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0014 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_GET_COMPRESSION = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x003C | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_SET_COMPRESSION = (FSCTL_FILESYSTEM | FSCTL_ACCESS_READ | FSCTL_ACCESS_WRITE | 0x0040 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_FILESYS_GET_STATISTICS = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0060 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_GET_NTFS_VOLUME_DATA = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0064 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_IS_VOLUME_DIRTY = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0078 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_FIND_FILES_BY_SID = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x008C | FSCTL_METHOD_NEITHER);
    public static final int FSCTL_SET_OBJECT_ID = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0098 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_GET_OBJECT_ID = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x009C | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_DELETE_OBJECT_ID = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x00A0 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_SET_REPARSE_POINT = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x00A4 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_GET_REPARSE_POINT = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x00A8 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_DELETE_REPARSE_POINT = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x00AC | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_SET_OBJECT_ID_EXTENDED = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x00BC | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_CREATE_OR_GET_OBJECT_ID = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x00C0 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_SET_SPARSE = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x00C4 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_SET_ZERO_DATA = (FSCTL_FILESYSTEM | FSCTL_ACCESS_WRITE | 0x00C8 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_SET_ZERO_ON_DEALLOCATION = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0194 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_READ_FILE_USN_DATA = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x00EB | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_WRITE_USN_CLOSE_RECORD = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x00EF | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_QUERY_ALLOCATED_RANGES = (FSCTL_FILESYSTEM | FSCTL_ACCESS_READ | 0x00CC | FSCTL_METHOD_NEITHER);
    public static final int FSCTL_QUERY_ON_DISK_VOLUME_INFO = (FSCTL_FILESYSTEM | FSCTL_ACCESS_WRITE | 0x013C | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_QUERY_SPARING_INFO = (FSCTL_FILESYSTEM | FSCTL_ACCESS_WRITE | 0x0138 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_FILE_LEVEL_TRIM = (FSCTL_FILESYSTEM | FSCTL_ACCESS_WRITE | 0x0208 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_OFFLOAD_READ = (FSCTL_FILESYSTEM | FSCTL_ACCESS_READ | 0x0264 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_OFFLOAD_WRITE = (FSCTL_FILESYSTEM | FSCTL_ACCESS_WRITE | 0x0268 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_SET_INTEGRITY_INFORMATION = (FSCTL_FILESYSTEM | FSCTL_ACCESS_READ | FSCTL_ACCESS_WRITE | 0x0280 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_DUP_EXTENTS_TO_FILE = (FSCTL_FILESYSTEM | FSCTL_ACCESS_WRITE | 0x0344 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_DUPLICATE_EXTENTS_TO_FILE_EX = (FSCTL_FILESYSTEM | FSCTL_ACCESS_WRITE | 0x03E8 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_STORAGE_QOS_CONTROL = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0350 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_SVHDX_SYNC_TUNNEL_REQUEST = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0304 | FSCTL_METHOD_BUFFERED);
    public static final int FSCTL_QUERY_SHARED_VIRTUAL_DISK_SUPPORT = (FSCTL_FILESYSTEM | FSCTL_ACCESS_ANY | 0x0300 | FSCTL_METHOD_BUFFERED);

    static {
        try {
            for (Class cl : IOCTLSubCall.class.getDeclaredClasses()) {
                if (cl.getSuperclass() == IOCTLSubCall.class) {
                    IOCTLSubCall.Smb2IOCTL ctl = (IOCTLSubCall.Smb2IOCTL) cl.getAnnotation(IOCTLSubCall.Smb2IOCTL.class);
                    if (ctl != null) {
                        try {
                            subCallMap.put(ctl.value(), cl.getDeclaredConstructor());
                        } catch (Exception e) {
                            log.error("load ioctl sub call fail", e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("load ioctl sub call total fail", e);
        }
    }
}
