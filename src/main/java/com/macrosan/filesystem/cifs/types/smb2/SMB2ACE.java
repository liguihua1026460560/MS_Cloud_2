package com.macrosan.filesystem.cifs.types.smb2;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @Author: WANG CHENXING
 * @Date: 2025/5/14
 * @Description:https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/628ebb1d-c509-4ea0-a10f-77ef97ca4586
 */

@EqualsAndHashCode
@Data
public abstract class SMB2ACE {
    public ACEHeader header;

    public abstract int writeStruct(ByteBuf buf, int offset);

    public abstract int readStruct(ByteBuf buf, int offset);

    public abstract short size();

    @Data
    public static class ACEHeader {
        byte aceType;
        //aceFlags实际为无符号 1byte，超过java byte显示范围，因此通过short表示，writeStruct和readStruct时均需转换
        short aceFlags;
        //aceSize为无符号 2byte类型数值，表示整个SMBACE的大小，虽然short为有符号数值，但范围-32768-32767足够表示一个ace，因此不做处理
        short aceSize;

        public int writeStruct(ByteBuf buf, int offset) {
            buf.setByte(offset, aceType);
            buf.setByte(offset + 1, (aceFlags & 0xFF));
            buf.setShortLE(offset + 2, aceSize);
            return 4;
        }

        public int readStruct(ByteBuf buf, int offset) {
            aceType = buf.getByte(offset);
            aceFlags = buf.getUnsignedByte(offset + 1);
            aceSize = buf.getShortLE(offset + 2);
            return 4;
        }

    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/72e7c7ea-bc02-4c74-a619-818a16bf6adb
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class ACCESS_ALLOWED_ACE extends SMB2ACE {
        //4byte, 无符号int
        long mask;
        SID sid;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            return 8 + sid.writeStruct(buf, offset + 8);
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            sid = new SID();
            return 4 + sid.readStruct(buf, offset + 4);
        }

        @Override
        public short size() {
            short res = 8;
            if (null != sid) {
                res += sid.size();
            }
            return res;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/c79a383c-2b3f-4655-abe7-dcbb7ce0cfbe
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class ACCESS_ALLOWED_OBJECT_ACE extends SMB2ACE {
        long mask;
        int flags;
        //16 byte
        byte[] objectType = new byte[16];
        //16 byte
        byte[] inheritedObjectType = new byte[16];
        SID sid;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            buf.setIntLE(offset + 8, flags);
            buf.setBytes(offset + 12, objectType);
            buf.setBytes(offset + 28, inheritedObjectType);
            return 46 + sid.writeStruct(buf, offset + 46);
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            flags = buf.getIntLE(offset + 4);
            buf.getBytes(offset + 8, objectType);
            buf.getBytes(offset + 24, inheritedObjectType);

            sid = new SID();
            return 40 + sid.readStruct(buf, offset + 40);
        }

        @Override
        public short size() {
            short res = (short) (12 + objectType.length + inheritedObjectType.length);

            if (null != sid) {
                res += sid.size();
            }

            return res;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/b1e1321d-5816-4513-be67-b65d8ae52fe8
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class ACCESS_DENIED_ACE extends SMB2ACE {
        long mask;
        SID sid;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            return 8 + sid.writeStruct(buf, offset + 8);
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            sid = new SID();
            int res = sid.readStruct(buf, offset + 4);
            return 4 + res;
        }

        @Override
        public short size() {
            short res = 8;
            if (null != sid) {
                res += sid.size();
            }
            return res;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/8720fcf3-865c-4557-97b1-0b3489a6c270
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class ACCESS_DENIED_OBJECT_ACE extends SMB2ACE {
        long mask;
        int flags;
        //16 byte
        byte[] objectType = new byte[16];
        //16 byte
        byte[] inheritedObjectType = new byte[16];
        SID sid;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            buf.setIntLE(offset + 8, flags);
            buf.setBytes(offset + 12, objectType);
            buf.setBytes(offset + 28, inheritedObjectType);
            return 46 + sid.writeStruct(buf, offset + 46);
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            flags = buf.getIntLE(offset + 4);
            buf.getBytes(offset + 8, objectType);
            buf.getBytes(offset + 24, inheritedObjectType);

            sid = new SID();
            return 40 + sid.readStruct(buf, offset + 40);
        }

        @Override
        public short size() {
            short res = (short) (12 + objectType.length + inheritedObjectType.length);

            if (null != sid) {
                res += sid.size();
            }

            return res;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/c9579cf4-0f4a-44f1-9444-422dfb10557a
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class ACCESS_ALLOWED_CALLBACK_ACE extends SMB2ACE {
        long mask;
        SID sid;
        byte[] applicationData;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            int off = 8;
            if (null != sid) {
                off += sid.writeStruct(buf, offset + off);
            }

            if (null != applicationData && applicationData.length > 0) {
                buf.setBytes(offset + off, applicationData);
                return off + applicationData.length;
            }

            return off;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            sid = new SID();
            int sidSize = sid.readStruct(buf, offset + 4);
            int res = header.aceSize - 4 - 4 - sidSize;
            if (res > 0) {
                applicationData = new byte[res];
                buf.getBytes(offset + 4 + sidSize, applicationData);
                return 4 + sidSize + res;
            }

            return 4 + sidSize;
        }

        @Override
        public short size() {
            return 0;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/35adad6b-fda5-4cc1-b1b5-9beda5b07d2e
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class ACCESS_DENIED_CALLBACK_ACE extends SMB2ACE {
        long mask;
        SID sid;
        byte[] applicationData;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            int off = 8;
            if (null != sid) {
                off += sid.writeStruct(buf, offset + off);
            }

            if (null != applicationData && applicationData.length > 0) {
                buf.setBytes(offset + off, applicationData);
                return off + applicationData.length;
            }

            return off;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            sid = new SID();
            int sidSize = sid.readStruct(buf, offset + 4);
            int res = header.aceSize - 4 - 4 - sidSize;
            if (res > 0) {
                applicationData = new byte[res];
                buf.getBytes(offset + 4 + sidSize, applicationData);
                return 4 + sidSize + res;
            }

            return 4 + sidSize;
        }

        @Override
        public short size() {
            return 0;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/fe1838ea-ea34-4a5e-b40e-eb870f8322ae
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class ACCESS_ALLOWED_CALLBACK_OBJECT_ACE extends SMB2ACE {
        long mask;
        int flags;
        //16 byte
        byte[] objectType = new byte[16];
        //16 byte
        byte[] inheritedObjectType = new byte[16];
        SID sid;
        byte[] applicationData;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            buf.setIntLE(offset + 8, flags);
            buf.setBytes(offset + 12, objectType);
            buf.setBytes(offset + 28, inheritedObjectType);
            int off = 44;
            if (null != sid) {
                off += sid.writeStruct(buf, offset + off);
            }

            if (null != applicationData && applicationData.length > 0) {
                buf.setBytes(offset + off, applicationData);
                return off + applicationData.length;
            }

            return off;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            flags = buf.getIntLE(offset + 4);
            buf.getBytes(offset + 8, objectType);
            buf.getBytes(offset + 24, inheritedObjectType);

            sid = new SID();
            int sidSize = sid.readStruct(buf, offset + 40);
            int res = header.aceSize - 4 - 40 - sidSize;
            if (res > 0) {
                applicationData = new byte[res];
                buf.getBytes(offset + 40 + sidSize, applicationData);
                return 40 + sidSize + res;
            }

            return 40 + sidSize;
        }

        @Override
        public short size() {
            return 0;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/4652f211-82d5-4b90-bd58-43bf3b0fc48d
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class ACCESS_DENIED_CALLBACK_OBJECT_ACE extends SMB2ACE {
        long mask;
        int flags;
        //16 byte
        byte[] objectType = new byte[16];
        //16 byte
        byte[] inheritedObjectType = new byte[16];
        SID sid;
        byte[] applicationData;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            buf.setIntLE(offset + 8, flags);
            buf.setBytes(offset + 12, objectType);
            buf.setBytes(offset + 28, inheritedObjectType);
            int off = 44;
            if (null != sid) {
                off += sid.writeStruct(buf, offset + off);
            }

            if (null != applicationData && applicationData.length > 0) {
                buf.setBytes(offset + off, applicationData);
                return off + applicationData.length;
            }

            return off;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            flags = buf.getIntLE(offset + 4);
            buf.getBytes(offset + 8, objectType);
            buf.getBytes(offset + 24, inheritedObjectType);

            sid = new SID();
            int sidSize = sid.readStruct(buf, offset + 40);
            int res = header.aceSize - 4 - 40 - sidSize;
            if (res > 0) {
                applicationData = new byte[res];
                buf.getBytes(offset + 40 + sidSize, applicationData);
                return 40 + sidSize + res;
            }

            return 40 + sidSize;
        }

        @Override
        public short size() {
            return 0;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/9431fd0f-5b9a-47f0-b3f0-3015e2d0d4f9
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class SYSTEM_AUDIT_ACE extends SMB2ACE {
        long mask;
        SID sid;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));

            if (null != sid) {
                return 8 + sid.writeStruct(buf, offset + 8);
            }

            return 8;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            sid = new SID();
            int sidSize = sid.readStruct(buf, offset + 4);

            return 4 + sidSize;
        }

        @Override
        public short size() {
            return 0;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/c8da72ae-6b54-4a05-85f4-e2594936d3d5
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class SYSTEM_AUDIT_OBJECT_ACE extends SMB2ACE {
        long mask;
        int flags;
        //16 byte
        byte[] objectType = new byte[16];
        //16 byte
        byte[] inheritedObjectType = new byte[16];
        SID sid;
        byte[] applicationData;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            buf.setIntLE(offset + 8, flags);
            buf.setBytes(offset + 12, objectType);
            buf.setBytes(offset + 28, inheritedObjectType);
            int off = 44;
            if (null != sid) {
                off += sid.writeStruct(buf, offset + off);
            }

            if (null != applicationData && applicationData.length > 0) {
                buf.setBytes(offset + off, applicationData);
                return off + applicationData.length;
            }

            return off;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            flags = buf.getIntLE(offset + 4);
            buf.getBytes(offset + 8, objectType);
            buf.getBytes(offset + 24, inheritedObjectType);

            sid = new SID();
            int sidSize = sid.readStruct(buf, offset + 40);
            int res = header.aceSize - 4 - 40 - sidSize;
            if (res > 0) {
                applicationData = new byte[res];
                buf.getBytes(offset + 40 + sidSize, applicationData);
                return 40 + sidSize + res;
            }

            return 40 + sidSize;
        }

        @Override
        public short size() {
            return 0;
        }

    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/bd6b6fd8-4bef-427e-9a43-b9b46457e934
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class SYSTEM_AUDIT_CALLBACK_ACE extends SMB2ACE {
        long mask;
        SID sid;
        byte[] applicationData;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            int off = 8;
            if (null != sid) {
                off += sid.writeStruct(buf, offset + off);
            }

            if (null != applicationData && applicationData.length > 0) {
                buf.setBytes(offset + off, applicationData);
                return off + applicationData.length;
            }

            return off;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            sid = new SID();
            int sidSize = sid.readStruct(buf, offset + 4);
            int res = header.aceSize - 4 - 4 - sidSize;
            if (res > 0) {
                applicationData = new byte[res];
                buf.getBytes(offset + 4 + sidSize, applicationData);
                return 4 + sidSize + res;
            }

            return 4 + sidSize;
        }

        @Override
        public short size() {
            return 0;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/949b02e7-f55d-4c26-969f-52a009597469
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class SYSTEM_AUDIT_CALLBACK_OBJECT_ACE extends SMB2ACE {
        long mask;
        int flags;
        //16 byte
        byte[] objectType = new byte[16];
        //16 byte
        byte[] inheritedObjectType = new byte[16];
        SID sid;
        byte[] applicationData;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            buf.setIntLE(offset + 8, flags);
            buf.setBytes(offset + 12, objectType);
            buf.setBytes(offset + 28, inheritedObjectType);
            int off = 44;
            if (null != sid) {
                off += sid.writeStruct(buf, offset + off);
            }

            if (null != applicationData && applicationData.length > 0) {
                buf.setBytes(offset + off, applicationData);
                return off + applicationData.length;
            }

            return off;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            flags = buf.getIntLE(offset + 4);
            buf.getBytes(offset + 8, objectType);
            buf.getBytes(offset + 24, inheritedObjectType);

            sid = new SID();
            int sidSize = sid.readStruct(buf, offset + 40);
            int res = header.aceSize - 4 - 40 - sidSize;
            if (res > 0) {
                applicationData = new byte[res];
                buf.getBytes(offset + 40 + sidSize, applicationData);
                return 40 + sidSize + res;
            }

            return 40 + sidSize;
        }

        @Override
        public short size() {
            return 0;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/352944c7-4fb6-4988-8036-0a25dcedc730
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class SYSTEM_RESOURCE_ATTRIBUTE_ACE extends SMB2ACE {
        long mask;
        SID sid;
        byte[] attributeData;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            int off = 8;
            if (null != sid) {
                off += sid.writeStruct(buf, offset + off);
            }

            if (null != attributeData && attributeData.length > 0) {
                buf.setBytes(offset + off, attributeData);
                return off + attributeData.length;
            }

            return off;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            sid = new SID();
            int sidSize = sid.readStruct(buf, offset + 4);
            int res = header.aceSize - 4 - 4 - sidSize;
            if (res > 0) {
                attributeData = new byte[res];
                buf.getBytes(offset + 4 + sidSize, attributeData);
                return 4 + sidSize + res;
            }

            return 4 + sidSize;
        }

        @Override
        public short size() {
            return 0;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/aa0c0f62-4b4c-44f0-9718-c266a6accd9f
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class SYSTEM_SCOPED_POLICY_ID_ACE extends SMB2ACE {
        long mask;
        SID sid;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            header.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, (int) (mask & 0xFFFFFFFFL));
            int off = 8;
            if (null != sid) {
                off += sid.writeStruct(buf, offset + off);
            }

            return off;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            mask = buf.getUnsignedIntLE(offset);
            sid = new SID();
            int sidSize = sid.readStruct(buf, offset + 4);

            return 4 + sidSize;
        }

        @Override
        public short size() {
            return 0;
        }
    }


}
