package com.macrosan.filesystem.cifs.types.smb2;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.ToString;

import javax.xml.bind.DatatypeConverter;
import java.util.LinkedList;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/75364667-3a93-4e2c-b771-592d8d5e876d
 */
@Data
public class CreateContext {
    public int next;
    public byte[] tag;
    public int dataSize = 0;
    public int dataOff;

    CreateContext(int next, byte[] tag) {
        this.next = next;
        this.tag = tag;
    }

    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    public int writeStruct(ByteBuf buf, int offset, boolean end) {
        buf.setIntLE(offset, next);
        //tag off
        buf.setShortLE(offset + 4, 16);
        buf.setIntLE(offset + 6, tag.length);
        //data off
        dataOff = 16 + tag.length;
        dataOff = (dataOff + 7) & ~7;
        buf.setShortLE(offset + 10, dataOff);
        buf.setIntLE(offset + 12, dataSize);
        buf.setBytes(offset + 16, tag);

        next = dataOff + dataSize;
        next = (next + 7) & ~7;

        if (end) {
            buf.setIntLE(offset, 0);
        } else {
            buf.setIntLE(offset, next);
        }

        return next;
    }


    public static CreateContext[] readCreateContexts(ByteBuf buf, int offset, int last) {
        LinkedList<CreateContext> res = new LinkedList<>();
        int next;

        do {
            next = buf.getIntLE(offset);
            int tagOff = buf.getShortLE(offset + 4);
            int tagLen = buf.getIntLE(offset + 6);
            byte[] tag = new byte[tagLen];
            int dataOff = buf.getShortLE(offset + 10);
            int dataLen = buf.getIntLE(offset + 12);
            buf.getBytes(offset + tagOff, tag);

            String t = new String(tag);
            switch (t) {
                case "DHnQ":
                    CreateDurableHandle createDurableHandle = new CreateDurableHandle(next, tag);
                    res.add(createDurableHandle);
                    break;
                case "QFid":
                    QueryFsId queryFsId = new QueryFsId(next, tag);
                    res.add(queryFsId);
                    break;
                case "MxAc":
                    MaxAccess maxAccess = new MaxAccess(next, tag);
                    res.add(maxAccess);
                    break;
                case "DH2Q":
                    CreateDurableV2Handle createDurableHandleV2 = new CreateDurableV2Handle(next, tag);
                    createDurableHandleV2.readStruct(buf, offset + dataOff);
                    res.add(createDurableHandleV2);
                    break;
                case "DH2C":
                    CreateDurableReConnectV2Handle reConnect = new CreateDurableReConnectV2Handle(next, tag);
                    reConnect.readStruct(buf, offset + dataOff);
                    res.add(reConnect);
                    break;
                case "AlSi" :
                    CreateAllocationSize createAllocationSize = new CreateAllocationSize(next, tag);
                    createAllocationSize.readStruct(buf, offset + dataOff);
                    res.add(createAllocationSize);
                case "RqLs":
                    // Lease_V1
                    if (dataLen == 32) {
                        CreateRequestLease createRequestLease = new CreateRequestLease(next, tag);
                        createRequestLease.readStruct(buf, offset + dataOff);
                        createRequestLease.dataSize = 32;
                        res.add(createRequestLease);
                    }
                    // Lease_V2
                    else if (dataLen == 52) {
                        CreateRequestLeaseV2 createRequestLeaseV2 = new CreateRequestLeaseV2(next, tag);
                        createRequestLeaseV2.readStruct(buf, offset + dataOff);
                        createRequestLeaseV2.dataSize = 52;
                        res.add(createRequestLeaseV2);
                    }
                    break;
                default:
                    res.add(new CreateContext(next, tag));
                    break;
            }

            offset += next;
        } while (next != 0);

        return res.toArray(new CreateContext[res.size()]);
    }

    //call https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/9999d870-b664-4e51-a187-1c3c16a1ae1c 忽略
    //reply https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/a3a11598-f228-47da-82bb-9418b9397041
    //不支持 DurableOpen，可以不返回
    public static class CreateDurableHandle extends CreateContext {
        public CreateDurableHandle(int next, byte[] tag) {
            super(next, tag);
        }

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean end) {
            dataSize = 8;

            int size = super.writeStruct(buf, offset, end);
            buf.setLongLE(dataOff + offset, 0);
            return size;
        }
    }

    //call https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/5e361a29-81a7-4774-861d-f290ea53a00e
    //reply https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/48c1049f-25a4-4f23-9a57-11ddd72ce985
    public static class CreateDurableV2Handle extends CreateContext {
        //客户端重试的超时时间
        public int timeout;
        public int flags;
        byte[] guid = new byte[16];

        public CreateDurableV2Handle(int next, byte[] tag) {
            super(next, tag);
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            timeout = buf.getIntLE(offset);
            flags = buf.getIntLE(offset + 4);
            buf.getBytes(offset + 12, guid);
            return 32;
        }

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean end) {
            dataSize = 8;

            int size = super.writeStruct(buf, offset, end);
            buf.setIntLE(dataOff + offset, timeout);
            buf.setIntLE(dataOff + offset + 4, flags);
            return size;
        }
    }

    //call https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/a6d418a7-d2db-47c9-a1c7-5802222ad678
    //reply https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/62ba68d0-8806-4aef-a229-eefb5827160f
    public static class CreateDurableReConnectV2Handle extends CreateContext {
        public SMB2FileId oldFileID;
        byte[] guid = new byte[16];
        int flags;

        public CreateDurableReConnectV2Handle(int next, byte[] tag) {
            super(next, tag);
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            oldFileID = new SMB2FileId();
            oldFileID.readStruct(buf, offset);
            buf.getBytes(offset + 16, guid);
            flags = buf.getIntLE(offset + 32);
            return 36;
        }

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean end) {
            //不需要返回context
            return -1;
        }
    }

    //call empty
    //reply https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/5c977939-1d8f-4774-9111-21e9195f3aca
    public static class QueryFsId extends CreateContext {
        public QueryFsId(int next, byte[] tag) {
            super(next, tag);
        }

        public long nodeId;
        public long volumeId;

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean end) {
            dataSize = 32;

            int size = super.writeStruct(buf, offset, end);
            buf.setLongLE(dataOff + offset, nodeId);
            buf.setLongLE(dataOff + offset + 8, volumeId);
            return size;
        }
    }


    //call empty
    //reply https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/0fe6be15-3a76-4032-9a44-56f846ac6244
    public static class MaxAccess extends CreateContext {
        public MaxAccess(int next, byte[] tag) {
            super(next, tag);
        }

        public int status;
        public int maximalAccess;

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean end) {
            dataSize = 8;

            int size = super.writeStruct(buf, offset, end);
            buf.setIntLE(dataOff + offset, status);
            buf.setIntLE(dataOff + offset + 4, maximalAccess);
            return size;
        }
    }


    //call https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/250a5100-f8b0-4b32-a202-f592ce4c05e7
    //reply https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/a60d6c95-15ca-4c69-816e-c145506108a3
    @ToString
    public static class CreateRequestLease extends CreateContext {
        public CreateRequestLease(int next, byte[] tag) {
            super(next, tag);
        }

        public byte[] leaseKey = new byte[16];
        public int leaseState;
        public int leaseFlags;
        public byte[] leaseDuration =  new byte[8];

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            buf.getBytes(offset, leaseKey);
            leaseState = buf.getIntLE(offset + 16);
            leaseFlags = buf.getIntLE(offset + 20);
            buf.getBytes(offset + 24, leaseDuration);
            return 32;
        }

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean end) {
            dataSize = 32;

            int size = super.writeStruct(buf, offset, end);
            buf.setBytes(dataOff + offset, leaseKey);
            buf.setIntLE(dataOff + offset + 16, leaseState);
            buf.setIntLE(dataOff + offset + 20, leaseFlags);
            buf.setBytes(dataOff + offset + 24, leaseDuration);
            return size;
        }

        public String getLeaseKey() {
            return DatatypeConverter.printHexBinary(leaseKey).toLowerCase();
        }
    }


    //call https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/32c16a84-123f-40a9-99a8-00d34964308f
    //reply https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/1bccd8d3-a13e-4288-9c7b-26e498052a25
    @ToString
    public static class CreateRequestLeaseV2 extends CreateContext {
        public CreateRequestLeaseV2(int next, byte[] tag) {
            super(next, tag);
        }

        public byte[] leaseKey = new byte[16];
        public int leaseState;
        public int leaseFlags;
        public byte[] leaseDuration =  new byte[8];
        public byte[] parentLeaseKey = new byte[16];
        public short epoch;
        public byte[] reserved = new byte[2];

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            buf.getBytes(offset, leaseKey);
            leaseState = buf.getIntLE(offset + 16);
            leaseFlags = buf.getIntLE(offset + 20);
            buf.getBytes(offset + 24, leaseDuration);
            buf.getBytes(offset + 32, parentLeaseKey);
            epoch = buf.getShortLE(offset + 48);
            buf.getBytes(offset + 50, reserved);
            return 52;
        }

        @Override
        public int writeStruct(ByteBuf buf, int offset, boolean end) {
            dataSize = 52;

            int size = super.writeStruct(buf, offset, end);
            buf.setBytes(dataOff + offset, leaseKey);
            buf.setIntLE(dataOff + offset + 16, leaseState);
            buf.setIntLE(dataOff + offset + 20, leaseFlags);
            buf.setBytes(dataOff + offset + 24, leaseDuration);
            buf.setBytes(dataOff + offset + 32, parentLeaseKey);
            buf.setShortLE(dataOff + offset + 48, epoch);
            buf.setBytes(dataOff + offset + 50, reserved);
            return size;
        }

        public String getLeaseKey() {
            return DatatypeConverter.printHexBinary(leaseKey).toLowerCase();
        }
    }

    //call https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/433917a3-1771-4521-b441-d1994b59a121
    //reply empty
    public static class CreateAllocationSize extends CreateContext {
        CreateAllocationSize(int next, byte[] tag) {
            super(next, tag);
        }

        public long allocationSize;

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            allocationSize = buf.getLongLE(offset);
            return 8;
        }
    }
}
