package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.message.jsonmsg.Inode;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;

import java.util.LinkedList;
import java.util.List;

import static com.macrosan.filesystem.FsConstants.SMB2ACEType.*;

/**
 * @Author: WANG CHENXING
 * @Date: 2025/5/14
 * @Description: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/20233ed8-a6c6-4097-aafa-dd545ed24428
 */

@EqualsAndHashCode()
@Data
@Log4j2
public class SMB2ACL {
    byte aclRevision;
    byte Sbz1;
    // complete ACL size, including all ACEs
    short aclSize;
    // the number of ACE records in the ACL
    short aceCount;
    short Sbz2;
    List<SMB2ACE> aclList;

    //必须是4的倍数
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setByte(offset, aclRevision);
        buf.setByte(offset + 1, Sbz1);
        buf.setShortLE(offset + 2, aclSize);
        buf.setShortLE(offset + 4, aceCount);
        buf.setShortLE(offset + 6, Sbz2);

        int res = 8;
        if (aceCount > 0) {
            for (SMB2ACE smb2ACE : aclList) {
                res += smb2ACE.writeStruct(buf, offset + res);
            }
        }
        return res;
    }

    public int readStruct(ByteBuf buf, int offset) {
        aclRevision = buf.getByte(offset);
        Sbz1 = buf.getByte(offset + 1);
        aclSize = buf.getShortLE(offset + 2);
        aceCount = buf.getShortLE(offset + 4);
        Sbz2 = buf.getShortLE(offset + 6);
        int res = 8;
        if (aceCount > 0) {
            aclList = new LinkedList<>();
            for (int i = 0; i < aceCount; i++) {
                SMB2ACE.ACEHeader aceHeader = new SMB2ACE.ACEHeader();
                aceHeader.readStruct(buf, offset + res);
                SMB2ACE ace = mapBufToACE(aceHeader.aceType, buf, offset + res + 4);
                if (null != ace) {
                    ace.setHeader(aceHeader);
                    aclList.add(ace);
                }

                res = res + aceHeader.aceSize;
            }
        }

        return res;
    }

    public int size() {
        int res = 8;
        if (aceCount > 0) {
            for (SMB2ACE smb2ACE : aclList) {
                res += smb2ACE.size();
            }
        }
        return res;
    }

    public static SMB2ACE mapBufToACE(byte aceType, ByteBuf buf, int offset) {
        SMB2ACE ace = null;
        switch (aceType) {
            case ACCESS_ALLOWED_ACE_TYPE:
                ace = new SMB2ACE.ACCESS_ALLOWED_ACE();
                ace.readStruct(buf, offset);
                break;
            case ACCESS_DENIED_ACE_TYPE:
                ace = new SMB2ACE.ACCESS_DENIED_ACE();
                ace.readStruct(buf, offset);
                break;
            case ACCESS_ALLOWED_OBJECT_ACE_TYPE:
                ace = new SMB2ACE.ACCESS_ALLOWED_OBJECT_ACE();
                ace.readStruct(buf, offset);
                break;
            case ACCESS_DENIED_OBJECT_ACE_TYPE:
                ace = new SMB2ACE.ACCESS_DENIED_OBJECT_ACE();
                ace.readStruct(buf, offset);
                break;
            case SYSTEM_AUDIT_ACE_TYPE:
            case SYSTEM_ALARM_ACE_TYPE:
            case ACCESS_ALLOWED_COMPOUND_ACE_TYPE:
            case SYSTEM_AUDIT_OBJECT_ACE_TYPE:
            case SYSTEM_ALARM_OBJECT_ACE_TYPE:
            case ACCESS_ALLOWED_CALLBACK_ACE_TYPE:
            case ACCESS_DENIED_CALLBACK_ACE_TYPE:
            case ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE:
            case ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE:
            case SYSTEM_AUDIT_CALLBACK_ACE_TYPE:
            case SYSTEM_ALARM_CALLBACK_ACE_TYPE:
            case SYSTEM_AUDIT_CALLBACK_OBJECT_ACE_TYPE:
            case SYSTEM_ALARM_CALLBACK_OBJECT_ACE_TYPE:
            case SYSTEM_MANDATORY_LABEL_ACE_TYPE:
            case SYSTEM_RESOURCE_ATTRIBUTE_ACE_TYPE:
            case SYSTEM_SCOPED_POLICY_ID_ACE_TYPE:
            default:
                log.info("SMB2ACE: class: {} is not implemented", aceType);
        }

        return ace;
    }

    public static SMB2ACE mapInodeCIFSACEsToSMB2ACE(Inode.ACE inodeACE) {
        byte cType = inodeACE.getCType();
        short aceFlag = inodeACE.getFlag();
        String sidStr = inodeACE.getSid();
        long aceAccess = inodeACE.getMask();
        return mapInodeCIFSACEsToSMB2ACE(cType, aceFlag, sidStr, aceAccess);
    }

    public static SMB2ACE mapInodeCIFSACEsToSMB2ACE(byte cType, short aceFlag, String sidStr, long aceAccess) {

        SID sid = SID.generateSID(sidStr);
        SMB2ACE.ACEHeader aceHeader = new SMB2ACE.ACEHeader();
        aceHeader.setAceType(cType);
        aceHeader.setAceFlags(aceFlag);

        SMB2ACE ace = null;
        switch (cType) {
            case ACCESS_ALLOWED_ACE_TYPE:
                SMB2ACE.ACCESS_ALLOWED_ACE allowed_ace = new SMB2ACE.ACCESS_ALLOWED_ACE();
                allowed_ace.setSid(sid);
                allowed_ace.setMask(aceAccess);
                allowed_ace.setHeader(aceHeader);
                allowed_ace.getHeader().setAceSize(allowed_ace.size());
                return allowed_ace;
            case ACCESS_DENIED_ACE_TYPE:
                SMB2ACE.ACCESS_DENIED_ACE denied_ace = new SMB2ACE.ACCESS_DENIED_ACE();
                denied_ace.setSid(sid);
                denied_ace.setMask(aceAccess);
                denied_ace.setHeader(aceHeader);
                denied_ace.getHeader().setAceSize(denied_ace.size());
                return denied_ace;
            case ACCESS_ALLOWED_OBJECT_ACE_TYPE:
                SMB2ACE.ACCESS_ALLOWED_OBJECT_ACE allowed_object_ace = new SMB2ACE.ACCESS_ALLOWED_OBJECT_ACE();
                allowed_object_ace.setSid(sid);
                allowed_object_ace.setMask(aceAccess);
                allowed_object_ace.setHeader(aceHeader);
                allowed_object_ace.getHeader().setAceSize(allowed_object_ace.size());
                return allowed_object_ace;
            case ACCESS_DENIED_OBJECT_ACE_TYPE:
                SMB2ACE.ACCESS_DENIED_OBJECT_ACE denied_object_ace = new SMB2ACE.ACCESS_DENIED_OBJECT_ACE();
                denied_object_ace.setSid(sid);
                denied_object_ace.setMask(aceAccess);
                denied_object_ace.setHeader(aceHeader);
                denied_object_ace.getHeader().setAceSize(denied_object_ace.size());
                return denied_object_ace;
            case SYSTEM_AUDIT_ACE_TYPE:
            case SYSTEM_ALARM_ACE_TYPE:
            case ACCESS_ALLOWED_COMPOUND_ACE_TYPE:
            case SYSTEM_AUDIT_OBJECT_ACE_TYPE:
            case SYSTEM_ALARM_OBJECT_ACE_TYPE:
            case ACCESS_ALLOWED_CALLBACK_ACE_TYPE:
            case ACCESS_DENIED_CALLBACK_ACE_TYPE:
            case ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE:
            case ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE:
            case SYSTEM_AUDIT_CALLBACK_ACE_TYPE:
            case SYSTEM_ALARM_CALLBACK_ACE_TYPE:
            case SYSTEM_AUDIT_CALLBACK_OBJECT_ACE_TYPE:
            case SYSTEM_ALARM_CALLBACK_OBJECT_ACE_TYPE:
            case SYSTEM_MANDATORY_LABEL_ACE_TYPE:
            case SYSTEM_RESOURCE_ATTRIBUTE_ACE_TYPE:
            case SYSTEM_SCOPED_POLICY_ID_ACE_TYPE:
            default:
                log.info("SMB2ACE: class: {} is not implemented", cType);
        }

        return ace;
    }

    public static List<Inode.ACE> mapSMB2ACEToInodeACEs(List<SMB2ACE> aceList) {
        List<Inode.ACE> res = new LinkedList<>();
        for (SMB2ACE smb2ACE : aceList) {
            byte cType = smb2ACE.getHeader().getAceType();
            short aceFlag = smb2ACE.getHeader().getAceFlags();
            long mask = 0;
            SID sid = null;
            String sidStr = null;
            Inode.ACE storeACE = null;

            switch (cType) {
                case ACCESS_ALLOWED_ACE_TYPE:
                    SMB2ACE.ACCESS_ALLOWED_ACE allowed_ace = (SMB2ACE.ACCESS_ALLOWED_ACE) smb2ACE;
                    mask = allowed_ace.getMask();
                    sid = allowed_ace.getSid();
                    sidStr = SID.convertSIDToString(sid);
                    storeACE = new Inode.ACE(cType, aceFlag, sidStr, mask);
                    res.add(storeACE);
                    break;
                case ACCESS_DENIED_ACE_TYPE:
                    SMB2ACE.ACCESS_DENIED_ACE denied_ace = (SMB2ACE.ACCESS_DENIED_ACE) smb2ACE;
                    mask = denied_ace.getMask();
                    sid = denied_ace.getSid();
                    sidStr = SID.convertSIDToString(sid);
                    storeACE = new Inode.ACE(cType, aceFlag, sidStr, mask);
                    res.add(storeACE);
                    break;
                case ACCESS_ALLOWED_OBJECT_ACE_TYPE:
                    SMB2ACE.ACCESS_ALLOWED_OBJECT_ACE allowed_object_ace = (SMB2ACE.ACCESS_ALLOWED_OBJECT_ACE) smb2ACE;
                    mask = allowed_object_ace.getMask();
                    sid = allowed_object_ace.getSid();
                    sidStr = SID.convertSIDToString(sid);
                    storeACE = new Inode.ACE(cType, aceFlag, sidStr, mask);
                    res.add(storeACE);
                    break;
                case ACCESS_DENIED_OBJECT_ACE_TYPE:
                    SMB2ACE.ACCESS_DENIED_OBJECT_ACE denied_object_ace = (SMB2ACE.ACCESS_DENIED_OBJECT_ACE) smb2ACE;
                    mask = denied_object_ace.getMask();
                    sid = denied_object_ace.getSid();
                    sidStr = SID.convertSIDToString(sid);
                    storeACE = new Inode.ACE(cType, aceFlag, sidStr, mask);
                    res.add(storeACE);
                    break;
                case SYSTEM_AUDIT_ACE_TYPE:
                case SYSTEM_ALARM_ACE_TYPE:
                case ACCESS_ALLOWED_COMPOUND_ACE_TYPE:
                case SYSTEM_AUDIT_OBJECT_ACE_TYPE:
                case SYSTEM_ALARM_OBJECT_ACE_TYPE:
                case ACCESS_ALLOWED_CALLBACK_ACE_TYPE:
                case ACCESS_DENIED_CALLBACK_ACE_TYPE:
                case ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE:
                case ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE:
                case SYSTEM_AUDIT_CALLBACK_ACE_TYPE:
                case SYSTEM_ALARM_CALLBACK_ACE_TYPE:
                case SYSTEM_AUDIT_CALLBACK_OBJECT_ACE_TYPE:
                case SYSTEM_ALARM_CALLBACK_OBJECT_ACE_TYPE:
                case SYSTEM_MANDATORY_LABEL_ACE_TYPE:
                case SYSTEM_RESOURCE_ATTRIBUTE_ACE_TYPE:
                case SYSTEM_SCOPED_POLICY_ID_ACE_TYPE:
                default:
                    log.info("SMB2ACE: class: {} is not implemented", cType);
            }
        }

        return res;
    }
}
