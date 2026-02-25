package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: WANG CHENXING
 * @Date: 2025/5/14
 * @Description:
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Log4j2
public class SecurityInfo extends GetInfoReply.Info {
    byte revision = 1;
    byte Sbz1;
    short control;
    //当前inode的owner的sid在整个securityInfo结构体中的偏移起始位置
    int offsetOwner;
    //当前inode的group的sid在整个securityInfo结构体中的偏移起始位置
    int offsetGroup;
    //当前inode的系统acl在整个securityInfo结构体中的偏移起始位置
    int offsetSacl;
    //当前inode的acl在整个securityInfo结构体中的偏移起始位置
    int offsetDacl;
    SID ownerSID;
    SID groupSID;
    //系统acl 审计日志相关
    SMB2ACL sACL;
    //文件或目录的acl
    SMB2ACL dACL;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {

        buf.setByte(offset, revision);
        buf.setByte(offset + 1, Sbz1);
        buf.setShortLE(offset + 2, control);

        buf.setIntLE(offset + 4, offsetOwner);
        buf.setIntLE(offset + 8, offsetGroup);
        buf.setIntLE(offset + 12, offsetSacl);
        buf.setIntLE(offset + 16, offsetDacl);

        int res = 20;
        if (null != ownerSID && offsetOwner > 0) {
            res += ownerSID.writeStruct(buf, offset + res);
            if (CIFSACL.cifsACL) {
                log.info("ownerSID write: {}", res);
            }
        }

        if (null != groupSID && offsetGroup > 0) {
            res += groupSID.writeStruct(buf, offset + res);
            if (CIFSACL.cifsACL) {
                log.info("groupSID write: {}", res);
            }
        }

        if (null != sACL && offsetSacl > 0) {
            res += sACL.writeStruct(buf, offset + res);
            if (CIFSACL.cifsACL) {
                log.info("sACL write: {}", res);
            }
        }

        if (null != dACL && offsetDacl > 0) {
            res += dACL.writeStruct(buf, offset + res);
            if (CIFSACL.cifsACL) {
                log.info("dACL write: {}", res);
            }
        }

        return res;
    }

    /**
     * 在解析setInfo请求时使用，其返回值需根据SetInfo的readStruct中的调用决定是否有效
     **/
    @Override
    public int readStruct(ByteBuf buf, int offset) {
        revision = buf.getByte(offset);
        Sbz1 = buf.getByte(offset + 1);
        control = buf.getShortLE(offset + 2);
        offsetOwner = buf.getIntLE(offset + 4);
        offsetGroup = buf.getIntLE(offset + 8);
        offsetSacl = buf.getIntLE(offset + 12);
        offsetDacl = buf.getIntLE(offset + 16);

        int res = 20;
        //在请求时offsetOwner、group、sacl和dacl并不会按顺序来，首先需要进行排序
        //设0-owner，1-group，2-sacl，3-dacl
        Map<Integer, Integer> typeToOffset = new HashMap<>();
        typeToOffset.put(0, offsetOwner);
        typeToOffset.put(1, offsetGroup);
        typeToOffset.put(2, offsetSacl);
        typeToOffset.put(3, offsetDacl);

        int[] orderOffset = {0, 1, 2, 3};
        //最大数在最后
        for (int i = 0; i < typeToOffset.size() - 1; i++) {
            for (int j = 0; j < typeToOffset.size() - i - 1; j++) {
                if (typeToOffset.get(orderOffset[j]) >= typeToOffset.get(orderOffset[j + 1])) {
                    int temp = orderOffset[j];
                    orderOffset[j] = orderOffset[j + 1];
                    orderOffset[j + 1] = temp;
                }
            }
        }

        for (int i = 0; i < orderOffset.length; i++) {
            int type = orderOffset[i];
            int offsetValue = typeToOffset.get(type);
            if (offsetValue > 0) {
                switch (type) {
                    case 0:
                        if (CIFSACL.cifsACL) {
                            log.info("ownerSID1 read: {}, offsetOwner: {}", res, offsetOwner);
                        }

                        if (offsetOwner > 0) {
                            ownerSID = new SID();
                            res += ownerSID.readStruct(buf, offset + res);
                            if (CIFSACL.cifsACL) {
                                log.info("ownerSID2 read: {}", res);
                            }
                        }
                        break;
                    case 1:
                        if (CIFSACL.cifsACL) {
                            log.info("groupSID1 read: {}, groupSID: {}, offsetGroup: {}", res, groupSID, offsetGroup);
                        }
                        if (offsetGroup > 0) {
                            groupSID = new SID();
                            res += groupSID.readStruct(buf, offset + res);
                            if (CIFSACL.cifsACL) {
                                log.info("groupSID2 read: {}, groupSID: {}", res, groupSID);
                            }
                        }
                        break;
                    case 2:
                        if (CIFSACL.cifsACL) {
                            log.info("sACL1 read: {}, offsetSacl: {}", res, offsetSacl);
                        }
                        if (offsetSacl > 0) {
                            sACL = new SMB2ACL();
                            res += sACL.readStruct(buf, offset + res);
                            if (CIFSACL.cifsACL) {
                                log.info("sACL2 read: {}", res);
                            }
                        }
                        break;
                    case 3:
                        if (CIFSACL.cifsACL) {
                            log.info("dACL1 read: {}, offsetDacl: {}", res, offsetDacl);
                        }
                        if (offsetDacl > 0) {
                            dACL = new SMB2ACL();
                            res += dACL.readStruct(buf, offset + res);
                            if (CIFSACL.cifsACL) {
                                log.info("dACL2 read: {}, dAcl: {}", res, dACL);
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        return res;
    }

    @Override
    public int size() {
        int res = 20;
        if (null != ownerSID && offsetOwner > 0) {
            res += ownerSID.size();
            if (ACLUtils.aclDebug) {
                log.info("ownerSID size: {}", res);
            }

        }

        if (null != groupSID && offsetGroup > 0) {
            res += groupSID.size();
            if (ACLUtils.aclDebug) {
                log.info("groupSID size: {}", res);
            }
        }

        if (null != sACL) {
            res += sACL.size();
            if (ACLUtils.aclDebug) {
                log.info("sACL size: {}", res);
            }
        }

        if (null != dACL) {
            res += dACL.size();
            if (ACLUtils.aclDebug) {
                log.info("dACL size: {}", res);
            }
        }

        return res;
    }

    public static final int SELF_RELATIVE = 0b1000_0000_0000_0000;
    public static final int RM_CONTROL_VALID = 0b0100_0000_0000_0000;
    public static final int SACL_PROTECTED = 0b0010_0000_0000_0000;
    public static final int DACL_PROTECTED = 0b0001_0000_0000_0000;
    public static final int SACL_AUTO_INHERITED = 0b0000_1000_0000_0000;
    public static final int DACL_AUTO_INHERITED = 0b0000_0100_0000_0000;
    public static final int SACL_COMPUTED_INHERITANCE_REQUIRED = 0b0000_0010_0000_0000;
    public static final int DACL_COMPUTED_INHERITANCE_REQUIRED = 0b0000_0001_0000_0000;
    public static final int SERVER_SECURITY = 0b0000_0000_1000_0000;
    public static final int DACL_TRUSTED = 0b0000_0000_0100_0000;
    public static final int SACL_DEFAULTED = 0b0000_0000_0010_0000;
    public static final int SACL_PRESENT = 0b0000_0000_0001_0000;
    public static final int DACL_DEFAULTED = 0b0000_0000_0000_1000;
    public static final int DACL_PRESENT = 0b0000_0000_0000_0100;
    public static final int GROUP_DEFAULTED = 0b0000_0000_0000_0010;
    public static final int OWNER_DEFAULTED = 0b0000_0000_0000_0001;
}
