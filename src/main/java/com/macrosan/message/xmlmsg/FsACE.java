package com.macrosan.message.xmlmsg;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.message.jsonmsg.Inode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * @Author: WANG CHENXING
 * @Date: 2025/9/26
 * @Description:
 */

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "FsACE")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "FsACE", propOrder = {

})
public class FsACE {

    @XmlElement(name = "Proto", required = true)
    private int proto;

    @XmlElement(name = "NType", required = true)
    private int nType;

    @XmlElement(name = "Right", required = true)
    private int right;

    @XmlElement(name = "ID", required = true)
    private int id;

    @XmlElement(name = "CType", required = true)
    private byte cType;

    @XmlElement(name = "Flag", required = true)
    private short flag;

    @XmlElement(name = "SID", required = true)
    private String sid;

    @XmlElement(name = "Mask", required = true)
    private long mask;

    public static FsACE mapToNfsAce(Inode.ACE ace) {
        FsACE nfsAce = new FsACE();
        nfsAce.setNType(ace.getNType())
                .setRight(ace.getRight())
                .setId(ace.getId())
                .setProto(FsConstants.ProtoFlag.NFSV3_START);

        return nfsAce;
    }

    public static FsACE mapToCifsAce(Inode.ACE ace) {
        FsACE cifsAce = new FsACE();
        cifsAce.setProto(FsConstants.ProtoFlag.CIFS_START)
                .setCType(ace.getCType())
                .setFlag(ace.getFlag())
                .setSid(ace.getSid())
                .setMask(ace.getMask());

        return cifsAce;
    }

}
