package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @Author: WANG CHENXING
 * @Date: 2025/9/26
 * @Description:
 */

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "FsACL")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "FsACL", propOrder = {
        "obj",
        "nodeId",
        "owner",
        "group",
        "gids",
        "mode",
        "ACEs"
})
public class FsACL {
    @XmlElement(name = "Obj", required = true)
    private String obj;

    @XmlElement(name = "NodeId", required = true)
    private long nodeId;

    @XmlElement(name = "Owner", required = true)
    private int owner;

    @XmlElement(name = "Group", required = true)
    private int group;

    @XmlElement(name = "Gids", required = true)
    private List<Integer> gids;

    @XmlElement(name = "Mode", required = true)
    private int mode;

    @XmlElement(name = "ACEs", required = true)
    private List<FsACE> ACEs;
}
