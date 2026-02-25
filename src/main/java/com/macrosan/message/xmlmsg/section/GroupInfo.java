package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * GroupInfo
 * 保存用户组信息的容器
 *
 * @author shilinyong
 * @date 2019/08/02
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "GroupInfo")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GroupInfo", propOrder = {
        "groupId",
        "groupName",
        "groupNickName",
        "remark",
        "createTime",
        "groupMRN"
})
public class GroupInfo {

    @XmlElement(name = "GroupId", required = true)
    private String groupId;
    @XmlElement(name = "GroupName", required = true)
    private String groupName;
    @XmlElement(name = "GroupNickName", required = true)
    private String groupNickName;
    @XmlElement(name = "Remark", required = true)
    private String remark;
    @XmlElement(name = "CreateTime", required = true)
    private String createTime;
    @XmlElement(name = "GroupMRN", required = true)
    private String groupMRN;
}
