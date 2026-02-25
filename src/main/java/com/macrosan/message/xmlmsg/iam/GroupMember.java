package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/1 9:11
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "member")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "member",namespace = "Group",propOrder = {
        "path",
        "groupName",
        "groupId",
        "arn",
        "createDate"
})
public class GroupMember {
    @XmlElement(name = "Path",required = true)
    String path;
    @XmlElement(name = "GroupName",required = true)
    String groupName;
    @XmlElement(name = "GroupId",required = true)
    String groupId;
    @XmlElement(name = "Arn",required = true)
    String arn;
    @XmlElement(name = "CreateDate",required = true)
    String createDate;
}
