package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/7/31 19:18
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "Group")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Group", propOrder = {
        "path",
        "groupName",
        "groupId",
        "arn",
        "createDate"
})
public class Group {
    @XmlElement(name = "Path", required = true)
    public String path;
    @XmlElement(name = "GroupName", required = true)
    public String groupName;
    @XmlElement(name = "GroupId" ,required = true)
    public String groupId;
    @XmlElement(name = "Arn", required = true)
    public String arn;
    @XmlElement(name = "CreateDate", required = true)
    public String createDate;

}
