package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "User")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "User", propOrder = {
        "path",
        "userName",
        "userId",
        "arn",
        "createDate"
})
public class User {
    @XmlElement(name = "Path", required = true)
    public String path;
    @XmlElement(name = "UserName", required = true)
    public String userName;
    @XmlElement(name = "UserId", required = true)
    public String userId;
    @XmlElement(name = "Arn", required = true)
    public String arn;
    @XmlElement(name = "CreateDate", required = true)
    public String createDate;
}
