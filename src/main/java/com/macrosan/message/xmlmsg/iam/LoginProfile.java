package com.macrosan.message.xmlmsg.iam;


import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "LoginProfile")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "LoginProfile", propOrder = {
        "passwordResetRequired",
        "userName",
        "createDate"
})
public class LoginProfile {
    @XmlElement(name = "PasswordResetRequired", required = true)
    boolean passwordResetRequired;
    @XmlElement(name = "UserName", required = true)
    String userName;
    @XmlElement(name = "CreateDate", required = true)
    String createDate;
}
