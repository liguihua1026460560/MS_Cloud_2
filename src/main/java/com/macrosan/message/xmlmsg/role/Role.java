package com.macrosan.message.xmlmsg.role;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "Role")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Role", propOrder = {
        "path",
        "description",
        "maxSessionDuration",
        "arn",
        "roleName",
        "assumeRolePolicyDocument",
        "createDate",
        "roleId"
})
public class Role {
    @XmlElement(name = "Description", required = true)
    public String description;
    @XmlElement(name = "MaxSessionDuration", required = true)
    public String maxSessionDuration;
    @XmlElement(name = "Arn", required = true)
    public String arn;
    @XmlElement(name = "RoleName")
    public String roleName;
    @XmlElement(name = "AssumeRolePolicyDocument", required = true)
    public String assumeRolePolicyDocument;
    @XmlElement(name = "CreateDate", required = true)
    public String createDate;
    @XmlElement(name = "RoleId", required = true)
    public String roleId;
    @XmlElement(name = "Path", required = true)
    public String path;

}
