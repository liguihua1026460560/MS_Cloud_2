package com.macrosan.message.xmlmsg.role.getRolePoliy;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "GetRolePolicyResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GetRolePolicyResult", propOrder = {
        "policyName",
        "roleName",
        "policyDocument"
})
public class GetRolePolicyResult {
    @XmlElement(name = "PolicyName", required = true)
    public String policyName;
    @XmlElement(name = "RoleName", required = true)
    public String roleName;
    @XmlElement(name = "PolicyDocument", required = true)
    public String policyDocument;
}
