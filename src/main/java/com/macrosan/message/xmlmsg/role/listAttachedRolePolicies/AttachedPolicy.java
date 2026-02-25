package com.macrosan.message.xmlmsg.role.listAttachedRolePolicies;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "AttachedPolicy")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AttachedPolicy", propOrder = {
        "policyName",
        "policyArn",
})
public class AttachedPolicy {
    @XmlElement(name = "PolicyName", required = true)
    String policyName;
    @XmlElement(name = "PolicyArn", required = true)
    String policyArn;
}
