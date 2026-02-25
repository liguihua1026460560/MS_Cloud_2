package com.macrosan.message.xmlmsg.role.listAttachedRolePolicies;


import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "AttachedPolicies")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "RolePolicies", propOrder = {
        "attachedPolicies",
})
public class AttachedPolicies {
    @XmlElement(name = "member", required = true)
    public List<AttachedPolicy> attachedPolicies;
}
