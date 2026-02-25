package com.macrosan.message.xmlmsg.role.listAttachedRolePolicies;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListAttachedRolePoliciesResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListAttachedRolePoliciesResult", propOrder = {
        "attachedPolicies",
        "isTruncated",
        "marker"
})
public class ListAttachedRolePoliciesResult {
    @XmlElement(name = "AttachedPolicies", required = true)
    public AttachedPolicies attachedPolicies;

    @XmlElement(name = "IsTruncated", required = true)
    public boolean isTruncated;

    @XmlElement(name = "Marker")
    public String marker;
}
