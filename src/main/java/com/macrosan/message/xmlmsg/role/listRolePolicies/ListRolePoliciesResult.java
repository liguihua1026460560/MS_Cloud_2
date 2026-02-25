package com.macrosan.message.xmlmsg.role.listRolePolicies;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListRolePoliciesResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListRolePoliciesResult", propOrder = {
        "policyNames",
        "isTruncated",
        "marker"
})
public class ListRolePoliciesResult {
    @XmlElementWrapper(name = "PolicyNames")
    @XmlElement(name = "member", required = true)
    public List<String> policyNames;

    @XmlElement(name = "IsTruncated", required = true)
    public boolean isTruncated;

    @XmlElement(name = "Marker")
    public String marker;
}
