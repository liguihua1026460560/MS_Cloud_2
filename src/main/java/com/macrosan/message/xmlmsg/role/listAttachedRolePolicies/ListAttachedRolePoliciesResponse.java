package com.macrosan.message.xmlmsg.role.listAttachedRolePolicies;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListAttachedRolePoliciesResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListAttachedRolePoliciesResponse", propOrder = {
        "result",
        "responseMetadata"
})
public class ListAttachedRolePoliciesResponse {
    @XmlElement(name = "ListAttachedRolePoliciesResult", required = true)
    public ListAttachedRolePoliciesResult result;
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
