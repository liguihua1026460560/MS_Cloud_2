package com.macrosan.message.xmlmsg.role.listRolePolicies;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListRolePoliciesResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListRolePoliciesResponse", propOrder = {
        "result",
        "responseMetadata"
})
public class ListRolePoliciesResponse {
    @XmlElement(name = "ListRolePoliciesResult", required = true)
    public ListRolePoliciesResult result;
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
