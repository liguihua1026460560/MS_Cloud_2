package com.macrosan.message.xmlmsg.role.detachRolePolicy;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "DetachRolePolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DetachRolePolicyResponse", propOrder = {
        "responseMetadata"
})
public class DetachRolePolicyResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
