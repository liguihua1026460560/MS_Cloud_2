package com.macrosan.message.xmlmsg.role.attachRolePolicy;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "AttachRolePolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AttachRolePolicyResponse", propOrder = {
        "responseMetadata"
})
public class AttachRolePolicyResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
