package com.macrosan.message.xmlmsg.role.putRolePolicy;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "PutRolePolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PutRolePolicyResponse", propOrder = {
        "responseMetadata"
})
public class PutRolePolicyResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
