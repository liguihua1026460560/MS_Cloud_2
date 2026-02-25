package com.macrosan.message.xmlmsg.role.updateAssumeRolePolicy;


import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "UpdateAssumeRolePolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UpdateAssumeRolePolicyResponse", propOrder = {
        "responseMetadata"
})
public class UpdateAssumeRolePolicyResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
