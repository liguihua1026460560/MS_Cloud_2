package com.macrosan.message.xmlmsg.role.deleteRolePolicy;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "DeleteRolePolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DeleteRolePolicyResponse", propOrder = {
        "responseMetadata"
})
public class DeleteRolePolicyResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
