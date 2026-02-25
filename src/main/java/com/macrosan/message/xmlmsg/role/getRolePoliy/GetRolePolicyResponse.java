package com.macrosan.message.xmlmsg.role.getRolePoliy;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "GetRolePolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GetRolePolicyResponse", propOrder = {
        "rolePolicyResult",
        "responseMetadata"
})
public class GetRolePolicyResponse {
    @XmlElement(name = "GetRolePolicyResult", required = true)
    public GetRolePolicyResult rolePolicyResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
