package com.macrosan.message.xmlmsg.role.getRole;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "GetRoleResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GetRoleResponse", propOrder = {
        "getRoleResult",
        "responseMetadata"
})
public class GetRoleResponse {
    @XmlElement(name = "GetRoleResult", required = true)
    public GetRoleResult getRoleResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
