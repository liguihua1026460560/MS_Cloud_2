package com.macrosan.message.xmlmsg.role.updateRole;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "UpdateRoleResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UpdateRoleResponse", propOrder = {
        "updateRoleResult",
        "responseMetadata"
})
public class UpdateRoleResponse {
    @XmlElement(name = "UpdateRoleResult", required = true)
    public UpdateRoleResult updateRoleResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}

