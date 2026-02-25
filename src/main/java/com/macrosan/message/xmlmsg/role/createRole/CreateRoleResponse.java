package com.macrosan.message.xmlmsg.role.createRole;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "CreateRoleResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CreateRoleResponse", propOrder = {
        "createRoleResult",
        "responseMetadata"
})
public class CreateRoleResponse {
    @XmlElement(name = "CreateRoleResult", required = true)
    public CreateRoleResult createRoleResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
