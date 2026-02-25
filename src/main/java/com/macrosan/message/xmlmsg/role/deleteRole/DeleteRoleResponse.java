package com.macrosan.message.xmlmsg.role.deleteRole;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "DeleteRoleResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DeleteRoleResponse", propOrder = {
        "responseMetadata"
})
public class DeleteRoleResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
