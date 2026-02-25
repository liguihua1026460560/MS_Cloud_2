package com.macrosan.message.xmlmsg.role.listRoles;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListRolesResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListRolesResponse", propOrder = {
        "listRolesResult",
        "responseMetadata"
})
public class ListRolesResponse {
    @XmlElement(name = "ListRolesResult", required = true)
    public ListRolesResult listRolesResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
