package com.macrosan.message.xmlmsg.role.getRole;

import com.macrosan.message.xmlmsg.role.Role;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "GetRoleResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GetRoleResult", propOrder = {
        "role",
})
public class GetRoleResult {
    @XmlElement(name = "Role", required = true)
    public Role role;
}
