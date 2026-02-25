package com.macrosan.message.xmlmsg.role.createRole;

import com.macrosan.message.xmlmsg.role.Role;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "CreateRoleResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CreateRoleResult", propOrder = {
        "role",
})
public class CreateRoleResult {
    @XmlElement(name = "Role", required = true)
    public Role role;
}
