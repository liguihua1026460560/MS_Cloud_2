package com.macrosan.message.xmlmsg.role.updateRoleDescription;

import com.macrosan.message.xmlmsg.role.Role;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "UpdateRoleDescriptionResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UpdateRoleDescriptionResult", propOrder = {
        "role",
})
public class UpdateRoleDescriptionResult {
    @XmlElement(name = "Role", required = true)
    public Role role;
}
