package com.macrosan.message.xmlmsg.role.listRoles;

import com.macrosan.message.xmlmsg.role.Role;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListRolesResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListRolesResult", propOrder = {
        "isTruncated",
        "marker",
        "roles",
})
public class ListRolesResult {
    @XmlElement(name = "IsTruncated", required = true)
    public boolean isTruncated;

    @XmlElement(name = "Marker")
    public String marker;

    @XmlElementWrapper(name = "Roles")
    @XmlElement(name = "member", required = true)
    public List<Role> roles;
}
