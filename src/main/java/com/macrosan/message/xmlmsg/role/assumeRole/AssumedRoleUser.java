package com.macrosan.message.xmlmsg.role.assumeRole;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "AssumedRoleUser")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AssumedRoleUser", propOrder = {
        "arn",
        "assumedRoleId"
})
public class AssumedRoleUser {
    @XmlElement(name = "Arn", required = true)
    public String arn;

    @XmlElement(name = "AssumedRoleId", required = true)
    public String assumedRoleId;
}
