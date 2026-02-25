package com.macrosan.message.xmlmsg.role.assumeRole;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "AssumeRoleResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AssumeRoleResult", propOrder = {
        "assumedRoleUser",
        "credentials",
})
public class AssumeRoleResult {
    @XmlElement(name = "AssumedRoleUser", required = true)
    public AssumedRoleUser assumedRoleUser;
    @XmlElement(name = "Credentials", required = true)
    public Credentials credentials;
}
