package com.macrosan.message.xmlmsg.role.assumeRole;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "Credentials")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Credentials", propOrder = {
        "accessKeyId",
        "secretAccessKey",
        "sessionToken",
        "expiration"
})
public class Credentials {
    @XmlElement(name = "AccessKeyId", required = true)
    public String accessKeyId;
    @XmlElement(name = "SecretAccessKey", required = true)
    public String secretAccessKey;
    @XmlElement(name = "SessionToken")
    public String sessionToken;
    @XmlElement(name = "Expiration", required = true)
    public String expiration;

}
