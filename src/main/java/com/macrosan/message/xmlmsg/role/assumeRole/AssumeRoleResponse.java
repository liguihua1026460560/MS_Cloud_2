package com.macrosan.message.xmlmsg.role.assumeRole;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "AssumeRoleResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AssumeRoleResponse", propOrder = {
        "assumeRoleResult",
        "responseMetadata"
})
public class AssumeRoleResponse {
    @XmlElement(name = "AssumeRoleResult", required = true)
    public AssumeRoleResult assumeRoleResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;

}
