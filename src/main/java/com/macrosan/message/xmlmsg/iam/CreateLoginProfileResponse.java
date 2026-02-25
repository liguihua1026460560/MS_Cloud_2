package com.macrosan.message.xmlmsg.iam;


import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "CreateLoginProfileResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CreateLoginProfileResponse", propOrder = {
        "createLoginProfileResult",
        "responseMetadata"
})
public class CreateLoginProfileResponse {
    @XmlElement(name = "CreateLoginProfileResult", required = true)
    CreateLoginProfileResult createLoginProfileResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "CreateLoginProfileResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "CreateLoginProfileResult", propOrder = {
            "loginProfile"
    })
    public static class CreateLoginProfileResult {
        @XmlElement(name = "LoginProfile", required = true)
        LoginProfile loginProfile;
    }
}
