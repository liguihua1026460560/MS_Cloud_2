package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "GetUserResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GetUserResponse", propOrder = {
        "getUserResult",
        "responseMetadata"
})
public class GetUserResponse {
    @XmlElement(name = "GetUserResult", required = true)
    GetUserResult getUserResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "GetUserResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "GetUserResult", propOrder = {
            "user"
    })
    public static class GetUserResult {
        @XmlElement(name = "User", required = true)
        User user;
    }
}
