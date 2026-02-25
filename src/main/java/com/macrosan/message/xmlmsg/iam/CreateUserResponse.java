package com.macrosan.message.xmlmsg.iam;


import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "CreateUserResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CreateUserResponse", propOrder = {
        "createUserResult",
        "responseMetadata"
})
public class CreateUserResponse {
    @XmlElement(name = "CreateUserResult", required = true)
    CreateUserResult createUserResult;

    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "CreateUserResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "CreateUserResult", propOrder = {
            "user"
    })
    public static class CreateUserResult {
        @XmlElement(name = "User", required = true)
        User user;
    }
}
