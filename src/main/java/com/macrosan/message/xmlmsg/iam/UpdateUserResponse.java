package com.macrosan.message.xmlmsg.iam;


import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "UpdateUserResponse ")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UpdateUserResponse ", propOrder = {
        "updateUserResult",
        "responseMetadata"
})
public class UpdateUserResponse  {
    @XmlElement(name = "GetUserResult", required = true)
    UpdateUserResult updateUserResult;

    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "UpdateUserResult ")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "UpdateUserResult ", propOrder = {
            "user",
    })
    public static class UpdateUserResult {
        @XmlElement(name = "User", required = true)
        User user;
    }
}
