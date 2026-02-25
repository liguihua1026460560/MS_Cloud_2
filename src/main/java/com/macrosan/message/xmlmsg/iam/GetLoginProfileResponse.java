package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "GetLoginProfileResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GetLoginProfileResponse", propOrder = {
        "getLoginProfileResult",
        "responseMetadata"
})
public class GetLoginProfileResponse {
    @XmlElement(name = "GetLoginProfileResult", required = true)
    GetLoginProfileResult getLoginProfileResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "GetLoginProfileResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "GetLoginProfileResult", propOrder = {
            "loginProfile"
    })
    public static class GetLoginProfileResult {
        @XmlElement(name = "LoginProfile", required = true)
        LoginProfile loginProfile;
    }
}
