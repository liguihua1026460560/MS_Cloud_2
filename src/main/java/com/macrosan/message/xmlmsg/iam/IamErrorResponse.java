package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ErrorResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "IamErrorResponse", propOrder = {
        "error",
        "requestId"
})
public class IamErrorResponse {
    @XmlElement(name = "Error", required = true)
    Error error;
    @XmlElement(name = "RequestId", required = true)
    String requestId;

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "Error")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "IamError", propOrder = {
            "code",
            "message"
    })
    public static class Error {
        @XmlElement(name = "Code", required = true)
        String code;
        @XmlElement(name = "Message", required = true)
        String message;
    }
}
