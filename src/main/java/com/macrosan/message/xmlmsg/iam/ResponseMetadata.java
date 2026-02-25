package com.macrosan.message.xmlmsg.iam;


import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ResponseMetadata")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ResponseMetadata", propOrder = {
        "requestId"
})
public class ResponseMetadata {
    @XmlElement(name = "RequestId", required = true)
    String requestId;
}
