package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "DeleteAccessKeyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DeleteAccessKeyResponse", propOrder = {
        "responseMetadata"
})
public class DeleteAccessKeyResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;
}
