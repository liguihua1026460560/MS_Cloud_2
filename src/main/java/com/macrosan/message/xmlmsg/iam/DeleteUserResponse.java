package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "DeleteUserResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DeleteUserResponse", propOrder = {
        "responseMetadata"
})
public class DeleteUserResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;
}
