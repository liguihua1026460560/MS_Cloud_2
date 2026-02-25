package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "DeleteLoginProfileResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DeleteLoginProfileResponse", propOrder = {
        "responseMetadata"
})
public class DeleteLoginProfileResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;
}
