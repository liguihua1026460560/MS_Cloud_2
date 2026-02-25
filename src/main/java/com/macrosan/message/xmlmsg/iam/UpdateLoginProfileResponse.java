package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "UpdateLoginProfileResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UpdateLoginProfileResponse", propOrder = {
        "responseMetadata"
})
public class UpdateLoginProfileResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;
}
