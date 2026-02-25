package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * @author zhaoyang
 * @date 2023/07/25
 **/

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ComponentStrategy")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ComponentStrategy", propOrder = {
        "strategyName",
        "type",
        "process",
        "destination",
        "deleteSource",
        "copyUserMetaData"
})

public class ComponentStrategy {

    @XmlElement(name = "Name")
    private String strategyName;

    @XmlElement(name = "Type", required = true)
    private String type;

    @XmlElement(name = "Process", required = true)
    private String process;

    @XmlElement(name = "Destination", required = true)
    private String destination;
    @XmlElement(name = "DeleteSource", required = true)
    private String deleteSource;
    @XmlElement(name = "CopyUserMetaData", required = true)
    private String copyUserMetaData;
}
