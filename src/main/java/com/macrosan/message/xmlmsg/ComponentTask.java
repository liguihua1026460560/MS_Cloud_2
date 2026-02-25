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
@XmlRootElement(name = "ComponentTask")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ComponentTask", propOrder = {
        "taskName",
        "strategyType",
        "strategyName",
        "bucket",
        "prefix",
        "startTime"
})
public class ComponentTask {

    @XmlElement(name = "Name", required = true)
    private String taskName;

    @XmlElement(name = "StrategyName", required = true)
    private String strategyName;

    @XmlElement(name = "Type", required = true)
    private String strategyType;

    @XmlElement(name = "Bucket", required = true)
    private String bucket;

    @XmlElement(name = "Prefix", required = true)
    private String prefix;

    @XmlElement(name = "StartTime", required = true)
    private String startTime;


}
