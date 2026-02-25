package com.macrosan.message.xmlmsg.clear;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "ClearConfigration")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ClearConfigration", propOrder = {

})
public class ClearConfigration {
    @XmlElement(name = "Model")
    private String model;

    @XmlElement(name = "StartTime")
    private String startTime;

    @XmlElement(name = "EndTime")
    private String endTime;

    @XmlElement(name = "CapacityRemaining")
    private String capacityRemaining;

    @XmlElement(name = "ObjectRemaining")
    private String objectRemaining;
}
