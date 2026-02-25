package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "InstancePerformanceQuota")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Bucket.InstancePerformanceQuota", propOrder = {
        "name",
        "throughput",
        "bandWidth"
})
public class InstancePerformanceQuota {

    @XmlElement(name = "Name")
    private String name;

    @XmlElement(name = "Throughput")
    private String throughput;

    @XmlElement(name = "BandWidth")
    private String bandWidth;

}
