package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "FSPerformanceQuota")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Bucket.FSPerformanceQuota", propOrder = {
        "address",
        "instancePerformanceQuotaList"
})
public class BucketFSPerfQuota {
    @XmlElement(name = "Address")
    private String address;

    @XmlElement(name = "InstancePerformanceQuota")
    @XmlElementWrapper(name = "InstancePerformanceQuotaList")
    private List<InstancePerformanceQuota> instancePerformanceQuotaList;

}
