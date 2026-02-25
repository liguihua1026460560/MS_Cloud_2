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
@XmlRootElement(name = "DataSyncPerformanceQuota")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Bucket.DataSyncPerformanceQuota", propOrder = {
        "clusterName",
        "datasyncThroughput",
        "datasyncBandWidth"
})
public class DataSyncPerformanceQuota {

    @XmlElement(name = "ClusterName")
    private String clusterName;

    @XmlElement(name = "DataSyncThroughput")
    private String datasyncThroughput;

    @XmlElement(name = "DataSyncBandWidth")
    private String datasyncBandWidth;

}
