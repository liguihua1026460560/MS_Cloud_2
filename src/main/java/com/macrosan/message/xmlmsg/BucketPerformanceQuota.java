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
@XmlRootElement(name = "PerformanceQuota")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Bucket.PerformanceQuota", propOrder = {
        "throughPut",
        "bandWidth",
        "datasyncThroughput",
        "datasyncBandWidth",
        "dataSyncPerformanceQuotaList"
})
public class BucketPerformanceQuota {

    @XmlElement(name = "Throughput", required = true)
    private String throughPut;

    @XmlElement(name = "Bandwidth", required = true)
    private String bandWidth;

    @XmlElement(name = "DataSyncThroughput")
    private String datasyncThroughput;

    @XmlElement(name = "DataSyncBandWidth")
    private String datasyncBandWidth;

    @XmlElement(name = "DataSyncPerformanceQuota")
    @XmlElementWrapper(name = "DataSyncPerformanceQuotaList")
    private List<DataSyncPerformanceQuota> dataSyncPerformanceQuotaList;

}
