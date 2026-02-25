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
@XmlType(name = "Bucket.BucketAddressFSPerfQuota", propOrder = {
        "totalSize",
        "bucketFSPerfQuotaList"
})
public class BucketAddressFSPerfQuota {
    @XmlElement(name = "TotalSize")
    private String totalSize;

    @XmlElement(name = "BucketFSPerfQuota")
    @XmlElementWrapper(name = "BucketFSPerfQuotaList")
    private List<BucketFSPerfQuota> bucketFSPerfQuotaList;
}
