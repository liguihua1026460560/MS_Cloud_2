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
@XmlRootElement(name = "SysPerformanceQuota")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SysPerformanceQuota", propOrder = {
        "sysPerformanceQuotaList"
})
public class SysPerformanceQuota {

    @XmlElementWrapper(name = "PerformanceQuotaList")
    @XmlElement(name = "PerformanceQuota")
    private List<PerformanceQuota> sysPerformanceQuotaList;

}
