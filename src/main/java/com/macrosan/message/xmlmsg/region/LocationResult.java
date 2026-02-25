package com.macrosan.message.xmlmsg.region;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className LocationResult
 * @date 2021/10/19 10:52
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@XmlRootElement(name = "LocationResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "LocationResult", propOrder = {
        "regionName",
        "clusterName",
})
public class LocationResult {
    @XmlElement(name = "RegionName", required = true)
    private String regionName;
    @XmlElement(name = "ClusterName", required = true)
    private String clusterName;
}
