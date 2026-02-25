package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * 保存账户下流量统计信息的容器
 *
 * @author sunfang
 * @date 2019/7/11
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "TrafficStatisticsResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "TrafficStatisticsResult", propOrder = {
        "startTime",
        "endTime",
        "readCount",
        "writeCount",
        "totalCount",
        "readTraffic",
        "writeTraffic",
        "totalTraffic",
        "totalTime",
        "bucketName",
        "httpMethodTrafficStatisticsResult"
})
public class TrafficStatisticsResult {

    @XmlElement(name = "StartTime", required = true)
    private String startTime;

    @XmlElement(name = "EndTime", required = true)
    private String endTime;

    @XmlElement(name = "ReadCount")
    private String readCount;

    @XmlElement(name = "WriteCount")
    private String writeCount;

    @XmlElement(name = "TotalCount")
    private String totalCount;

    @XmlElement(name = "ReadTraffic")
    private String readTraffic;

    @XmlElement(name = "WriteTraffic")
    private String writeTraffic;

    @XmlElement(name = "TotalTraffic")
    private String totalTraffic;

    @XmlElement(name = "TotalTime")
    private String totalTime;

    @XmlElement(name = "BucketName")
    private String bucketName;

    @XmlElement(name = "HttpMethodTrafficStatisticsResult")
    @XmlElementWrapper(name = "HttpMethodTrafficStatisticsResultList")
    private List<HttpMethodTrafficStatisticsResult> httpMethodTrafficStatisticsResult;
}

