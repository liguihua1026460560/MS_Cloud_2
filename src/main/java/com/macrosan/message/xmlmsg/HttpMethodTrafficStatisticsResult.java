package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @author zhaoyang
 * @date 2025/07/01
 **/
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "HttpMethodTrafficStatisticsResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "HttpMethodTrafficStatisticsResult", propOrder = {
        "httpMethod",
        "successCount",
        "failCount",
        "traffic",
        "time",
        "httpStatusCodeStatisticsResult"
})
public class HttpMethodTrafficStatisticsResult {
    @XmlElement(name = "HttpMethod")
    private String httpMethod;
    @XmlElement(name = "SuccessCount")
    private String successCount;
    @XmlElement(name = "FailCount")
    private String failCount;
    @XmlElement(name = "Traffic")
    private String traffic;
    @XmlElement(name = "Time")
    private String time;
    @XmlElement(name = "HttpStatusCodeStatisticsResult")
    @XmlElementWrapper(name = "HttpStatusCodeStatisticsResultList")
    private List<HttpStatusCodeStatisticsResult> httpStatusCodeStatisticsResult;
}
