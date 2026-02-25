package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * @author zhaoyang
 * @date 2025/07/02
 **/
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "HttpStatusCodeStatisticsResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "HttpStatusCodeStatisticsResult", propOrder = {
        "statusCode",
        "count",
})
public class HttpStatusCodeStatisticsResult {
    @XmlElement(name = "StatusCode")
    private String statusCode;
    @XmlElement(name = "Count")
    private String count;
}
