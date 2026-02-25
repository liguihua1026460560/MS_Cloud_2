package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * @Author: WANG CHENXING
 * @Date: 2022/12/15
 * @Description: 在指定的以小时为单位的时间段内生效非默认策略
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "QoSEnabledHour")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "QoSEnabledHour", propOrder = {
        "startTime",      // 该处字母名称对应下方的变量名
        "endTime"
})
public class QoSEnabledHour {

    @XmlElement(name = "StartTime", required = true)
    private String startTime;

    @XmlElement(name = "EndTime", required = true)
    private String endTime;

}
