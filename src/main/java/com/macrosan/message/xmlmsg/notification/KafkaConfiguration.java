package com.macrosan.message.xmlmsg.notification;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/6/26 14:28
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "KafkaConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "KafkaConfiguration",propOrder = {
        "event",
        "id",
        "topic",
        "filter"
})
public class KafkaConfiguration {
    @XmlElement(name = "Event")
    private List<String> event;
    @XmlElement(name = "Id")
    private String id;
    @XmlElement(name = "Topic")
    private String topic;
    @XmlElement(name = "Filter")
    private Filter filter;
}
