package com.macrosan.message.xmlmsg.notification;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @author zhangzhixin
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "TopicConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "TopicConfiguration",propOrder = {
        "event",
        "id",
        "topic",
        "filter"
})
public class TopicConfiguration {
    @XmlElement(name = "Event")
    private List<String> event;
    @XmlElement(name = "Id")
    private String id;
    @XmlElement(name = "Topic")
    private String topic;
    @XmlElement(name = "Filter")
    private Filter filter;
}
