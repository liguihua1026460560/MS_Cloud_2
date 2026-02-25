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
@XmlRootElement(name = "QueueConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "QueueConfiguration",propOrder = {
        "event",
        "id",
        "queue",
        "filter"
})
public class QueueConfiguration {
    @XmlElement(name = "Event")
    private List<String> event;
    @XmlElement(name = "Id")
    private String id;
    @XmlElement(name = "Queue")
    private String queue;
    @XmlElement(name = "Filter")
    private Filter filter;
}
