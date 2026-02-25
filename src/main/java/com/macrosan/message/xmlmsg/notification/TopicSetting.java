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
@XmlRootElement(name = "TopicSetting")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "TopicSetting",propOrder = {
        "topic"
})
public class TopicSetting {
    @XmlElement(name = "Topic")
    List<String> topic;
}
