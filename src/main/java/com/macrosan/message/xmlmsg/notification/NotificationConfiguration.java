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
@XmlRootElement(name = "NotificationConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "NotificationConfiguration", propOrder = {
        "topicConfiguration",
        "queueConfiguration",
        "kafkaConfiguration",
        "cloudFunctionConfiguration"
})

public class NotificationConfiguration {
    @XmlElement(name = "TopicConfiguration", required = true)
    private List<TopicConfiguration> topicConfiguration;
    @XmlElement(name = "QueueConfiguration", required = true)
    private List<QueueConfiguration> queueConfiguration;
    @XmlElement(name = "KafkaConfiguration", required = true)
    private List<KafkaConfiguration> kafkaConfiguration;
    @XmlElement(name = "CloudFunctionConfiguration")
    private String cloudFunctionConfiguration;
}
