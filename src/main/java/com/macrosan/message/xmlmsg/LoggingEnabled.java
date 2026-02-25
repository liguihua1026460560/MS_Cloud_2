package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

    @Data
    @Accessors(chain = true)
    @AllArgsConstructor
    @NoArgsConstructor
    @XmlRootElement(name = "LoggingEnabled")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "LoggingEnabled", propOrder = {
            "targetBucket",
            "targetGrants",
            "targetPrefix"
    })
    public class LoggingEnabled {
        @XmlElement(name = "TargetBucket", required = true)
        private String targetBucket;
        @XmlElement(name = "TargetGrants", required = true)
        private TargetGrants targetGrants;
        @XmlElement(name = "TargetPrefix" , required = true)
        private String targetPrefix;
    }
